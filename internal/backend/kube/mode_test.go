package kube_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

func modeOf(t *testing.T, id model.VolumeID) corev1.PersistentVolumeMode {
	t.Helper()
	c := claimFor(t, id)
	if c == nil {
		t.Fatal("no claim")
	}
	if c.Spec.VolumeMode == nil {
		return corev1.PersistentVolumeFilesystem
	}
	return *c.Spec.VolumeMode
}

func restore(t *testing.T, b *kube.Block, snap model.SnapshotID) model.VolumeID {
	t.Helper()
	src, err := model.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	s := spec(t, model.VolumeTypeStandard, model.GiB)
	s.Source = src
	id := model.NewVolumeID()
	if err := b.CreateVolume(context.Background(), id, s); err != nil {
		t.Fatal(err)
	}
	return id
}

// KubeVirt hotplugs a Filesystem claim only when it holds disk.img (vws-test
// 25h HotplugFailed). A new volume is a raw Block claim.
func TestNewVolumeIsBlockMode(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	if m := modeOf(t, newVolume(t, b)); m != corev1.PersistentVolumeBlock {
		t.Fatalf("volumeMode %q, want Block", m)
	}
}

// A restore takes the mode of the claim the snapshot was taken from.
func TestRestoreKeepsTheSourceVolumeMode(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()

	snap := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, snap, newVolume(t, b))
	if got := snapshotFor(t, snap).GetAnnotations()["vws.popododo0720.io/source-volume-mode"]; got != "Block" {
		t.Fatalf("source mode annotation %q", got)
	}
	markReady(t, snap, true)
	if m := modeOf(t, restore(t, b, snap)); m != corev1.PersistentVolumeBlock {
		t.Fatalf("restore of a Block snapshot is %q", m)
	}

	// A Filesystem claim from before this change: its snapshot restores as Filesystem.
	legacy := model.NewVolumeID()
	fs := corev1.PersistentVolumeFilesystem
	class := kube.DefaultClass
	_, err := kc.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: b.ClaimName(legacy), Namespace: ns,
			Annotations: map[string]string{kube.VolumeAnnotation: legacy.String()},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: &class, VolumeMode: &fs,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	old := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, old, legacy)
	markReady(t, old, true)
	if m := modeOf(t, restore(t, b, old)); m != corev1.PersistentVolumeFilesystem {
		t.Fatalf("restore of a Filesystem snapshot is %q", m)
	}

	// A snapshot made before the annotation existed restores as Filesystem.
	pre := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, pre, legacy)
	vs := snapshotFor(t, pre)
	ann := vs.GetAnnotations()
	delete(ann, "vws.popododo0720.io/source-volume-mode")
	vs.SetAnnotations(ann)
	if _, err := dyn.Resource(snapGVR).Namespace(ns).Update(ctx, vs, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	markReady(t, pre, true)
	if m := modeOf(t, restore(t, b, pre)); m != corev1.PersistentVolumeFilesystem {
		t.Fatalf("restore of an unannotated snapshot is %q", m)
	}
}
