package kube_test

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

const linstor = "linstor.csi.linbit.com"

// classWithExpansion makes a class whose allowVolumeExpansion is expand.
func classWithExpansion(t *testing.T, name, provisioner string, expand *bool) {
	t.Helper()
	_, err := kc.StorageV1().StorageClasses().Create(context.Background(), &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: name},
		Provisioner:          provisioner,
		AllowVolumeExpansion: expand,
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

func linstorBlock(c kube.Class) *kube.Block {
	return kube.NewBlock(kc, dyn, ns, kube.Classes{}, "").WithClass(model.VolumeTypeGP2, c)
}

// The local driver is LINSTOR (#71). A Block mapping with an expandable class
// and a LINSTOR snapshot class is ready and makes a Block claim of that class.
func TestLinstorBlockMappingIsReadyAndServed(t *testing.T) {
	ctx := context.Background()
	storageClass(t, "linstor-r2", linstor)
	snapshotClassObject(t, "linstor-snap", linstor)
	b := linstorBlock(kube.Class{StorageClass: "linstor-r2", SnapshotClass: "linstor-snap"})
	if err := b.Ready(ctx); err != nil {
		t.Fatalf("LINSTOR in Block mode: %v", err)
	}
	if got := b.Provisioners(ctx)["linstor-r2"]; got != linstor {
		t.Fatalf("provisioner %q", got)
	}

	id := model.NewVolumeID()
	if err := b.CreateVolume(ctx, id, spec(t, model.VolumeTypeGP2, model.GiB)); err != nil {
		t.Fatal(err)
	}
	pvc := claimFor(t, id)
	if pvc == nil || pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "linstor-r2" ||
		pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode != corev1.PersistentVolumeBlock {
		t.Fatalf("claim %+v", pvc)
	}
	bind(t, id)
	if err := b.Expand(ctx, id, 2*model.GiB); err != nil {
		t.Fatalf("expand: %v", err)
	}
	snap := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, snap, id) //nolint:errcheck // busy until ReadyToUse; the object is what is checked
	vs := snapshotFor(t, snap)
	if got, _, _ := unstructured.NestedString(vs.Object, "spec", "volumeSnapshotClassName"); got != "linstor-snap" {
		t.Fatalf("snapshot class %q", got)
	}
}

// A LINSTOR class must grow and snapshot: without allowVolumeExpansion, with
// a type that has no snapshot class, or with a snapshot class of another
// driver, it is not ready.
func TestLinstorPreflightNeedsExpansionAndASnapshotClass(t *testing.T) {
	ctx := context.Background()
	no := false
	storageClass(t, "linstor-r2", linstor)
	classWithExpansion(t, "linstor-fixed", linstor, &no)
	classWithExpansion(t, "linstor-unset", linstor, nil)
	snapshotClassObject(t, "linstor-snap", linstor)
	snapshotClassObject(t, "rbd-only", "rbd.csi.ceph.com")

	needs := "provisioner linstor.csi.linbit.com needs "
	for _, c := range []struct {
		class, snap, want string
	}{
		{"linstor-fixed", "linstor-snap", `StorageClass "linstor-fixed": ` + needs + "allowVolumeExpansion: true"},
		{"linstor-unset", "linstor-snap", `StorageClass "linstor-unset": ` + needs + "allowVolumeExpansion: true"},
		{"linstor-r2", "", `StorageClass "linstor-r2": ` + needs + "a VolumeSnapshotClass for types gp2"},
		{"linstor-r2", "no-such-snap", `VolumeSnapshotClass "no-such-snap"`},
		{"linstor-r2", "rbd-only", `driver "rbd.csi.ceph.com" is not the provisioner`},
	} {
		err := linstorBlock(kube.Class{StorageClass: c.class, SnapshotClass: c.snap}).Ready(ctx)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s/%s: %v, want %q", c.class, c.snap, err, c.want)
		}
	}

	// The rules are LINSTOR's: a Ceph class with no snapshot class stays ready.
	if err := kube.NewBlock(kc, dyn, ns, kube.DefaultClasses(), "").Ready(ctx); err != nil {
		t.Fatalf("Ceph without snapshots: %v", err)
	}
}
