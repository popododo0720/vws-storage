package kube_test

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

// CDI makes the claim of a Filesystem DataVolume larger than the volume (the
// filesystem overhead). Expand compares with the volume size, not the claim
// request, and keeps the overhead on top.
func TestExpandOfAFilesystemVolumeIgnoresTheCDIOverhead(t *testing.T) {
	ctx := context.Background()
	b := kube.NewBlock(kc, dyn, ns, kube.DefaultClasses(), "").WithVolumeMode(corev1.PersistentVolumeFilesystem)
	id := model.NewVolumeID()
	if err := b.CreateVolume(ctx, id, spec(t, model.VolumeTypeStandard, model.GiB)); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("create: %v", err)
	}
	// What CDI does: the claim of the DataVolume, 1Gi plus 100Mi of overhead.
	fs, class, mib := corev1.PersistentVolumeFilesystem, kube.DefaultClass, int64(1<<20)
	_, err := kc.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: b.ClaimName(id), Namespace: ns, Annotations: map[string]string{kube.VolumeAnnotation: id.String()}},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: &class, VolumeMode: &fs,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: *resource.NewQuantity(1124*mib, resource.BinarySI)}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bind(t, id)

	if err := b.Expand(ctx, id, model.GiB); err != nil {
		t.Fatalf("same volume size is a no-op: %v", err)
	}
	// 1Gi+50Mi is below the claim request (1Gi+100Mi) but grows the volume.
	if err := b.Expand(ctx, id, model.Bytes(1074*mib)); err != nil {
		t.Fatalf("grow under the overhead: %v", err)
	}
	if got := claimFor(t, id).Spec.Resources.Requests.Storage().Value(); got != 1174*mib {
		t.Fatalf("claim request %d MiB, want 1174", got/mib)
	}
	if err := b.Expand(ctx, id, 2*model.GiB); err != nil {
		t.Fatal(err)
	}
	c := claimFor(t, id)
	if got := c.Spec.Resources.Requests.Storage().Value(); got != 2148*mib {
		t.Fatalf("second expand: claim request %d MiB, want 2148", got/mib)
	}
	if got := c.Annotations["vws.popododo0720.io/size"]; got != "2147483648" {
		t.Fatalf("size annotation %q", got)
	}
	if err := b.Expand(ctx, id, model.Bytes(1074*mib)); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("shrink below the volume size: %v", err)
	}
}
