package kube_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

var dvGVR = schema.GroupVersionResource{Group: "cdi.kubevirt.io", Version: "v1beta1", Resource: "datavolumes"}

func nfsClass(t *testing.T, name string) kube.Classes {
	t.Helper()
	_, err := kc.StorageV1().StorageClasses().Create(context.Background(), &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: "nfs.csi.k8s.io",
		Parameters:  map[string]string{"server": "nas.example", "share": "/export/vws"},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	return kube.Classes{model.VolumeTypeStandard: name, model.VolumeTypeSSD: name}
}

func dataVolume(t *testing.T, b *kube.Block, id model.VolumeID) *unstructured.Unstructured {
	t.Helper()
	dv, err := dyn.Resource(dvGVR).Namespace(ns).Get(context.Background(), b.ClaimName(id), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return dv
}

// An NFS volume is Filesystem, and KubeVirt hot-plugs a Filesystem claim only
// when it already holds disk.img. So it is a CDI blank DataVolume of the
// claim's name, and it is busy until CDI has written the image.
func TestFilesystemVolumeIsABlankDataVolumeAndWaitsForIt(t *testing.T) {
	ctx := context.Background()
	b := kube.NewBlock(kc, dyn, ns, nfsClass(t, "nfs-csi"), "").WithVolumeMode(corev1.PersistentVolumeFilesystem)
	id := model.NewVolumeID()
	s := spec(t, model.VolumeTypeStandard, model.GiB)
	if err := b.CreateVolume(ctx, id, s); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("before CDI finished: %v", err)
	}
	dv := dataVolume(t, b, id)
	if _, ok, _ := unstructured.NestedMap(dv.Object, "spec", "source", "blank"); !ok {
		t.Fatalf("source is not blank: %v", dv.Object["spec"])
	}
	for path, want := range map[string]string{"volumeMode": "Filesystem", "storageClassName": "nfs-csi"} {
		if got, _, _ := unstructured.NestedString(dv.Object, "spec", "storage", path); got != want {
			t.Fatalf("%s %q, want %q", path, got, want)
		}
	}
	if got := dv.GetAnnotations()["cdi.kubevirt.io/storage.deleteAfterCompletion"]; got != "false" {
		t.Fatalf("deleteAfterCompletion %q", got)
	}
	if claimFor(t, id) != nil {
		t.Fatal("storage made the claim itself; CDI owns it")
	}

	// CDI finished: the same request is now done, and a different zone conflicts.
	if err := unstructured.SetNestedField(dv.Object, "Succeeded", "status", "phase"); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(dvGVR).Namespace(ns).Update(ctx, dv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateVolume(ctx, id, s); err != nil {
		t.Fatalf("after CDI finished: %v", err)
	}
	other := s
	z, err := model.ParseZone("az-2")
	if err != nil {
		t.Fatal(err)
	}
	other.Zone = z
	if err := b.CreateVolume(ctx, id, other); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("other zone: %v", err)
	}

	if err := b.DeleteVolume(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(dvGVR).Namespace(ns).Get(ctx, b.ClaimName(id), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("data volume after delete: %v", err)
	}
	if err := b.DeleteVolume(ctx, id); err != nil {
		t.Fatalf("second delete: %v", err)
	}
}

func TestNoSnapshotClassMeansNoSnapshots(t *testing.T) {
	b := kube.NewBlock(kc, dyn, ns, kube.DefaultClasses(), "")
	err := b.CreateSnapshot(context.Background(), model.NewSnapshotID(), newVolume(t, b))
	if !errors.Is(err, backend.ErrInvalid) || !backend.IsPermanent(err) {
		t.Fatalf("snapshot without a class: %v", err)
	}
}

// Preflight reads the provisioner of each class: NFS in Block mode is not
// ready, NFS in Filesystem mode needs the CDI API, and a snapshot class of
// another driver is not ready.
func TestPreflightChecksTheProvisioner(t *testing.T) {
	ctx := context.Background()
	classes := nfsClass(t, "nfs-csi")
	err := kube.NewBlock(kc, dyn, ns, classes, "").Ready(ctx)
	if err == nil || !strings.Contains(err.Error(), "nfs.csi.k8s.io serves only Filesystem") {
		t.Fatalf("NFS in Block mode: %v", err)
	}
	fs := kube.NewBlock(kc, dyn, ns, classes, "").WithVolumeMode(corev1.PersistentVolumeFilesystem)
	if err := fs.Ready(ctx); err != nil {
		t.Fatalf("NFS in Filesystem mode: %v", err)
	}
	if got := fs.Provisioners(ctx); got["nfs-csi"] != "nfs.csi.k8s.io" {
		t.Fatalf("provisioners %v", got)
	}

	vsc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass",
		"metadata": map[string]any{"name": "rbd-only"}, "driver": "rbd.csi.ceph.com", "deletionPolicy": "Delete",
	}}
	if _, err := dyn.Resource(snapClassGVR).Create(ctx, vsc, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	err = kube.NewBlock(kc, dyn, ns, classes, "rbd-only").WithVolumeMode(corev1.PersistentVolumeFilesystem).Ready(ctx)
	if err == nil || !strings.Contains(err.Error(), `driver "rbd.csi.ceph.com" is not the provisioner`) {
		t.Fatalf("mismatched snapshot class: %v", err)
	}
}
