package kube_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

func vtype(t *testing.T, s string) model.VolumeType {
	t.Helper()
	typ, err := model.ParseVolumeType(s)
	if err != nil {
		t.Fatal(err)
	}
	return typ
}

func storageClass(t *testing.T, name, provisioner string) {
	t.Helper()
	expand := true
	_, err := kc.StorageV1().StorageClasses().Create(context.Background(), &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: name}, Provisioner: provisioner, AllowVolumeExpansion: &expand,
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

func snapshotClassObject(t *testing.T, name, driver string) {
	t.Helper()
	vsc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass",
		"metadata": map[string]any{"name": name}, "driver": driver, "deletionPolicy": "Delete",
	}}
	_, err := dyn.Resource(snapClassGVR).Create(context.Background(), vsc, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

func snapshotClassOf(t *testing.T, id model.SnapshotID) string {
	t.Helper()
	class, _, err := unstructured.NestedString(snapshotFor(t, id).Object, "spec", "volumeSnapshotClassName")
	if err != nil {
		t.Fatal(err)
	}
	return class
}

// Each type has its own StorageClass, volumeMode and VolumeSnapshotClass
// from the admin's mapping. Fields the mapping leaves empty take the
// backend-wide values, which is what the pre-#123 config set for every type.
func TestEachTypeUsesItsOwnClassModeAndSnapshotClass(t *testing.T) {
	ctx := context.Background()
	gp3, st1 := vtype(t, "gp3"), vtype(t, "st1")
	nfsClass(t, "nfs-csi")
	b := newBlock(kube.DefaultClasses()).
		WithClass(gp3, kube.Class{
			StorageClass: "fast-rbd", VolumeMode: corev1.PersistentVolumeBlock, SnapshotClass: "fast-snap",
		}).
		WithClass(st1, kube.Class{StorageClass: "nfs-csi", VolumeMode: corev1.PersistentVolumeFilesystem})

	fast := model.NewVolumeID()
	if err := b.CreateVolume(ctx, fast, spec(t, gp3, model.GiB)); err != nil {
		t.Fatal(err)
	}
	pvc := claimFor(t, fast)
	if *pvc.Spec.StorageClassName != "fast-rbd" || *pvc.Spec.VolumeMode != corev1.PersistentVolumeBlock {
		t.Fatalf("gp3 claim class %q mode %q", *pvc.Spec.StorageClassName, *pvc.Spec.VolumeMode)
	}
	if got := pvc.Annotations[kube.VolumeTypeAnnotation]; got != "gp3" {
		t.Fatalf("type annotation %q", got)
	}
	fastSnap := model.NewSnapshotID()
	if err := b.CreateSnapshot(ctx, fastSnap, fast); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("gp3 snapshot: %v", err)
	}
	if got := snapshotClassOf(t, fastSnap); got != "fast-snap" {
		t.Fatalf("gp3 snapshot class %q", got)
	}

	std := newVolume(t, b)
	if got := *claimFor(t, std).Spec.StorageClassName; got != kube.DefaultClass {
		t.Fatalf("standard class %q", got)
	}
	stdSnap := model.NewSnapshotID()
	if err := b.CreateSnapshot(ctx, stdSnap, std); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("standard snapshot: %v", err)
	}
	if got := snapshotClassOf(t, stdSnap); got != "vws-snapclass" {
		t.Fatalf("standard snapshot class %q", got)
	}

	cold := model.NewVolumeID()
	if err := b.CreateVolume(ctx, cold, spec(t, st1, model.GiB)); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("st1 is a DataVolume: %v", err)
	}
	dv := dataVolume(t, b, cold)
	for path, want := range map[string]string{"volumeMode": "Filesystem", "storageClassName": "nfs-csi"} {
		if got, _, _ := unstructured.NestedString(dv.Object, "spec", "storage", path); got != want {
			t.Fatalf("st1 %s %q, want %q", path, got, want)
		}
	}
	if err := b.DeleteVolume(ctx, cold); err != nil {
		t.Fatal(err)
	}
}

// ServeType reads only the mapping. An unmapped type is refused with a
// reason that names it, and CreateVolume makes no object for it.
func TestUnmappedTypeIsNotServed(t *testing.T) {
	gp3, io2 := vtype(t, "gp3"), vtype(t, "io2")
	b := newBlock(kube.DefaultClasses()).WithClass(gp3, kube.Class{StorageClass: "fast-rbd"})
	for _, typ := range []model.VolumeType{model.VolumeTypeStandard, vtype(t, "gp2"), gp3} {
		if err := b.ServeType(typ); err != nil {
			t.Errorf("%s: %v", typ, err)
		}
	}
	err := b.ServeType(io2)
	if err == nil || !strings.Contains(err.Error(), "no StorageClass is mapped to volume type io2") {
		t.Fatalf("io2: %v", err)
	}
	before := claims(t)
	err = b.CreateVolume(context.Background(), model.NewVolumeID(), spec(t, io2, 4*model.GiB))
	if !errors.Is(err, backend.ErrInvalid) || !backend.IsPermanent(err) {
		t.Fatalf("create io2: %v", err)
	}
	if claims(t) != before {
		t.Fatal("claim created")
	}
}

// A claim made before #123 has no type annotation. Its snapshot class is the
// one every type of its StorageClass shares; when they disagree the backend
// refuses rather than guess.
func TestLegacyClaimSnapshotClassFollowsItsStorageClass(t *testing.T) {
	ctx := context.Background()
	gp3, io1 := vtype(t, "gp3"), vtype(t, "io1")
	legacy := func(class string) model.VolumeID {
		t.Helper()
		id := model.NewVolumeID()
		mode := corev1.PersistentVolumeBlock
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: kube.PVCName(id), Namespace: ns,
				Annotations: map[string]string{kube.VolumeAnnotation: id.String()},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: &class, VolumeMode: &mode,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		if _, err := kc.CoreV1().PersistentVolumeClaims(ns).Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	b := newBlock(kube.DefaultClasses()).WithClass(gp3, kube.Class{StorageClass: "fast-rbd", SnapshotClass: "fast-snap"})
	old := legacy(kube.DefaultClass)
	snap := model.NewSnapshotID()
	if err := b.CreateSnapshot(ctx, snap, old); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("legacy snapshot: %v", err)
	}
	if got := snapshotClassOf(t, snap); got != "vws-snapclass" {
		t.Fatalf("legacy snapshot class %q", got)
	}
	split := newBlock(kube.DefaultClasses()).
		WithClass(gp3, kube.Class{StorageClass: "shared-rbd", SnapshotClass: "snap-a"}).
		WithClass(io1, kube.Class{StorageClass: "shared-rbd", SnapshotClass: "snap-b"})
	err := split.CreateSnapshot(ctx, model.NewSnapshotID(), legacy("shared-rbd"))
	if !errors.Is(err, backend.ErrInvalid) || !backend.IsPermanent(err) {
		t.Fatalf("ambiguous legacy snapshot class: %v", err)
	}
}

// The preflight checks every mapping: a Block-mode type on a Filesystem-only
// provisioner and a type's snapshot class of another driver are named.
func TestPreflightChecksEveryTypeMapping(t *testing.T) {
	ctx := context.Background()
	gp3, st1 := vtype(t, "gp3"), vtype(t, "st1")
	nfsClass(t, "nfs-csi")
	storageClass(t, "fast-rbd", "rbd.csi.ceph.com")
	snapshotClassObject(t, "fast-snap", "rbd.csi.ceph.com")
	snapshotClassObject(t, "nfs-snap", "nfs.csi.k8s.io")
	good := newBlock(kube.DefaultClasses()).
		WithClass(gp3, kube.Class{StorageClass: "fast-rbd", SnapshotClass: "fast-snap"}).
		WithClass(st1, kube.Class{
			StorageClass: "nfs-csi", VolumeMode: corev1.PersistentVolumeFilesystem, SnapshotClass: "nfs-snap",
		})
	snapshotClassObject(t, "vws-snapclass", "rbd.csi.ceph.com")
	if err := good.Ready(ctx); err != nil {
		t.Fatalf("good mapping: %v", err)
	}
	blockNFS := newBlock(kube.DefaultClasses()).
		WithClass(st1, kube.Class{StorageClass: "nfs-csi", SnapshotClass: "nfs-snap"})
	if err := blockNFS.Ready(ctx); err == nil || !strings.Contains(err.Error(), "nfs.csi.k8s.io serves only Filesystem") {
		t.Fatalf("st1 Block on NFS: %v", err)
	}
	wrongSnap := newBlock(kube.DefaultClasses()).
		WithClass(gp3, kube.Class{StorageClass: "fast-rbd", SnapshotClass: "nfs-snap"})
	if err := wrongSnap.Ready(ctx); err == nil || !strings.Contains(err.Error(), `VolumeSnapshotClass "nfs-snap"`) {
		t.Fatalf("gp3 snapshot class of another driver: %v", err)
	}
}

// -kube-volume-type values are TYPE=STORAGECLASS[/VOLUMEMODE[/SNAPSHOTCLASS]].
func TestParseTypeClass(t *testing.T) {
	for in, want := range map[string]kube.Class{
		"gp3=fast":       {StorageClass: "fast"},
		"io1=nvme/Block": {StorageClass: "nvme", VolumeMode: corev1.PersistentVolumeBlock},
		"st1=nfs/Filesystem/nfsnap": {
			StorageClass: "nfs", VolumeMode: corev1.PersistentVolumeFilesystem, SnapshotClass: "nfsnap",
		},
		"sc1=cold//coldsnap": {StorageClass: "cold", SnapshotClass: "coldsnap"},
	} {
		typ, got, err := kube.ParseTypeClass(in)
		if err != nil || got != want || typ.String() != in[:3] {
			t.Errorf("%q: %s %+v err %v", in, typ, got, err)
		}
	}
	for _, bad := range []string{
		"", "gp3", "gp3=", "nvme=fast", "gp3=fast/Raw", "gp3=a/Block/c/d", "=fast", "gp3=/Block",
	} {
		if _, _, err := kube.ParseTypeClass(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

// Config.Types adds or replaces type mappings over Classes; a bad mode in a
// mapping fails Open.
func TestOpenAppliesTypeMappings(t *testing.T) {
	gp3 := vtype(t, "gp3")
	cfg := kube.DefaultConfig()
	cfg.Kubeconfig = writeKubeconfig(t)
	cfg.Namespace = ns
	cfg.Types = map[model.VolumeType]kube.Class{gp3: {StorageClass: "fast-rbd"}}
	b, err := kube.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.ServeType(gp3); err != nil {
		t.Fatalf("gp3: %v", err)
	}
	if err := b.ServeType(vtype(t, "io2")); err == nil {
		t.Fatal("io2 served")
	}
	cfg.Types = map[model.VolumeType]kube.Class{gp3: {StorageClass: "fast-rbd", VolumeMode: "Raw"}}
	if _, err := kube.Open(cfg); err == nil {
		t.Fatal("opened with mode Raw")
	}
}

// A claim's volumeMode cannot change either, so a retype between types of
// one StorageClass but different modes is refused.
func TestRetypeNeedsTheSameVolumeMode(t *testing.T) {
	gp3 := vtype(t, "gp3")
	b := newBlock(kube.DefaultClasses()).
		WithClass(gp3, kube.Class{StorageClass: kube.DefaultClass, VolumeMode: corev1.PersistentVolumeFilesystem})
	err := b.Retype(vtype(t, "gp2"), gp3)
	if !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), "volumeMode") {
		t.Fatalf("gp2 Block to gp3 Filesystem: %v", err)
	}
	same := newBlock(kube.DefaultClasses()).WithClass(gp3, kube.Class{StorageClass: kube.DefaultClass})
	if err := same.Retype(vtype(t, "gp2"), gp3); err != nil {
		t.Fatalf("same class and mode: %v", err)
	}
}
