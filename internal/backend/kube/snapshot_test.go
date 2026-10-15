package kube_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

var snapGVR = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}

func snapshots(t *testing.T) []unstructured.Unstructured {
	t.Helper()
	l, err := dyn.Resource(snapGVR).Namespace(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return l.Items
}

func snapshotFor(t *testing.T, id model.SnapshotID) *unstructured.Unstructured {
	t.Helper()
	for _, s := range snapshots(t) {
		if s.GetAnnotations()[kube.SnapshotAnnotation] == id.String() {
			return &s
		}
	}
	return nil
}

func markReady(t *testing.T, id model.SnapshotID, ready bool) {
	t.Helper()
	vs := snapshotFor(t, id)
	if vs == nil {
		t.Fatal("no snapshot")
	}
	if err := unstructured.SetNestedField(vs.Object, ready, "status", "readyToUse"); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(snapGVR).Namespace(ns).UpdateStatus(context.Background(), vs, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func newVolume(t *testing.T, b *kube.Block) model.VolumeID {
	t.Helper()
	id := model.NewVolumeID()
	if err := b.CreateVolume(context.Background(), id, spec(t, model.VolumeTypeStandard, model.GiB)); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestCreateSnapshotWaitsForReadyToUseAndIsIdempotent(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	vol := newVolume(t, b)
	snap := model.NewSnapshotID()
	before := len(snapshots(t))
	for range 2 {
		if err := b.CreateSnapshot(ctx, snap, vol); !errors.Is(err, backend.ErrBusy) || backend.IsPermanent(err) {
			t.Fatalf("not ready: %v", err)
		}
	}
	if got := len(snapshots(t)) - before; got != 1 {
		t.Fatalf("snapshots created %d", got)
	}
	markReady(t, snap, true)
	if err := b.CreateSnapshot(ctx, snap, vol); err != nil {
		t.Fatalf("ready: %v", err)
	}
	err := b.CreateSnapshot(ctx, snap, newVolume(t, b))
	if !errors.Is(err, backend.ErrConflict) || !backend.IsPermanent(err) {
		t.Fatalf("other source: %v", err)
	}
}

func TestCreateSnapshotOfMissingVolumeIsNotFound(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	err := b.CreateSnapshot(context.Background(), model.NewSnapshotID(), model.NewVolumeID())
	if !errors.Is(err, backend.ErrNotFound) || !backend.IsPermanent(err) {
		t.Fatalf("err %v", err)
	}
}

func TestDeleteSnapshotIsIdempotent(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	snap := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, snap, newVolume(t, b))
	for range 2 {
		if err := b.DeleteSnapshot(ctx, snap); err != nil {
			t.Fatal(err)
		}
	}
	if snapshotFor(t, snap) != nil {
		t.Fatal("snapshot remains")
	}
	if err := b.DeleteSnapshot(ctx, model.NewSnapshotID()); err != nil {
		t.Fatalf("absent: %v", err)
	}
}

func TestRestoreUsesSnapshotDataSourceOnlyWhenReady(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	snap := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, snap, newVolume(t, b))
	src, err := model.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	s := spec(t, model.VolumeTypeStandard, model.GiB)
	s.Source = src
	id := model.NewVolumeID()
	if err := b.CreateVolume(ctx, id, s); !errors.Is(err, backend.ErrBusy) || backend.IsPermanent(err) {
		t.Fatalf("not ready: %v", err)
	}
	if claimFor(t, id) != nil {
		t.Fatal("claim created before ready")
	}
	markReady(t, snap, true)
	for range 2 {
		if err := b.CreateVolume(ctx, id, s); err != nil {
			t.Fatal(err)
		}
	}
	ds := claimFor(t, id).Spec.DataSource
	if ds == nil || ds.Kind != "VolumeSnapshot" || *ds.APIGroup != "snapshot.storage.k8s.io" || ds.Name != snapshotFor(t, snap).GetName() {
		t.Fatalf("data source %+v", ds)
	}
	missing, _ := model.FromSnapshot(model.NewSnapshotID())
	s.Source = missing
	if err := b.CreateVolume(ctx, model.NewVolumeID(), s); !errors.Is(err, backend.ErrNotFound) || !backend.IsPermanent(err) {
		t.Fatalf("missing: %v", err)
	}
}

func TestDeletingSourceVolumeKeepsSnapshot(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	vol := newVolume(t, b)
	snap := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, snap, vol)
	if err := b.DeleteVolume(ctx, vol); err != nil {
		t.Fatal(err)
	}
	if snapshotFor(t, snap) == nil {
		t.Fatal("snapshot deleted with its volume")
	}
}

func setStatus(t *testing.T, id model.SnapshotID, field map[string]any) {
	t.Helper()
	vs := snapshotFor(t, id)
	if vs == nil {
		t.Fatal("no snapshot")
	}
	for k, v := range field {
		if err := unstructured.SetNestedField(vs.Object, v, "status", k); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dyn.Resource(snapGVR).Namespace(ns).UpdateStatus(context.Background(), vs, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreRejectsSizeBelowRestoreSize(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	snap := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, snap, newVolume(t, b))
	setStatus(t, snap, map[string]any{"readyToUse": true, "restoreSize": "2Gi"})
	src, err := model.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	s := spec(t, model.VolumeTypeStandard, model.GiB)
	s.Source = src
	id := model.NewVolumeID()
	if err := b.CreateVolume(ctx, id, s); !errors.Is(err, backend.ErrInvalid) || !backend.IsPermanent(err) {
		t.Fatalf("smaller than restoreSize: %v", err)
	}
	if claimFor(t, id) != nil {
		t.Fatal("claim created for a too-small restore")
	}
	s.Size = 2 * model.GiB
	if err := b.CreateVolume(ctx, id, s); err != nil {
		t.Fatalf("equal to restoreSize: %v", err)
	}
}

func TestSnapshotErrorIsRetriedThenFailsPermanently(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	snap := model.NewSnapshotID()
	vol := newVolume(t, b)
	_ = b.CreateSnapshot(ctx, snap, vol)
	defer kube.SetSnapshotErrorGrace(100 * time.Millisecond)()
	time.Sleep(150 * time.Millisecond)

	setStatus(t, snap, map[string]any{"error": map[string]any{"message": "cannot snapshot"}})
	err := b.CreateSnapshot(ctx, snap, vol)
	if !errors.Is(err, backend.ErrBusy) || backend.IsPermanent(err) {
		t.Fatalf("first sight of an error on an old snapshot: %v", err)
	}
	if snapshotFor(t, snap).GetAnnotations()[kube.ErrorSeenAnnotation] == "" {
		t.Fatal("first observation not recorded")
	}

	time.Sleep(150 * time.Millisecond)
	err = b.CreateSnapshot(ctx, snap, vol)
	if err == nil || errors.Is(err, backend.ErrBusy) || !backend.IsPermanent(err) {
		t.Fatalf("error seen past the grace: %v", err)
	}
}

func TestSnapshotErrorClockResetsWhenTheErrorClears(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	snap := model.NewSnapshotID()
	vol := newVolume(t, b)
	_ = b.CreateSnapshot(ctx, snap, vol)
	defer kube.SetSnapshotErrorGrace(100 * time.Millisecond)()

	setStatus(t, snap, map[string]any{"error": map[string]any{"message": "cannot snapshot"}})
	_ = b.CreateSnapshot(ctx, snap, vol)
	time.Sleep(150 * time.Millisecond)
	setStatus(t, snap, map[string]any{"error": nil})
	if err := b.CreateSnapshot(ctx, snap, vol); !errors.Is(err, backend.ErrBusy) || backend.IsPermanent(err) {
		t.Fatalf("cleared error: %v", err)
	}
	if _, ok := snapshotFor(t, snap).GetAnnotations()[kube.ErrorSeenAnnotation]; ok {
		t.Fatal("clock kept after the error cleared")
	}
	setStatus(t, snap, map[string]any{"error": map[string]any{"message": "again"}})
	if err := b.CreateSnapshot(ctx, snap, vol); !errors.Is(err, backend.ErrBusy) || backend.IsPermanent(err) {
		t.Fatalf("recurring error restarts the clock: %v", err)
	}
	setStatus(t, snap, map[string]any{"readyToUse": true})
	if err := b.CreateSnapshot(ctx, snap, vol); err != nil {
		t.Fatalf("recovered snapshot: %v", err)
	}
}

// A restore from a snapshot whose error outlasts the grace fails for good,
// as CreateSnapshot does, instead of returning ErrBusy forever, and makes no
// claim (#19).
func TestRestoreFromAFailedSnapshotIsPermanent(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	ctx := context.Background()
	snap := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, snap, newVolume(t, b))
	defer kube.SetSnapshotErrorGrace(100 * time.Millisecond)()
	setStatus(t, snap, map[string]any{"error": map[string]any{"message": "cannot snapshot"}})
	src, err := model.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	s := spec(t, model.VolumeTypeStandard, model.GiB)
	s.Source = src
	id := model.NewVolumeID()
	if err := b.CreateVolume(ctx, id, s); !errors.Is(err, backend.ErrBusy) || backend.IsPermanent(err) {
		t.Fatalf("fresh error: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if err := b.CreateVolume(ctx, id, s); err == nil || errors.Is(err, backend.ErrBusy) || !backend.IsPermanent(err) || !strings.Contains(err.Error(), "cannot snapshot") {
		t.Fatalf("error past the grace: %v", err)
	}
	if claimFor(t, id) != nil {
		t.Fatal("claim created from a failed snapshot")
	}
}
