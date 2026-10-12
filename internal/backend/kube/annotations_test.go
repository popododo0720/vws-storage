package kube_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

func TestAnnotationKeysUseTheSharedPrefix(t *testing.T) {
	b := newBlock(kube.DefaultClasses())
	vol := newVolume(t, b)
	snap := model.NewSnapshotID()
	_ = b.CreateSnapshot(context.Background(), snap, vol)

	pvc := claimFor(t, vol)
	want := map[string]string{
		"vws.popododo0720.io/volume": vol.String(),
		"vws.popododo0720.io/zone":   "az-1",
		"vws.popododo0720.io/source": "",
	}
	for k, v := range want {
		if got, ok := pvc.Annotations[k]; !ok || got != v {
			t.Errorf("pvc %s = %q, %v; want %q", k, got, ok, v)
		}
	}
	vs := snapshotFor(t, snap)
	if got := vs.GetAnnotations()["vws.popododo0720.io/snapshot"]; got != snap.String() {
		t.Errorf("snapshot name annotation %q", got)
	}
	if got := vs.GetAnnotations()["vws.popododo0720.io/source"]; got != vol.String() {
		t.Errorf("snapshot source annotation %q", got)
	}
	for k := range pvc.Annotations {
		if strings.HasPrefix(k, "vws.storage/") {
			t.Errorf("legacy key written: %s", k)
		}
	}
}

func TestRepeatOnObjectsWithLegacyAnnotationsIsTheSameResource(t *testing.T) {
	ctx := context.Background()
	b := newBlock(kube.DefaultClasses())
	vol := model.NewVolumeID()
	sp := spec(t, model.VolumeTypeStandard, model.GiB)
	if err := b.CreateVolume(ctx, vol, sp); err != nil {
		t.Fatal(err)
	}
	snap := model.NewSnapshotID()
	_ = b.CreateSnapshot(ctx, snap, vol)
	markReady(t, snap, true)

	pvc := claimFor(t, vol)
	pvc.Annotations = map[string]string{
		"vws.storage/volume":          vol.String(),
		"vws.storage/zone":            "az-1",
		"vws.storage/source-snapshot": "",
	}
	if _, err := kc.CoreV1().PersistentVolumeClaims(ns).Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	vs := snapshotFor(t, snap)
	vs.SetAnnotations(map[string]string{
		"vws.storage/snapshot":      snap.String(),
		"vws.storage/source-volume": vol.String(),
	})
	if _, err := dyn.Resource(snapGVR).Namespace(ns).Update(ctx, vs, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := b.CreateVolume(ctx, vol, sp); err != nil {
		t.Fatalf("repeat CreateVolume on legacy claim: %v", err)
	}
	if err := b.CreateSnapshot(ctx, snap, vol); err != nil {
		t.Fatalf("repeat CreateSnapshot on legacy snapshot: %v", err)
	}
	if err := b.CreateSnapshot(ctx, snap, newVolume(t, b)); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("other source on legacy snapshot: %v", err)
	}
	other := sp
	other.Size = 2 * model.GiB
	if err := b.CreateVolume(ctx, vol, other); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("changed size on legacy claim: %v", err)
	}
}
