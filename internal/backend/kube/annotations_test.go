package kube_test

import (
	"context"
	"strings"
	"testing"

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
