package kube_test

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/model"
)

var snapClassGVR = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotclasses"}

func TestReadyNamesWhatIsMissing(t *testing.T) {
	ctx := context.Background()
	class := kube.DefaultClasses()[model.VolumeTypeStandard]
	good := kube.Classes{model.VolumeTypeStandard: class, model.VolumeTypeSSD: class}

	// Nothing but the StorageClass exists: the snapshot class is reported, and the
	// namespace and StorageClass are not.
	err := kube.NewBlock(kc, dyn, ns, good, "ready-snapclass").Ready(ctx)
	if err == nil || !strings.Contains(err.Error(), `VolumeSnapshotClass "ready-snapclass"`) ||
		strings.Contains(err.Error(), "namespace") || strings.Contains(err.Error(), "StorageClass") {
		t.Fatalf("missing snapshot class: %v", err)
	}

	vsc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion":     "snapshot.storage.k8s.io/v1",
		"kind":           "VolumeSnapshotClass",
		"metadata":       map[string]any{"name": "ready-snapclass"},
		"driver":         "rbd.csi.ceph.com",
		"deletionPolicy": "Delete",
	}}
	if _, err := dyn.Resource(snapClassGVR).Create(ctx, vsc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := kube.NewBlock(kc, dyn, ns, good, "ready-snapclass").Ready(ctx); err != nil {
		t.Fatalf("everything exists: %v", err)
	}

	bad := kube.Classes{model.VolumeTypeStandard: class, model.VolumeTypeSSD: "no-such-class"}
	err = kube.NewBlock(kc, dyn, "no-such-ns", bad, "ready-snapclass").Ready(ctx)
	if err == nil || !strings.Contains(err.Error(), `namespace "no-such-ns"`) || !strings.Contains(err.Error(), `StorageClass "no-such-class"`) {
		t.Fatalf("missing namespace and class: %v", err)
	}
}
