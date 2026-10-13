package kube

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/model"
)

var dataVolumeGVR = schema.GroupVersionResource{Group: "cdi.kubevirt.io", Version: "v1beta1", Resource: "datavolumes"}

// blankVolume creates an empty Filesystem volume as a CDI blank DataVolume of
// the claim's name. CDI writes a sparse disk.img owned by qemu (uid 107) and
// sizes the claim for the filesystem overhead. KubeVirt creates a missing
// disk.img only when a VM starts, not on hotplug, so an empty Filesystem claim
// without it cannot be hot-plugged. The volume is busy until the DataVolume
// has Succeeded.
func (b *Block) blankVolume(ctx context.Context, id model.VolumeID, spec model.VolumeSpec, class string) error {
	dvs := b.dyn.Resource(dataVolumeGVR).Namespace(b.ns)
	dv := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cdi.kubevirt.io/v1beta1",
		"kind":       "DataVolume",
		"metadata": map[string]any{
			"name":      pvcName(id),
			"namespace": b.ns,
			"annotations": map[string]any{
				VolumeAnnotation: id.String(),
				zoneAnnotation:   spec.Zone.String(),
				// A completed DataVolume must stay: it owns the claim.
				"cdi.kubevirt.io/storage.deleteAfterCompletion": "false",
			},
		},
		"spec": map[string]any{
			"source": map[string]any{"blank": map[string]any{}},
			"storage": map[string]any{
				"accessModes":      []any{string(corev1.ReadWriteOnce)},
				"storageClassName": class,
				"volumeMode":       string(corev1.PersistentVolumeFilesystem),
				"resources": map[string]any{"requests": map[string]any{
					"storage": resource.NewQuantity(int64(spec.Size), resource.BinarySI).String(),
				}},
			},
		},
	}}
	cur, err := dvs.Create(ctx, dv, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		cur, err = dvs.Get(ctx, pvcName(id), metav1.GetOptions{})
		if err == nil {
			got, _, _ := unstructured.NestedString(cur.Object, "spec", "storage", "storageClassName") //nolint:errcheck // absent is ""
			if cur.GetAnnotations()[zoneAnnotation] != spec.Zone.String() || got != class {
				return backend.Permanent(fmt.Errorf("%w: volume", backend.ErrConflict))
			}
		}
	}
	if meta.IsNoMatchError(err) {
		return fmt.Errorf("%w: the CDI DataVolume API is not installed", backend.ErrBusy)
	}
	if err != nil {
		return fmt.Errorf("kube: create data volume: %w", err)
	}
	if phase, _, _ := unstructured.NestedString(cur.Object, "status", "phase"); phase != "Succeeded" { //nolint:errcheck // absent is not ready
		return fmt.Errorf("%w: data volume phase %q", backend.ErrBusy, phase)
	}
	return nil
}

func (b *Block) deleteDataVolume(ctx context.Context, id model.VolumeID) error {
	err := b.dyn.Resource(dataVolumeGVR).Namespace(b.ns).Delete(ctx, pvcName(id), metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
		return fmt.Errorf("kube: delete data volume: %w", err)
	}
	return nil
}
