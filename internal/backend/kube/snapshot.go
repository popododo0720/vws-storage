package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/model"
)

var snapshotGVR = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}

func snapshotName(id model.SnapshotID) string {
	sum := sha256.Sum256([]byte(id.String()))
	return "snap-" + hex.EncodeToString(sum[:16])
}

// CreateSnapshot creates the VolumeSnapshot and returns ErrBusy until
// status.readyToUse is true, so the engine retries until the snapshot is usable.
func (b *Block) CreateSnapshot(ctx context.Context, id model.SnapshotID, source model.VolumeID) error {
	_, err := b.kc.CoreV1().PersistentVolumeClaims(b.ns).Get(ctx, pvcName(source), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return backend.Permanent(fmt.Errorf("%w: volume", backend.ErrNotFound))
	}
	if err != nil {
		return err
	}
	snaps := b.dyn.Resource(snapshotGVR).Namespace(b.ns)
	vs := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind":       "VolumeSnapshot",
		"metadata": map[string]any{
			"name":      snapshotName(id),
			"namespace": b.ns,
			"annotations": map[string]any{
				SnapshotAnnotation: id.String(),
				sourceAnnotation:   source.String(),
			},
		},
		"spec": map[string]any{
			"volumeSnapshotClassName": b.snapshotClass,
			"source":                  map[string]any{"persistentVolumeClaimName": pvcName(source)},
		},
	}}
	cur, err := snaps.Create(ctx, vs, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		cur, err = snaps.Get(ctx, snapshotName(id), metav1.GetOptions{})
		if err == nil && cur.GetAnnotations()[sourceAnnotation] != source.String() {
			return backend.Permanent(fmt.Errorf("%w: snapshot", backend.ErrConflict))
		}
	}
	if err != nil {
		return err
	}
	return ready(cur)
}

// SnapshotObjectName is the name of the VolumeSnapshot that backs id.
func (b *Block) SnapshotObjectName(id model.SnapshotID) string { return snapshotName(id) }

// DeleteSnapshot deletes the VolumeSnapshot. An absent one is not an error.
func (b *Block) DeleteSnapshot(ctx context.Context, id model.SnapshotID) error {
	err := b.dyn.Resource(snapshotGVR).Namespace(b.ns).Delete(ctx, snapshotName(id), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (b *Block) requireReady(ctx context.Context, id model.SnapshotID) error {
	vs, err := b.dyn.Resource(snapshotGVR).Namespace(b.ns).Get(ctx, snapshotName(id), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return backend.Permanent(fmt.Errorf("%w: snapshot", backend.ErrNotFound))
	}
	if err != nil {
		return err
	}
	return ready(vs)
}

// ready reads only status.readyToUse. A missing field is not ready.
func ready(vs *unstructured.Unstructured) error {
	ok, _, _ := unstructured.NestedBool(vs.Object, "status", "readyToUse")
	if !ok {
		return fmt.Errorf("%w: snapshot is not ready", backend.ErrBusy)
	}
	return nil
}
