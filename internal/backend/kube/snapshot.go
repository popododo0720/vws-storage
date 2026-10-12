package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
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
	return b.ready(ctx, cur)
}

// DeleteSnapshot deletes the VolumeSnapshot. An absent one is not an error.
func (b *Block) DeleteSnapshot(ctx context.Context, id model.SnapshotID) error {
	err := b.dyn.Resource(snapshotGVR).Namespace(b.ns).Delete(ctx, snapshotName(id), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (b *Block) requireReady(ctx context.Context, id model.SnapshotID) (*unstructured.Unstructured, error) {
	vs, err := b.dyn.Resource(snapshotGVR).Namespace(b.ns).Get(ctx, snapshotName(id), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, backend.Permanent(fmt.Errorf("%w: snapshot", backend.ErrNotFound))
	}
	if err != nil {
		return nil, err
	}
	return vs, b.ready(ctx, vs)
}

// snapshotErrorGrace is how long a VolumeSnapshot may keep reporting
// status.error, counted from when this backend first saw it, before it counts
// as failed. The snapshotter sets status.error on a transient failure and
// clears it when a retry succeeds, so a fresh error is not final.
var snapshotErrorGrace = 10 * time.Minute

const errorSeenAnnotation = "vws.popododo0720.io/error-first-seen"

// ready reads status.readyToUse. A missing field is not ready and is retried.
// A status.error is retried as ErrBusy until it has been observed for longer
// than snapshotErrorGrace, then the failure is permanent so the operation
// fails and the engine deletes the snapshot. The first observation time is an
// annotation on the VolumeSnapshot, so it survives a restart. It is removed
// when the error is gone.
func (b *Block) ready(ctx context.Context, vs *unstructured.Unstructured) error {
	if ok, _, _ := unstructured.NestedBool(vs.Object, "status", "readyToUse"); ok {
		return nil
	}
	msg, _, _ := unstructured.NestedString(vs.Object, "status", "error", "message")
	ann := vs.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	if msg == "" {
		if _, ok := ann[errorSeenAnnotation]; ok {
			delete(ann, errorSeenAnnotation)
			if err := b.setAnnotations(ctx, vs, ann); err != nil {
				return err
			}
		}
		return fmt.Errorf("%w: snapshot is not ready", backend.ErrBusy)
	}
	first, err := time.Parse(time.RFC3339Nano, ann[errorSeenAnnotation])
	if err != nil {
		ann[errorSeenAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
		if err := b.setAnnotations(ctx, vs, ann); err != nil {
			return err
		}
	} else if time.Since(first) > snapshotErrorGrace {
		return backend.Permanent(fmt.Errorf("kube: snapshot failed: %s", msg))
	}
	return fmt.Errorf("%w: snapshot reports an error: %s", backend.ErrBusy, msg)
}

func (b *Block) setAnnotations(ctx context.Context, vs *unstructured.Unstructured, ann map[string]string) error {
	vs.SetAnnotations(ann)
	_, err := b.dyn.Resource(snapshotGVR).Namespace(b.ns).Update(ctx, vs, metav1.UpdateOptions{})
	return err
}

// restoreSize is status.restoreSize in bytes, when the snapshotter reported it.
func restoreSize(vs *unstructured.Unstructured) (int64, bool) {
	raw, _, _ := unstructured.NestedString(vs.Object, "status", "restoreSize")
	if raw == "" {
		return 0, false
	}
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return 0, false
	}
	return q.Value(), true
}
