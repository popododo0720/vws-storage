package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
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
	claim, err := b.kc.CoreV1().PersistentVolumeClaims(b.ns).Get(ctx, pvcName(source), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return backend.Permanent(fmt.Errorf("%w: volume", backend.ErrNotFound))
	}
	if err != nil {
		return fmt.Errorf("kube: get claim: %w", err)
	}
	snapshotClass, err := b.snapshotClassFor(ctx, claim)
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
				SnapshotAnnotation:   id.String(),
				sourceAnnotation:     source.String(),
				sourceModeAnnotation: string(claimMode(claim)),
			},
		},
		"spec": map[string]any{
			"volumeSnapshotClassName": snapshotClass,
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
		return fmt.Errorf("kube: create snapshot: %w", err)
	}
	return b.ready(ctx, cur)
}

// snapshotClassFor is the VolumeSnapshotClass of the claim's volume type.
// The type is the claim's annotation, else its DataVolume's. A claim from
// before vws-storage#123 has neither: then it is the class every type of
// the claim's StorageClass shares, and when they differ it fails rather
// than guess. No class means the type takes no snapshots.
func (b *Block) snapshotClassFor(ctx context.Context, claim *corev1.PersistentVolumeClaim) (string, error) {
	name := claim.Annotations[VolumeTypeAnnotation]
	if name == "" {
		dv, err := b.dyn.Resource(dataVolumeGVR).Namespace(b.ns).Get(ctx, claim.Name, metav1.GetOptions{})
		if err == nil {
			name = dv.GetAnnotations()[VolumeTypeAnnotation]
		}
	}
	none := backend.Permanent(fmt.Errorf("%w: this storage backend has no VolumeSnapshotClass for the volume",
		backend.ErrInvalid))
	if name != "" {
		typ, err := model.ParseVolumeType(name)
		if err != nil {
			return "", backend.Permanent(fmt.Errorf("%w: claim annotation %s=%q",
				backend.ErrInvalid, VolumeTypeAnnotation, name))
		}
		c, ok := b.classFor(typ)
		if !ok {
			return "", backend.Permanent(fmt.Errorf("%w: no StorageClass is mapped to volume type %s", backend.ErrInvalid, typ))
		}
		if c.SnapshotClass == "" {
			return "", none
		}
		return c.SnapshotClass, nil
	}
	var sc string
	if claim.Spec.StorageClassName != nil {
		sc = *claim.Spec.StorageClassName
	}
	found := map[string]bool{}
	for _, c := range b.mapped() {
		if c.StorageClass == sc {
			found[c.SnapshotClass] = true
		}
	}
	switch {
	case len(found) > 1:
		return "", backend.Permanent(fmt.Errorf(
			"%w: the claim has no volume type, and the types of StorageClass %q have different VolumeSnapshotClasses",
			backend.ErrInvalid, sc))
	case len(found) == 0 || found[""]:
		return "", none
	}
	for class := range found {
		return class, nil
	}
	return "", none
}

// claimMode is the claim's volumeMode. An unset mode is Filesystem.
func claimMode(c *corev1.PersistentVolumeClaim) corev1.PersistentVolumeMode {
	if c.Spec.VolumeMode == nil {
		return corev1.PersistentVolumeFilesystem
	}
	return *c.Spec.VolumeMode
}

// restoreMode is the volumeMode for a claim restored from vs: the mode of the
// claim the snapshot was taken from, so the restored disk is laid out the same
// way. A snapshot without the annotation predates Block volumes.
func restoreMode(vs *unstructured.Unstructured) corev1.PersistentVolumeMode {
	if m := corev1.PersistentVolumeMode(vs.GetAnnotations()[sourceModeAnnotation]); m == corev1.PersistentVolumeBlock {
		return m
	}
	return corev1.PersistentVolumeFilesystem
}

// SnapshotObjectName is the name of the VolumeSnapshot that backs id.
func (b *Block) SnapshotObjectName(id model.SnapshotID) string { return snapshotName(id) }

// DeleteSnapshot deletes the VolumeSnapshot. An absent one is not an error.
func (b *Block) DeleteSnapshot(ctx context.Context, id model.SnapshotID) error {
	err := b.dyn.Resource(snapshotGVR).Namespace(b.ns).Delete(ctx, snapshotName(id), metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("kube: delete snapshot: %w", err)
	}
	return nil
}

func (b *Block) requireReady(ctx context.Context, id model.SnapshotID) (*unstructured.Unstructured, error) {
	vs, err := b.dyn.Resource(snapshotGVR).Namespace(b.ns).Get(ctx, snapshotName(id), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, backend.Permanent(fmt.Errorf("%w: snapshot", backend.ErrNotFound))
	}
	if err != nil {
		return nil, fmt.Errorf("kube: get snapshot: %w", err)
	}
	return vs, b.ready(ctx, vs)
}

// snapshotErrorGrace is how long a VolumeSnapshot may keep reporting
// status.error, counted from when this backend first saw it, before it counts
// as failed. The snapshotter sets status.error on a transient failure and
// clears it when a retry succeeds, so a fresh error is not final.
var snapshotErrorGrace = 10 * time.Minute

// ready reads status.readyToUse. A missing field is not ready and is retried.
// A status.error is retried as ErrBusy until it has been observed for longer
// than snapshotErrorGrace, then the failure is permanent so the operation
// fails and the engine deletes the snapshot. The first observation time is an
// annotation on the VolumeSnapshot, so it survives a restart. It is removed
// when the error is gone.
func (b *Block) ready(ctx context.Context, vs *unstructured.Unstructured) error {
	if nestedBool(vs, "status", "readyToUse") {
		return nil
	}
	msg := nestedString(vs, "status", "error", "message")
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
	if _, err := b.dyn.Resource(snapshotGVR).Namespace(b.ns).Update(ctx, vs, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("kube: update snapshot annotations: %w", err)
	}
	return nil
}

// nestedBool and nestedString read a field the snapshotter writes. A value of
// the wrong type is read as absent, the same as a missing field.
func nestedBool(vs *unstructured.Unstructured, fields ...string) bool {
	v, _, err := unstructured.NestedBool(vs.Object, fields...)
	return err == nil && v
}

func nestedString(vs *unstructured.Unstructured, fields ...string) string {
	v, _, err := unstructured.NestedString(vs.Object, fields...)
	if err != nil {
		return ""
	}
	return v
}

// restoreSize is status.restoreSize in bytes, when the snapshotter reported it.
func restoreSize(vs *unstructured.Unstructured) (int64, bool) {
	raw := nestedString(vs, "status", "restoreSize")
	if raw == "" {
		return 0, false
	}
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return 0, false
	}
	return q.Value(), true
}
