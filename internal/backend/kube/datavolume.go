package kube

import (
	"context"
	"fmt"
	"strconv"
	"time"

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

// DefaultDataVolumeRetries is how many times a Failed DataVolume is made
// again. CDI already retries a failing importer pod before it reports Failed,
// so Failed is its final word on that object.
const DefaultDataVolumeRetries = 3

// DefaultDataVolumeTimeout bounds the time from the first DataVolume to
// Succeeded. A blank disk.img is a sparse file written in seconds; the time
// goes to binding the claim and pulling the importer image on a cold node,
// minutes at most. It matches snapshotErrorGrace, the other wait on a CSI
// object this backend bounds.
const DefaultDataVolumeTimeout = 10 * time.Minute

// dvRetry is the retry state of one volume's DataVolume.
type dvRetry struct {
	attempt int
	first   time.Time
}

// dvSource is what a DataVolume fills its claim from.
type dvSource struct {
	// field is the DataVolume spec key, "source" or "sourceRef", and value
	// its content.
	field string
	value map[string]any
	// key names the source for the AlreadyExists comparison: empty for
	// blank, the image name for a clone.
	key string
	// mode is the claim volumeMode.
	mode corev1.PersistentVolumeMode
	// annotations are added to the DataVolume.
	annotations map[string]string
}

func blankSource() dvSource {
	return dvSource{field: "source", value: map[string]any{"blank": map[string]any{}}, mode: corev1.PersistentVolumeFilesystem}
}

// blankVolume creates an empty Filesystem volume as a CDI blank DataVolume of
// the claim's name. CDI writes a sparse disk.img owned by qemu (uid 107) and
// sizes the claim for the filesystem overhead. KubeVirt creates a missing
// disk.img only when a VM starts, not on hotplug, so an empty Filesystem claim
// without it cannot be hot-plugged. The volume is busy until the DataVolume
// has Succeeded.
//
// Failed is final for a DataVolume: CDI has retried its pod already and the
// validation webhook rejects spec updates, so the documented retry is to
// delete it and create it again. That happens up to dvRetries times, and the
// whole wait is bounded by dvTimeout from the first DataVolume. Past either,
// the error is permanent ErrGaveUp, and the volume is kept in error with the
// DataVolume and claim until DeleteVolume.
func (b *Block) blankVolume(ctx context.Context, id model.VolumeID, spec model.VolumeSpec, class string) error {
	return b.dataVolume(ctx, id, spec, class, blankSource())
}

// dataVolume makes the volume a DataVolume from src and waits for it to
// Succeed, with the retry and timeout described on blankVolume.
func (b *Block) dataVolume(ctx context.Context, id model.VolumeID, spec model.VolumeSpec, class string, src dvSource) error {
	cur, err := b.createDataVolume(ctx, id, spec, class, src)
	if err != nil {
		return err
	}
	if cur.GetDeletionTimestamp() != nil {
		return fmt.Errorf("%w: data volume is being deleted for a retry", backend.ErrBusy)
	}
	phase, _, _ := unstructured.NestedString(cur.Object, "status", "phase") //nolint:errcheck // absent is not ready
	if phase == "Succeeded" {
		return nil
	}
	r := retryOf(cur)
	if time.Since(r.first) > b.dvTimeout {
		return backend.Permanent(fmt.Errorf("%w: the CDI DataVolume did not succeed within %s (attempt %d, phase %q)", backend.ErrGaveUp, b.dvTimeout, r.attempt, phase))
	}
	if phase != "Failed" {
		return fmt.Errorf("%w: data volume phase %q", backend.ErrBusy, phase)
	}
	if r.attempt > b.dvRetries {
		return backend.Permanent(fmt.Errorf("%w: the CDI DataVolume failed %d times (retry limit %d)", backend.ErrGaveUp, r.attempt, b.dvRetries))
	}
	// Retry: remember the state, delete the Failed object and its claim (it
	// holds a blank image, no data), and create the next one.
	b.mu.Lock()
	b.carried[pvcName(id)] = dvRetry{attempt: r.attempt + 1, first: r.first}
	b.mu.Unlock()
	if err := b.deleteDataVolume(ctx, id); err != nil {
		return err
	}
	if err := b.kc.CoreV1().PersistentVolumeClaims(b.ns).Delete(ctx, pvcName(id), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("kube: delete failed claim: %w", err)
	}
	if _, err := b.createDataVolume(ctx, id, spec, class, src); err != nil {
		return err
	}
	return fmt.Errorf("%w: data volume Failed; made attempt %d of %d", backend.ErrBusy, r.attempt+1, b.dvRetries+1)
}

// retryOf reads the retry state of a DataVolume. One without the annotations
// predates this code: it is attempt 1, created at its creation time.
func retryOf(dv *unstructured.Unstructured) dvRetry {
	ann := dv.GetAnnotations()
	r := dvRetry{attempt: 1, first: dv.GetCreationTimestamp().Time}
	if n, err := strconv.Atoi(ann[dvAttemptAnnotation]); err == nil && n > 0 {
		r.attempt = n
	}
	if t, err := time.Parse(time.RFC3339Nano, ann[dvFirstAnnotation]); err == nil {
		r.first = t
	}
	return r
}

// createDataVolume creates the blank DataVolume of id, or returns the one
// that exists if it matches. A new one takes the carried retry state, if any.
func (b *Block) createDataVolume(ctx context.Context, id model.VolumeID, spec model.VolumeSpec, class string, src dvSource) (*unstructured.Unstructured, error) {
	name := pvcName(id)
	b.mu.Lock()
	r, ok := b.carried[name]
	b.mu.Unlock()
	if !ok {
		r = dvRetry{attempt: 1, first: time.Now()}
	}
	dvs := b.dyn.Resource(dataVolumeGVR).Namespace(b.ns)
	dv := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cdi.kubevirt.io/v1beta1",
		"kind":       "DataVolume",
		"metadata": map[string]any{
			"name":      name,
			"namespace": b.ns,
			"annotations": map[string]any{
				VolumeAnnotation:     id.String(),
				zoneAnnotation:       spec.Zone.String(),
				dvAttemptAnnotation:  strconv.Itoa(r.attempt),
				dvFirstAnnotation:    r.first.UTC().Format(time.RFC3339Nano),
				sourceAnnotation:     src.key,
				VolumeTypeAnnotation: spec.Type.String(),
				// A completed DataVolume must stay: it owns the claim.
				"cdi.kubevirt.io/storage.deleteAfterCompletion": "false",
			},
		},
		"spec": map[string]any{
			src.field: src.value,
			"storage": map[string]any{
				"accessModes":      []any{string(corev1.ReadWriteOnce)},
				"storageClassName": class,
				"volumeMode":       string(src.mode),
				"resources": map[string]any{"requests": map[string]any{
					"storage": resource.NewQuantity(int64(spec.Size), resource.BinarySI).String(),
				}},
			},
		},
	}}
	ann := dv.GetAnnotations()
	for k, v := range src.annotations {
		ann[k] = v
	}
	dv.SetAnnotations(ann)
	cur, err := dvs.Create(ctx, dv, metav1.CreateOptions{})
	if err == nil {
		b.mu.Lock()
		delete(b.carried, name)
		b.mu.Unlock()
	}
	if apierrors.IsAlreadyExists(err) {
		cur, err = dvs.Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			got, _, _ := unstructured.NestedString(cur.Object, "spec", "storage", "storageClassName") //nolint:errcheck // absent is ""
			// A DataVolume from before the source annotation is a blank one.
			if cur.GetAnnotations()[zoneAnnotation] != spec.Zone.String() || got != class || cur.GetAnnotations()[sourceAnnotation] != src.key {
				return nil, backend.Permanent(fmt.Errorf("%w: volume", backend.ErrConflict))
			}
		}
	}
	if meta.IsNoMatchError(err) {
		return nil, fmt.Errorf("%w: the CDI DataVolume API is not installed", backend.ErrBusy)
	}
	if err != nil {
		return nil, fmt.Errorf("kube: create data volume: %w", err)
	}
	return cur, nil
}

func (b *Block) deleteDataVolume(ctx context.Context, id model.VolumeID) error {
	err := b.dyn.Resource(dataVolumeGVR).Namespace(b.ns).Delete(ctx, pvcName(id), metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
		return fmt.Errorf("kube: delete data volume: %w", err)
	}
	return nil
}
