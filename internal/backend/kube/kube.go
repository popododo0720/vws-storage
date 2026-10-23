// Package kube is the block backend over Kubernetes. A volume is one
// PersistentVolumeClaim whose StorageClass comes from the type table.
package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/model"
)

// Classes maps a volume type to a StorageClass name, with the backend-wide
// volumeMode and VolumeSnapshotClass. WithClass maps a type with its own.
// Together they are the one place that decides which CSI driver serves a
// type; a type in neither is not served.
type Classes map[model.VolumeType]string

// Class is the cluster objects behind one volume type. An empty VolumeMode
// or SnapshotClass takes the backend-wide value (WithVolumeMode, NewBlock).
type Class struct {
	StorageClass  string
	VolumeMode    corev1.PersistentVolumeMode
	SnapshotClass string
}

// DefaultClass is the StorageClass name every type maps to by default.
const DefaultClass = "ceph-block"

// DefaultClasses serves every type from the Rook Ceph RBD class.
func DefaultClasses() Classes {
	return Classes{
		model.VolumeTypeStandard: DefaultClass,
		model.VolumeTypeGP2:      DefaultClass,
	}
}

// Block is the PVC-backed volume backend.
type Block struct {
	kc            kubernetes.Interface
	dyn           dynamic.Interface
	ns            string
	classes       Classes
	types         map[model.VolumeType]Class
	snapshotClass string
	mode          corev1.PersistentVolumeMode
	dvRetries     int
	dvTimeout     time.Duration
	imageNS       string

	// carried holds the retry state of a DataVolume deleted after Failed until
	// its replacement is created. Only for that gap; it is lost on restart,
	// and then the next attempt counts from 1 again.
	mu      sync.Mutex
	carried map[string]dvRetry
}

var (
	_ backend.Block      = (*Block)(nil)
	_ backend.Retyper    = (*Block)(nil)
	_ backend.TypeServer = (*Block)(nil)
)

// NewBlock returns a backend that creates Block-mode PVCs and VolumeSnapshots
// in namespace ns. snapshotClass is the VolumeSnapshotClass for new
// snapshots; empty means the backend takes no snapshots.
func NewBlock(kc kubernetes.Interface, dyn dynamic.Interface, ns string, classes Classes, snapshotClass string) *Block {
	return &Block{
		kc: kc, dyn: dyn, ns: ns, classes: classes, types: map[model.VolumeType]Class{},
		snapshotClass: snapshotClass, mode: corev1.PersistentVolumeBlock,
		dvRetries: DefaultDataVolumeRetries, dvTimeout: DefaultDataVolumeTimeout, carried: map[string]dvRetry{},
		imageNS: DefaultImageNamespace,
	}
}

// WithDataVolumeRetry sets how many times a Failed DataVolume is made again
// and how long a volume may take to reach Succeeded from its first
// DataVolume. Past either, CreateVolume fails permanently with ErrGaveUp.
func (b *Block) WithDataVolumeRetry(retries int, timeout time.Duration) *Block {
	b.dvRetries, b.dvTimeout = retries, timeout
	return b
}

// WithImageNamespace sets the namespace of the image DataSources an image
// volume clones from.
func (b *Block) WithImageNamespace(ns string) *Block {
	b.imageNS = ns
	return b
}

// WithVolumeMode sets the volumeMode of new empty volumes. Filesystem is for
// a provisioner that serves only Filesystem (NFS): such a volume is a CDI
// blank DataVolume, so it holds the disk.img KubeVirt needs.
func (b *Block) WithVolumeMode(m corev1.PersistentVolumeMode) *Block {
	b.mode = m
	return b
}

// WithClass maps type t to its own Class, replacing what Classes maps it to.
func (b *Block) WithClass(t model.VolumeType, c Class) *Block {
	b.types[t] = c
	return b
}

// classFor is the resolved mapping of t: every field set.
func (b *Block) classFor(t model.VolumeType) (Class, bool) {
	c, ok := b.types[t]
	if !ok {
		sc, mapped := b.classes[t]
		if !mapped {
			return Class{}, false
		}
		c = Class{StorageClass: sc}
	}
	if c.StorageClass == "" {
		return Class{}, false
	}
	if c.VolumeMode == "" {
		c.VolumeMode = b.mode
	}
	if c.SnapshotClass == "" {
		c.SnapshotClass = b.snapshotClass
	}
	return c, true
}

// mapped is every served type with its resolved Class.
func (b *Block) mapped() map[model.VolumeType]Class {
	out := map[model.VolumeType]Class{}
	for _, t := range model.VolumeTypes() {
		if c, ok := b.classFor(t); ok {
			out[t] = c
		}
	}
	return out
}

// ServeType reports whether new volumes of type t can be made: t needs a
// StorageClass in the mapping. It reads only configuration.
func (b *Block) ServeType(t model.VolumeType) error {
	if _, ok := b.classFor(t); !ok {
		return fmt.Errorf("%w: no StorageClass is mapped to volume type %s", backend.ErrInvalid, t)
	}
	return nil
}

// Retype implements backend.Retyper. A claim's StorageClass and volumeMode
// cannot change, and its annotation keeps the snapshot class of its type, so a
// type change is allowed only when both types map to the same class, mode and
// snapshot class.
func (b *Block) Retype(from, to model.VolumeType) error {
	cf, okf := b.classFor(from)
	ct, okt := b.classFor(to)
	switch {
	case !okt:
		return fmt.Errorf("%w: volume type %q", backend.ErrInvalid, to)
	case !okf || cf.StorageClass != ct.StorageClass:
		return fmt.Errorf("%w: volume type %q is StorageClass %q, and a claim's StorageClass cannot change",
			backend.ErrInvalid, to, ct.StorageClass)
	case cf.VolumeMode != ct.VolumeMode:
		return fmt.Errorf("%w: volume type %q is volumeMode %s, and a claim's volumeMode cannot change",
			backend.ErrInvalid, to, ct.VolumeMode)
	case cf.SnapshotClass != ct.SnapshotClass:
		return fmt.Errorf("%w: volume type %q uses VolumeSnapshotClass %q, and a claim keeps the snapshot class of its type",
			backend.ErrInvalid, to, ct.SnapshotClass)
	}
	return nil
}

// pvcName is derived from the full volume name, so the same id in two
// projects cannot share a claim.
func pvcName(id model.VolumeID) string {
	sum := sha256.Sum256([]byte(id.String()))
	return "vol-" + hex.EncodeToString(sum[:16])
}

// ClaimName is the name of the PVC that backs id, in the configured namespace.
func (b *Block) ClaimName(id model.VolumeID) string { return pvcName(id) }

// CreateVolume creates the claim for id. A repeat with the same spec is a no-op,
// including after Expand: a claim at least as large as the request is the same
// volume, because a volume only grows.
func (b *Block) CreateVolume(ctx context.Context, id model.VolumeID, spec model.VolumeSpec) error {
	c, ok := b.classFor(spec.Type)
	if !ok {
		return backend.Permanent(b.ServeType(spec.Type))
	}
	class := c.StorageClass
	if img, ok := spec.Source.Image(); ok {
		return b.imageVolume(ctx, id, spec, c, img)
	}
	var from model.SnapshotID
	// A Block claim is the disk itself. A Filesystem claim must hold disk.img,
	// which only a restore (the snapshot has it) or CDI (blankVolume) provides.
	mode := c.VolumeMode
	if snap, ok := spec.Source.Snapshot(); ok {
		from = snap
		vs, err := b.requireReady(ctx, snap)
		if err != nil {
			return err
		}
		mode = restoreMode(vs)
		if rs, ok := restoreSize(vs); ok && int64(spec.Size) < rs {
			return backend.Permanent(fmt.Errorf("%w: volume is smaller than the snapshot (%d bytes)", backend.ErrInvalid, rs))
		}
	}
	if from.IsZero() && mode == corev1.PersistentVolumeFilesystem {
		return b.blankVolume(ctx, id, spec, class)
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName(id),
			Namespace: b.ns,
			Annotations: map[string]string{
				VolumeAnnotation:     id.String(),
				zoneAnnotation:       spec.Zone.String(),
				sourceAnnotation:     from.String(),
				VolumeTypeAnnotation: spec.Type.String(),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &class,
			VolumeMode:       &mode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: *resource.NewQuantity(int64(spec.Size), resource.BinarySI)},
			},
		},
	}
	if !from.IsZero() {
		group := snapshotGVR.Group
		pvc.Spec.DataSource = &corev1.TypedLocalObjectReference{APIGroup: &group, Kind: "VolumeSnapshot", Name: snapshotName(from)}
	}
	pvcs := b.kc.CoreV1().PersistentVolumeClaims(b.ns)
	_, err := pvcs.Create(ctx, pvc, metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		if err != nil {
			return fmt.Errorf("kube: create claim: %w", err)
		}
		return nil
	}
	cur, err := pvcs.Get(ctx, pvc.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("kube: get claim: %w", err)
	}
	if cur.Annotations[zoneAnnotation] != spec.Zone.String() ||
		cur.Annotations[sourceAnnotation] != from.String() ||
		cur.Spec.StorageClassName == nil || *cur.Spec.StorageClassName != class ||
		cur.Spec.Resources.Requests.Storage().Cmp(*pvc.Spec.Resources.Requests.Storage()) < 0 {
		return backend.Permanent(fmt.Errorf("%w: volume", backend.ErrConflict))
	}
	return nil
}

// Expand grows the claim. A smaller size is ErrInvalid and the same size is a no-op.
func (b *Block) Expand(ctx context.Context, id model.VolumeID, size model.Bytes) error {
	pvcs := b.kc.CoreV1().PersistentVolumeClaims(b.ns)
	cur, err := pvcs.Get(ctx, pvcName(id), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return backend.Permanent(fmt.Errorf("%w: volume", backend.ErrNotFound))
	}
	if err != nil {
		return fmt.Errorf("kube: get claim: %w", err)
	}
	have := cur.Spec.Resources.Requests.Storage().Value()
	// The volume size is not the claim request when CDI added its overhead.
	// Compare with the volume size and keep the overhead on top.
	logical, err := b.volumeSize(ctx, cur)
	if err != nil {
		return err
	}
	switch {
	case int64(size) < logical:
		return backend.Permanent(fmt.Errorf("%w: volume cannot shrink", backend.ErrInvalid))
	case int64(size) == logical:
		return nil
	}
	if overhead := have - logical; overhead > 0 {
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		cur.Annotations[sizeAnnotation] = strconv.FormatInt(int64(size), 10)
		size += model.Bytes(overhead)
	}
	cur.Spec.Resources.Requests[corev1.ResourceStorage] = *resource.NewQuantity(int64(size), resource.BinarySI)
	if _, err = pvcs.Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("kube: expand claim: %w", err)
	}
	return nil
}

// anyFilesystem reports whether some type makes Filesystem volumes, which
// are DataVolumes.
func (b *Block) anyFilesystem() bool {
	for _, c := range b.mapped() {
		if c.VolumeMode == corev1.PersistentVolumeFilesystem {
			return true
		}
	}
	return false
}

// volumeSize is the size of the volume a claim stands for: the size
// annotation, else in Filesystem mode the request of its DataVolume, else the
// claim request.
func (b *Block) volumeSize(ctx context.Context, c *corev1.PersistentVolumeClaim) (int64, error) {
	if v, ok := c.Annotations[sizeAnnotation]; ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return 0, backend.Permanent(fmt.Errorf("%w: claim annotation %s=%q", backend.ErrInvalid, sizeAnnotation, v))
		}
		return n, nil
	}
	have := c.Spec.Resources.Requests.Storage().Value()
	if claimMode(c) != corev1.PersistentVolumeFilesystem {
		return have, nil
	}
	dv, err := b.dyn.Resource(dataVolumeGVR).Namespace(b.ns).Get(ctx, c.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
		return have, nil // a restored claim has no DataVolume and no overhead
	}
	if err != nil {
		return 0, fmt.Errorf("kube: get data volume: %w", err)
	}
	req, _, _ := unstructured.NestedString(dv.Object, "spec", "storage", "resources", "requests", "storage") //nolint:errcheck // absent is ""
	q, err := resource.ParseQuantity(req)
	if err != nil {
		return 0, backend.Permanent(fmt.Errorf("%w: data volume request %q", backend.ErrInvalid, req))
	}
	return q.Value(), nil
}

// DeleteVolume deletes the DataVolume that owns the claim, if any (a
// Filesystem volume or an image volume), and the claim. An absent object is not an error.
func (b *Block) DeleteVolume(ctx context.Context, id model.VolumeID) error {
	b.mu.Lock()
	delete(b.carried, pvcName(id))
	b.mu.Unlock()
	if err := b.deleteDataVolume(ctx, id); err != nil {
		// In Block mode only an image volume has a DataVolume, and a
		// deployment that grants no DataVolume access has none of those.
		if b.anyFilesystem() || !apierrors.IsForbidden(err) {
			return err
		}
	}
	err := b.kc.CoreV1().PersistentVolumeClaims(b.ns).Delete(ctx, pvcName(id), metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("kube: delete claim: %w", err)
	}
	return nil
}
