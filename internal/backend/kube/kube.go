// Package kube is the block backend over Kubernetes. A volume is one
// PersistentVolumeClaim whose StorageClass comes from the type table.
package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/model"
)

// Classes maps a volume type to a StorageClass name. It is the one place
// that decides which CSI driver serves a type.
type Classes map[model.VolumeType]string

// DefaultClass is the StorageClass name every type maps to by default.
const DefaultClass = "ceph-block"

// DefaultClasses serves every type from the Rook Ceph RBD class.
func DefaultClasses() Classes {
	return Classes{
		model.VolumeTypeStandard: DefaultClass,
		model.VolumeTypeSSD:      DefaultClass,
	}
}

const (
	// VolumeAnnotation holds the VWS volume name on the PVC.
	VolumeAnnotation = "vws.storage/volume"
	zoneAnnotation   = "vws.storage/zone"
	sourceAnnotation = "vws.storage/source-snapshot"
	// SnapshotAnnotation holds the VWS snapshot name on the VolumeSnapshot.
	SnapshotAnnotation       = "vws.storage/snapshot"
	snapshotSourceAnnotation = "vws.storage/source-volume"
)

// Block is the PVC-backed volume backend.
type Block struct {
	kc            kubernetes.Interface
	dyn           dynamic.Interface
	ns            string
	classes       Classes
	snapshotClass string
}

var _ backend.Block = (*Block)(nil)

// NewBlock returns a backend that creates PVCs and VolumeSnapshots in
// namespace ns. snapshotClass is the VolumeSnapshotClass for new snapshots.
func NewBlock(kc kubernetes.Interface, dyn dynamic.Interface, ns string, classes Classes, snapshotClass string) *Block {
	return &Block{kc: kc, dyn: dyn, ns: ns, classes: classes, snapshotClass: snapshotClass}
}

// pvcName is derived from the full volume name, so the same id in two
// projects cannot share a claim.
func pvcName(id model.VolumeID) string {
	sum := sha256.Sum256([]byte(id.String()))
	return "vol-" + hex.EncodeToString(sum[:16])
}

// CreateVolume creates the claim for id. A repeat with the same spec is a no-op,
// including after Expand: a claim at least as large as the request is the same
// volume, because a volume only grows.
func (b *Block) CreateVolume(ctx context.Context, id model.VolumeID, spec model.VolumeSpec) error {
	class, ok := b.classes[spec.Type]
	if !ok {
		return backend.Permanent(fmt.Errorf("%w: volume type %q", backend.ErrInvalid, spec.Type))
	}
	if _, ok := spec.Source.Image(); ok {
		return backend.Permanent(fmt.Errorf("%w: image source", backend.ErrInvalid))
	}
	var from model.SnapshotID
	if snap, ok := spec.Source.Snapshot(); ok {
		from = snap
		if err := b.requireReady(ctx, snap); err != nil {
			return err
		}
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName(id),
			Namespace: b.ns,
			Annotations: map[string]string{
				VolumeAnnotation: id.String(),
				zoneAnnotation:   spec.Zone.String(),
				sourceAnnotation: from.String(),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &class,
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
		return err
	}
	cur, err := pvcs.Get(ctx, pvc.Name, metav1.GetOptions{})
	if err != nil {
		return err
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
		return err
	}
	have := cur.Spec.Resources.Requests.Storage().Value()
	switch {
	case int64(size) < have:
		return backend.Permanent(fmt.Errorf("%w: volume cannot shrink", backend.ErrInvalid))
	case int64(size) == have:
		return nil
	}
	cur.Spec.Resources.Requests[corev1.ResourceStorage] = *resource.NewQuantity(int64(size), resource.BinarySI)
	_, err = pvcs.Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

// DeleteVolume deletes the claim. An absent claim is not an error.
func (b *Block) DeleteVolume(ctx context.Context, id model.VolumeID) error {
	err := b.kc.CoreV1().PersistentVolumeClaims(b.ns).Delete(ctx, pvcName(id), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
