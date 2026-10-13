package kube

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var snapshotClassGVR = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotclasses"}

// filesystemOnly lists provisioners that cannot serve a Block claim.
var filesystemOnly = map[string]bool{
	"nfs.csi.k8s.io": true,
	"smb.csi.k8s.io": true,
}

// Provisioners maps each configured StorageClass to its provisioner. A class
// that cannot be read is absent.
func (b *Block) Provisioners(ctx context.Context) map[string]string {
	out := map[string]string{}
	for _, class := range b.classes {
		if sc, err := b.kc.StorageV1().StorageClasses().Get(ctx, class, metav1.GetOptions{}); err == nil {
			out[class] = sc.Provisioner
		}
	}
	return out
}

// Ready is the preflight. It reports whether the cluster answers and holds
// what the backend needs: the namespace, each StorageClass with a provisioner
// that can serve the volume mode, the VolumeSnapshotClass when one is set
// (with the same driver), and the CDI DataVolume API in Filesystem mode. It
// only reads. Storage is not installed here; it must exist beforehand.
func (b *Block) Ready(ctx context.Context) error {
	var errs []error
	if _, err := b.kc.CoreV1().Namespaces().Get(ctx, b.ns, metav1.GetOptions{}); err != nil {
		errs = append(errs, fmt.Errorf("namespace %q: %w", b.ns, err))
	}
	var classes []string
	seen := map[string]bool{}
	for _, class := range b.classes {
		if !seen[class] {
			seen[class] = true
			classes = append(classes, class)
		}
	}
	sort.Strings(classes)
	drivers := map[string]bool{}
	for _, class := range classes {
		sc, err := b.kc.StorageV1().StorageClasses().Get(ctx, class, metav1.GetOptions{})
		if err != nil {
			errs = append(errs, fmt.Errorf("StorageClass %q: %w", class, err))
			continue
		}
		drivers[sc.Provisioner] = true
		if b.mode == corev1.PersistentVolumeBlock && filesystemOnly[sc.Provisioner] {
			errs = append(errs, fmt.Errorf("StorageClass %q: provisioner %s serves only Filesystem volumes; set the volume mode to Filesystem", class, sc.Provisioner))
		}
	}
	if b.snapshotClass != "" {
		vsc, err := b.dyn.Resource(snapshotClassGVR).Get(ctx, b.snapshotClass, metav1.GetOptions{})
		if err != nil {
			errs = append(errs, fmt.Errorf("VolumeSnapshotClass %q: %w", b.snapshotClass, err))
		} else if d, _, _ := unstructured.NestedString(vsc.Object, "driver"); len(drivers) > 0 && !drivers[d] { //nolint:errcheck // absent is ""
			errs = append(errs, fmt.Errorf("VolumeSnapshotClass %q: driver %q is not the provisioner of the StorageClasses", b.snapshotClass, d))
		}
	}
	if b.mode == corev1.PersistentVolumeFilesystem {
		if _, err := b.dyn.Resource(dataVolumeGVR).Namespace(b.ns).List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
			errs = append(errs, fmt.Errorf("CDI DataVolume API (needed for Filesystem volumes): %w", err))
		}
	}
	return errors.Join(errs...)
}
