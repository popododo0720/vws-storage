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
	for _, c := range b.mapped() {
		if _, done := out[c.StorageClass]; done {
			continue
		}
		if sc, err := b.kc.StorageV1().StorageClasses().Get(ctx, c.StorageClass, metav1.GetOptions{}); err == nil {
			out[c.StorageClass] = sc.Provisioner
		}
	}
	return out
}

// Ready is the preflight. It reports whether the cluster answers and holds
// what each type mapping needs: the namespace, each StorageClass with a
// provisioner that can serve the type's volume mode, each VolumeSnapshotClass
// with the driver of the StorageClasses that use it, and the CDI DataVolume
// API when a type is Filesystem. It only reads. Storage is not installed
// here; it must exist beforehand.
func (b *Block) Ready(ctx context.Context) error {
	var errs []error
	if _, err := b.kc.CoreV1().Namespaces().Get(ctx, b.ns, metav1.GetOptions{}); err != nil {
		errs = append(errs, fmt.Errorf("namespace %q: %w", b.ns, err))
	}
	mapped := b.mapped()
	blockOn := map[string]bool{}       // StorageClass -> some type uses it in Block mode
	snapUsers := map[string][]string{} // VolumeSnapshotClass -> StorageClasses
	fs := false
	for _, c := range mapped {
		if c.VolumeMode == corev1.PersistentVolumeBlock {
			blockOn[c.StorageClass] = true
		} else {
			if _, seen := blockOn[c.StorageClass]; !seen {
				blockOn[c.StorageClass] = false
			}
			fs = true
		}
		if c.SnapshotClass != "" {
			snapUsers[c.SnapshotClass] = append(snapUsers[c.SnapshotClass], c.StorageClass)
		}
	}
	classes := make([]string, 0, len(blockOn))
	for class := range blockOn {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	provisioner := map[string]string{}
	for _, class := range classes {
		sc, err := b.kc.StorageV1().StorageClasses().Get(ctx, class, metav1.GetOptions{})
		if err != nil {
			errs = append(errs, fmt.Errorf("StorageClass %q: %w", class, err))
			continue
		}
		provisioner[class] = sc.Provisioner
		if blockOn[class] && filesystemOnly[sc.Provisioner] {
			errs = append(errs, fmt.Errorf("StorageClass %q: provisioner %s serves only Filesystem volumes; set the volume mode to Filesystem", class, sc.Provisioner))
		}
	}
	snaps := make([]string, 0, len(snapUsers))
	for name := range snapUsers {
		snaps = append(snaps, name)
	}
	sort.Strings(snaps)
	for _, name := range snaps {
		vsc, err := b.dyn.Resource(snapshotClassGVR).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			errs = append(errs, fmt.Errorf("VolumeSnapshotClass %q: %w", name, err))
			continue
		}
		d, _, _ := unstructured.NestedString(vsc.Object, "driver") //nolint:errcheck // absent is ""
		for _, class := range snapUsers[name] {
			if p, ok := provisioner[class]; ok && p != d {
				errs = append(errs, fmt.Errorf(
					"VolumeSnapshotClass %q: driver %q is not the provisioner of the StorageClasses", name, d))
				break
			}
		}
	}
	if fs {
		if _, err := b.dyn.Resource(dataVolumeGVR).Namespace(b.ns).List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
			errs = append(errs, fmt.Errorf("CDI DataVolume API (needed for Filesystem volumes): %w", err))
		}
	}
	return errors.Join(errs...)
}
