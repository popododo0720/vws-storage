package kube

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var snapshotClassGVR = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotclasses"}

// rule is what a provisioner needs from its StorageClass and type mappings.
type rule struct {
	filesystemOnly bool // cannot serve a Block claim
	expansion      bool // the StorageClass must set allowVolumeExpansion
	snapshots      bool // every type mapped to the class must set a VolumeSnapshotClass
}

// rules lists the provisioners with readiness rules. Any other provisioner has
// none. LINSTOR is the local driver (#71): DRBD-replicated local disks, which
// grow and snapshot, so a class without either is a misconfigured install.
var rules = map[string]rule{
	"nfs.csi.k8s.io":         {filesystemOnly: true},
	"smb.csi.k8s.io":         {filesystemOnly: true},
	"linstor.csi.linbit.com": {expansion: true, snapshots: true},
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
// with the driver of the StorageClasses that use it, the per-provisioner
// rules, and the CDI DataVolume API when a type is Filesystem. It only reads. Storage is not installed
// here; it must exist beforehand.
func (b *Block) Ready(ctx context.Context) error {
	var errs []error
	if _, err := b.kc.CoreV1().Namespaces().Get(ctx, b.ns, metav1.GetOptions{}); err != nil {
		errs = append(errs, fmt.Errorf("namespace %q: %w", b.ns, err))
	}
	mapped := b.mapped()
	blockOn := map[string]bool{}       // StorageClass -> some type uses it in Block mode
	snapUsers := map[string][]string{} // VolumeSnapshotClass -> StorageClasses
	unsnapped := map[string][]string{} // StorageClass -> types mapped to it with no VolumeSnapshotClass
	fs := false
	for t, c := range mapped {
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
		} else {
			unsnapped[c.StorageClass] = append(unsnapped[c.StorageClass], t.String())
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
		r := rules[sc.Provisioner]
		if blockOn[class] && r.filesystemOnly {
			errs = append(errs, fmt.Errorf("StorageClass %q: provisioner %s serves only Filesystem volumes; set the volume mode to Filesystem", class, sc.Provisioner))
		}
		if r.expansion && (sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion) {
			errs = append(errs, fmt.Errorf(
				"StorageClass %q: provisioner %s needs allowVolumeExpansion: true", class, sc.Provisioner))
		}
		if types := unsnapped[class]; r.snapshots && len(types) > 0 {
			sort.Strings(types)
			errs = append(errs, fmt.Errorf("StorageClass %q: provisioner %s needs a VolumeSnapshotClass for types %s",
				class, sc.Provisioner, strings.Join(types, ", ")))
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
