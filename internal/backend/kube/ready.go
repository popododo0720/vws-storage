package kube

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var snapshotClassGVR = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotclasses"}

// Ready reports whether the cluster answers and holds every name the backend
// writes: the namespace, the StorageClass of each volume type and the
// VolumeSnapshotClass. It only reads. A missing object is an error here and
// stays a retry for the operations that need it.
func (b *Block) Ready(ctx context.Context) error {
	var errs []error
	if _, err := b.kc.CoreV1().Namespaces().Get(ctx, b.ns, metav1.GetOptions{}); err != nil {
		errs = append(errs, fmt.Errorf("namespace %q: %w", b.ns, err))
	}
	seen := map[string]bool{}
	for _, class := range b.classes {
		if seen[class] {
			continue
		}
		seen[class] = true
		if _, err := b.kc.StorageV1().StorageClasses().Get(ctx, class, metav1.GetOptions{}); err != nil {
			errs = append(errs, fmt.Errorf("StorageClass %q: %w", class, err))
		}
	}
	if _, err := b.dyn.Resource(snapshotClassGVR).Get(ctx, b.snapshotClass, metav1.GetOptions{}); err != nil {
		errs = append(errs, fmt.Errorf("VolumeSnapshotClass %q: %w", b.snapshotClass, err))
	}
	return errors.Join(errs...)
}
