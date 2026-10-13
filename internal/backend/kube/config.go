package kube

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/popododo0720/vws-storage/internal/model"
)

// DefaultSnapshotClass is the VolumeSnapshotClass used when none is configured.
const DefaultSnapshotClass = "ceph-block"

// DefaultNamespace is the VM namespace. Claims live there because KubeVirt can
// hot-plug only a claim that is in the same namespace as the VM. There is no
// separate volume namespace.
const DefaultNamespace = "vws-vms"

// Config selects the cluster and every name the backend writes into it.
type Config struct {
	// Kubeconfig is a kubeconfig path. Empty means in-cluster credentials.
	Kubeconfig string
	Namespace  string
	Classes    Classes
	// SnapshotClass is the VolumeSnapshotClass. Empty disables snapshots.
	SnapshotClass string
	// VolumeMode is the volumeMode of new empty volumes: Block (default) or
	// Filesystem for a provisioner that serves only Filesystem, such as NFS.
	VolumeMode corev1.PersistentVolumeMode
}

// DefaultConfig serves every type from the ceph-block classes in the VM namespace.
func DefaultConfig() Config {
	return Config{Namespace: DefaultNamespace, Classes: DefaultClasses(), SnapshotClass: DefaultSnapshotClass, VolumeMode: corev1.PersistentVolumeBlock}
}

// Open connects to the cluster and returns the backend. It does not call the API server.
func Open(c Config) (*Block, error) {
	if c.Namespace == "" {
		return nil, fmt.Errorf("kube: namespace is required")
	}
	switch c.VolumeMode {
	case corev1.PersistentVolumeBlock, corev1.PersistentVolumeFilesystem:
	default:
		return nil, fmt.Errorf("kube: volume mode %q is not Block or Filesystem", c.VolumeMode)
	}
	for _, typ := range []model.VolumeType{model.VolumeTypeStandard, model.VolumeTypeSSD} {
		if c.Classes[typ] == "" {
			return nil, fmt.Errorf("kube: no StorageClass for volume type %q", typ)
		}
	}
	rc, err := clientcmd.BuildConfigFromFlags("", c.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("kube: %w", err)
	}
	kc, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("kube: %w", err)
	}
	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("kube: %w", err)
	}
	return NewBlock(kc, dyn, c.Namespace, c.Classes, c.SnapshotClass).WithVolumeMode(c.VolumeMode), nil
}
