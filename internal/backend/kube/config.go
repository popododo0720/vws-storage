package kube

import (
	"fmt"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/popododo0720/vws-storage/internal/model"
)

// DefaultSnapshotClass is the VolumeSnapshotClass used when none is configured.
const DefaultSnapshotClass = "vws-rbd"

// DefaultNamespace holds the claims when none is configured.
const DefaultNamespace = "default"

// Config selects the cluster and every name the backend writes into it.
type Config struct {
	// Kubeconfig is a kubeconfig path. Empty means in-cluster credentials.
	Kubeconfig    string
	Namespace     string
	Classes       Classes
	SnapshotClass string
}

// DefaultConfig serves every type from the vws-rbd classes in the default namespace.
func DefaultConfig() Config {
	return Config{Namespace: DefaultNamespace, Classes: DefaultClasses(), SnapshotClass: DefaultSnapshotClass}
}

// Open connects to the cluster and returns the backend. It does not call the API server.
func Open(c Config) (*Block, error) {
	if c.Namespace == "" || c.SnapshotClass == "" {
		return nil, fmt.Errorf("kube: namespace and snapshot class are required")
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
	return NewBlock(kc, dyn, c.Namespace, c.Classes, c.SnapshotClass), nil
}
