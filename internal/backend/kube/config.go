package kube

import (
	"fmt"
	"strings"
	"time"

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
	// Classes maps types to StorageClasses with the backend-wide mode and
	// snapshot class. Types adds or replaces a type with its own Class
	// (-kube-volume-type). A type in neither is refused (vws-storage#123).
	Classes Classes
	Types   map[model.VolumeType]Class
	// SnapshotClass is the VolumeSnapshotClass. Empty disables snapshots.
	SnapshotClass string
	// VolumeMode is the volumeMode of new empty volumes: Block (default) or
	// Filesystem for a provisioner that serves only Filesystem, such as NFS.
	VolumeMode corev1.PersistentVolumeMode
	// DataVolumeRetries and DataVolumeTimeout bound a Filesystem volume's CDI
	// DataVolume: how often a Failed one is made again, and how long it may
	// take from the first one to Succeeded.
	DataVolumeRetries int
	DataVolumeTimeout time.Duration
	// ImageNamespace holds the CDI DataSources of images (vws-compute
	// -image-namespace). An image volume clones from there.
	ImageNamespace string
}

// DefaultConfig serves every type from the ceph-block classes in the VM namespace.
func DefaultConfig() Config {
	return Config{
		Namespace:         DefaultNamespace,
		Classes:           DefaultClasses(),
		SnapshotClass:     DefaultSnapshotClass,
		VolumeMode:        corev1.PersistentVolumeBlock,
		DataVolumeRetries: DefaultDataVolumeRetries, DataVolumeTimeout: DefaultDataVolumeTimeout,
		ImageNamespace: DefaultImageNamespace,
	}
}

// Open connects to the cluster and returns the backend. It does not call the API server.
func Open(c Config) (*Block, error) {
	if c.Namespace == "" {
		return nil, fmt.Errorf("kube: namespace is required")
	}
	if c.ImageNamespace == "" {
		return nil, fmt.Errorf("kube: image namespace is required")
	}
	if c.DataVolumeRetries < 0 || c.DataVolumeTimeout <= 0 {
		return nil, fmt.Errorf("kube: data volume retries must be >= 0 and the timeout > 0")
	}
	switch c.VolumeMode {
	case corev1.PersistentVolumeBlock, corev1.PersistentVolumeFilesystem:
	default:
		return nil, fmt.Errorf("kube: volume mode %q is not Block or Filesystem", c.VolumeMode)
	}
	for _, typ := range []model.VolumeType{model.VolumeTypeStandard, model.VolumeTypeGP2} {
		if c.Classes[typ] == "" && c.Types[typ].StorageClass == "" {
			return nil, fmt.Errorf("kube: no StorageClass for volume type %q", typ)
		}
	}
	for typ, tc := range c.Types {
		if err := checkClass(typ, tc); err != nil {
			return nil, err
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
	b := NewBlock(kc, dyn, c.Namespace, c.Classes, c.SnapshotClass).
		WithVolumeMode(c.VolumeMode).
		WithDataVolumeRetry(c.DataVolumeRetries, c.DataVolumeTimeout).
		WithImageNamespace(c.ImageNamespace)
	for typ, tc := range c.Types {
		b.WithClass(typ, tc)
	}
	return b, nil
}

func checkClass(typ model.VolumeType, c Class) error {
	if !typ.Valid() {
		return fmt.Errorf("kube: volume type %d is not an EBS type", typ)
	}
	if c.StorageClass == "" {
		return fmt.Errorf("kube: volume type %s has no StorageClass", typ)
	}
	switch c.VolumeMode {
	case "", corev1.PersistentVolumeBlock, corev1.PersistentVolumeFilesystem:
		return nil
	default:
		return fmt.Errorf("kube: volume type %s: volume mode %q is not Block or Filesystem", typ, c.VolumeMode)
	}
}

// ParseTypeClass parses TYPE=STORAGECLASS[/VOLUMEMODE[/SNAPSHOTCLASS]], the
// -kube-volume-type flag. TYPE is an EC2 volume type name; an empty
// VOLUMEMODE or SNAPSHOTCLASS takes the backend-wide value.
func ParseTypeClass(s string) (model.VolumeType, Class, error) {
	name, rest, ok := strings.Cut(s, "=")
	if !ok {
		return 0, Class{}, fmt.Errorf("kube: volume type mapping %q is not TYPE=STORAGECLASS[/VOLUMEMODE[/SNAPSHOTCLASS]]", s)
	}
	typ, err := model.ParseVolumeType(name)
	if err != nil {
		return 0, Class{}, fmt.Errorf("kube: volume type mapping %q: unknown volume type %q", s, name)
	}
	parts := strings.Split(rest, "/")
	if len(parts) > 3 {
		return 0, Class{}, fmt.Errorf("kube: volume type mapping %q has more than three fields", s)
	}
	parts = append(parts, "", "")
	c := Class{StorageClass: parts[0], VolumeMode: corev1.PersistentVolumeMode(parts[1]), SnapshotClass: parts[2]}
	if err := checkClass(typ, c); err != nil {
		return 0, Class{}, err
	}
	return typ, c, nil
}
