package kube

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/popododo0720/vws-storage/internal/model"
)

// ReloadVolumeTypes replaces the explicit type mappings from one named
// ConfigMap. Missing or invalid data leaves the last good mapping unchanged.
// Legacy standard and gp2 class flags remain the fallback for absent keys.
func (b *Block) ReloadVolumeTypes(ctx context.Context, namespace, name string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cm, err := b.kc.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("kube: read volume types ConfigMap %s/%s: %w", namespace, name, err)
	}
	keys := make([]string, 0, len(cm.Data))
	for key := range cm.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	types := make(map[model.VolumeType]Class, len(keys))
	for _, key := range keys {
		typ, class, err := ParseTypeClass(key + "=" + cm.Data[key])
		if err != nil {
			return false, fmt.Errorf("kube: volume types key %q: %w", key, err)
		}
		if _, exists := types[typ]; exists {
			return false, fmt.Errorf("kube: volume types key %q duplicates type %s", key, typ)
		}
		types[typ] = class
	}
	b.typesMu.Lock()
	defer b.typesMu.Unlock()
	if maps.Equal(b.types, types) {
		return false, nil
	}
	b.types = types
	return true, nil
}
