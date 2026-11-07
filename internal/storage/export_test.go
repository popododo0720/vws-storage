package storage

import (
	"context"
	"time"

	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/kube"
)

// StatusOf exposes statusOf to the external tests.
var StatusOf = statusOf

// LeaseDuration is the lease KubeReplica campaigns for.
const LeaseDuration = 2 * time.Second

// KubeReplica is Kube as the replica named identity, on a lease short enough
// for a test to wait out.
func KubeReplica(path, namespace, identity string) StoreConfig {
	return StoreConfig{open: func(ctx context.Context) (store.Store, error) {
		return kube.Connect(ctx, path, kube.Options{
			Namespace: namespace, Identity: identity,
			LeaseDuration: LeaseDuration, RenewDeadline: time.Second, RetryPeriod: 200 * time.Millisecond,
		})
	}}
}

// PurgeTombstonesAsOf opens the Kube store as if the clock read time.Now()
// plus ahead, so Open purges the tombstones older than the TTL by then.
func PurgeTombstonesAsOf(ctx context.Context, path, namespace string, ahead time.Duration) error {
	st, err := kube.Connect(ctx, path, kube.Options{
		Namespace: namespace, Now: func() time.Time { return time.Now().Add(ahead) },
	})
	if err != nil {
		return err
	}
	return st.Close()
}
