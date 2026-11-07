package kube

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typedcoord "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

// stallClient lets the first Lease update reach the API server, holds its
// reply for delay, and hangs every later update until its context ends: a
// holder cut off from the API server right after one slow renewal.
type stallClient struct {
	kubernetes.Interface
	coord *stallCoord
}

func (c stallClient) CoordinationV1() typedcoord.CoordinationV1Interface { return c.coord }

type stallCoord struct {
	typedcoord.CoordinationV1Interface
	updates atomic.Int32
	renewed chan struct{}
	delay   time.Duration
}

func (c *stallCoord) Leases(ns string) typedcoord.LeaseInterface {
	return stallLeases{LeaseInterface: c.CoordinationV1Interface.Leases(ns), c: c}
}

type stallLeases struct {
	typedcoord.LeaseInterface
	c *stallCoord
}

func (l stallLeases) Update(
	ctx context.Context, lease *coordinationv1.Lease, o metav1.UpdateOptions,
) (*coordinationv1.Lease, error) {
	if l.c.updates.Add(1) > 1 {
		<-ctx.Done()
		return nil, fmt.Errorf("stalled update: %w", ctx.Err())
	}
	got, err := l.LeaseInterface.Update(ctx, lease, o)
	if err != nil {
		return nil, fmt.Errorf("update: %w", err)
	}
	close(l.c.renewed)
	select {
	case <-time.After(l.c.delay):
		return got, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("slow reply: %w", ctx.Err())
	}
}

func TestRunEndsBeforeASuccessorCanTakeTheExpiredLease(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delay time.Duration
		retry time.Duration
	}{
		{"renew deadline close to the lease", 0, 1500 * time.Millisecond},
		{"slow renewal reply", 1500 * time.Millisecond, 500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns := namespace(t)
			opts := Options{
				Namespace: ns, Identity: "a",
				LeaseDuration: 3 * time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: tc.retry,
			}
			coord := &stallCoord{CoordinationV1Interface: kc.CoordinationV1(), renewed: make(chan struct{}), delay: tc.delay}
			a, err := Open(context.Background(), stallClient{Interface: kc, coord: coord}, opts)
			if err != nil {
				t.Fatal(err)
			}
			b := open(t, leaseOpts(ns, "b"))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			adone, bdone := make(chan error, 1), make(chan error, 1)
			first := make(chan context.Context, 1)
			go func() {
				adone <- a.Lead(ctx, func(c context.Context) error { first <- c; <-c.Done(); return nil })
			}()
			old := within(t, first, 5*time.Second, "first term")
			within(t, coord.renewed, 5*time.Second, "renewal")
			overlap := make(chan bool, 1)
			go func() {
				bdone <- b.Lead(ctx, func(c context.Context) error { overlap <- old.Err() == nil; <-c.Done(); return nil })
			}()
			if within(t, overlap, 10*time.Second, "successor term") {
				t.Fatal("the successor started while the first term's context was live")
			}
			cancel()
			within(t, adone, 5*time.Second, "first Lead return")
			within(t, bdone, 5*time.Second, "successor Lead return")
		})
	}
}
