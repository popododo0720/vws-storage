package kube

import (
	"context"
	"errors"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/popododo0720/vws-storage/internal/model"
)

func leaseOpts(ns, id string) Options {
	return Options{
		Namespace: ns, Identity: id,
		LeaseDuration: 2 * time.Second, RenewDeadline: time.Second, RetryPeriod: 200 * time.Millisecond,
	}
}

func lease(t *testing.T, ns string) *coordinationv1.Lease {
	t.Helper()
	l, err := kc.CoordinationV1().Leases(ns).Get(context.Background(), leaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func holderOf(t *testing.T, ns string) string {
	t.Helper()
	if h := lease(t, ns).Spec.HolderIdentity; h != nil {
		return *h
	}
	return ""
}

func within[T any](t *testing.T, ch <-chan T, d time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatalf("no %s within %s", what, d)
		var zero T
		return zero
	}
}

// steal hands the Lease to another holder for one second, as a replica
// that took over an expired lease would.
func steal(t *testing.T, ns string) {
	t.Helper()
	for {
		l := lease(t, ns)
		thief, one, now := "thief", int32(1), metav1.NewMicroTime(time.Now())
		l.Spec.HolderIdentity, l.Spec.LeaseDurationSeconds, l.Spec.RenewTime = &thief, &one, &now
		_, err := kc.CoordinationV1().Leases(ns).Update(context.Background(), l, metav1.UpdateOptions{})
		if err == nil {
			return
		}
		if !apierrors.IsConflict(err) {
			t.Fatal(err)
		}
	}
}

// A lost lease cancels run's context, and Lead does not campaign again until
// run returned, even after the other holder's lease expired.
func TestLeadWaitsForRunBeforeCampaigningAgain(t *testing.T) {
	ns := namespace(t)
	st := open(t, leaseOpts(ns, "a"))
	started := make(chan int, 2)
	lost := make(chan struct{})
	unblock := make(chan struct{})
	terms := 0
	ctx, cancel := context.WithCancel(context.Background())
	led := make(chan error, 1)
	go func() {
		led <- st.Lead(ctx, func(ctx context.Context) error {
			terms++
			started <- terms
			<-ctx.Done()
			if terms == 1 {
				close(lost)
				<-unblock
			}
			return nil
		})
	}()
	t.Cleanup(cancel)

	within(t, started, 5*time.Second, "first term")
	if got := holderOf(t, ns); got != "a" {
		t.Fatalf("holder %q, want a", got)
	}
	steal(t, ns)
	within(t, lost, 5*time.Second, "cancel of the first term")

	time.Sleep(2 * time.Second)
	select {
	case n := <-started:
		t.Fatalf("term %d started while the first had not returned", n)
	default:
	}
	if got := holderOf(t, ns); got != "thief" {
		t.Fatalf("holder %q while the first term runs, want thief", got)
	}

	close(unblock)
	if n := within(t, started, 5*time.Second, "second term"); n != 2 {
		t.Fatalf("term %d, want 2", n)
	}
	if got := holderOf(t, ns); got != "a" {
		t.Fatalf("holder %q after the second campaign, want a", got)
	}
	cancel()
	if err := within(t, led, 5*time.Second, "Lead return"); err != nil {
		t.Fatal(err)
	}
	if got := holderOf(t, ns); got != "" {
		t.Fatalf("holder %q after a clean stop, want released", got)
	}
}

// run's error ends Lead, and the lease is released for another replica.
func TestLeadReturnsRunErrorAndReleases(t *testing.T) {
	ns := namespace(t)
	st := open(t, leaseOpts(ns, "a"))
	boom := errors.New("boom")
	err := st.Lead(context.Background(), func(context.Context) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("Lead %v, want boom", err)
	}
	if got := holderOf(t, ns); got != "" {
		t.Fatalf("holder %q, want released", got)
	}
}

// A lease the API rounds down, or a renew deadline that outlasts the lease,
// lets two replicas lead at once, so Open refuses it before any API call.
func TestOpenRefusesLeaseDurationsThatSplitTheLeader(t *testing.T) {
	ms := time.Millisecond
	for name, o := range map[string]Options{
		"under a second":     {LeaseDuration: 500 * ms, RenewDeadline: 100 * ms, RetryPeriod: 10 * ms},
		"fractional second":  {LeaseDuration: 1500 * ms, RenewDeadline: 1000 * ms, RetryPeriod: 100 * ms},
		"renew outlasts":     {LeaseDuration: 2000 * ms, RenewDeadline: 2000 * ms, RetryPeriod: 100 * ms},
		"retry outlasts":     {LeaseDuration: 2000 * ms, RenewDeadline: 1000 * ms, RetryPeriod: 1000 * ms},
		"defaults but renew": {RenewDeadline: time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			o.Namespace = "unused"
			if _, err := Open(context.Background(), kc, o); !errors.Is(err, model.ErrInvalidArgument) {
				t.Fatalf("Open %v, want ErrInvalidArgument", err)
			}
		})
	}
}
