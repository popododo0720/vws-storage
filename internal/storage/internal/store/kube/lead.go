package kube

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/popododo0720/vws-storage/internal/model"
)

const leaseName = "vws-storage-run"

func checkLease(o Options) error {
	// The Lease stores whole seconds. A truncated duration lets another
	// replica take over while this one still believes it leads.
	if o.LeaseDuration < time.Second || o.LeaseDuration%time.Second != 0 {
		return fmt.Errorf("kubestore: lease duration %s is not a whole number of seconds: %w",
			o.LeaseDuration, model.ErrInvalidArgument)
	}
	jittered := time.Duration(leaderelection.JitterFactor * float64(o.RetryPeriod))
	if o.RenewDeadline >= o.LeaseDuration || o.RenewDeadline <= jittered {
		return fmt.Errorf("kubestore: need lease duration %s > renew deadline %s > %.1f x retry period %s: %w",
			o.LeaseDuration, o.RenewDeadline, leaderelection.JitterFactor, o.RetryPeriod, model.ErrInvalidArgument)
	}
	return nil
}

func identity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = managedBy
	}
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("kubestore: crypto/rand: %v", err))
	}
	return host + "-" + hex.EncodeToString(b)
}

// Lead campaigns for the vws-storage-run Lease and runs run while it holds
// it. The lease is released only after run returned, so a successor never
// starts while run still writes; client-go's ReleaseOnCancel releases first.
func (s *Store) Lead(ctx context.Context, run func(context.Context) error) error {
	for ctx.Err() == nil {
		if err := s.term(ctx, run); err != nil {
			return err
		}
	}
	return nil
}

// term campaigns until it leads or ctx ends, then runs run until the lease
// is lost or run returns.
func (s *Store) term(ctx context.Context, run func(context.Context) error) error {
	tctx, stop := context.WithCancel(ctx)
	defer stop()
	lock := &boundedLock{LeaseLock: &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: leaseName, Namespace: s.o.Namespace},
		Client:     s.c.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: s.o.Identity},
	}, timeout: s.o.CallTimeout}
	done := make(chan error, 1)
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: s.o.LeaseDuration,
		RenewDeadline: s.o.RenewDeadline,
		RetryPeriod:   s.o.RetryPeriod,
		Name:          leaseName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(lctx context.Context) {
				s.o.Log.InfoContext(lctx, "kubestore: leading", s.leaseAttrs()...)
				err := run(lctx)
				// run returned while it may still lead: stop renewing.
				stop()
				done <- err
			},
			OnStoppedLeading: func() {},
		},
	})
	if err != nil {
		return fmt.Errorf("kubestore: leader election: %w", err)
	}
	le.Run(tctx)
	if lock.acquired.Load() {
		err = <-done
		s.o.Log.InfoContext(ctx, "kubestore: stopped leading", s.leaseAttrs()...)
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.o.RenewDeadline)
	defer cancel()
	s.release(rctx)
	return err
}

// release clears the holder if it is still this replica. The update carries
// the resourceVersion just read, so it never clears a successor's hold.
func (s *Store) release(ctx context.Context) {
	leases := s.c.CoordinationV1().Leases(s.o.Namespace)
	for range maxCallAttempts {
		var l *coordinationv1.Lease
		err := s.call(ctx, func(ctx context.Context) error {
			var err error
			l, err = leases.Get(ctx, leaseName, metav1.GetOptions{})
			return apiErr(err)
		})
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil {
			s.releaseFailed(ctx, err)
			return
		}
		if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != s.o.Identity {
			return
		}
		l.Spec.HolderIdentity = nil
		err = s.call(ctx, func(ctx context.Context) error {
			_, err := leases.Update(ctx, l, metav1.UpdateOptions{})
			return apiErr(err)
		})
		if apierrors.IsConflict(err) {
			continue
		}
		if err != nil {
			s.releaseFailed(ctx, err)
		}
		return
	}
}

func (s *Store) releaseFailed(ctx context.Context, err error) {
	attrs := append(s.leaseAttrs(), slog.String("err", err.Error()))
	s.o.Log.WarnContext(ctx, "kubestore: lease release left to expire", attrs...)
}

func (s *Store) leaseAttrs() []any {
	return []any{
		slog.String("identity", s.o.Identity),
		slog.String("namespace", s.o.Namespace),
		slog.String("lease", leaseName),
	}
}

// boundedLock bounds each Lease call, which client-go leaves unbounded while
// it campaigns. acquired records a successful write of this identity: only
// then does client-go start run.
type boundedLock struct {
	*resourcelock.LeaseLock
	timeout  time.Duration
	acquired atomic.Bool
}

func (l *boundedLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	rec, raw, err := l.LeaseLock.Get(ctx)
	return rec, raw, apiErr(err)
}

func (l *boundedLock) Create(ctx context.Context, r resourcelock.LeaderElectionRecord) error {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	err := l.LeaseLock.Create(ctx, r)
	if err == nil {
		l.acquired.Store(true)
	}
	return apiErr(err)
}

func (l *boundedLock) Update(ctx context.Context, r resourcelock.LeaderElectionRecord) error {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	err := l.LeaseLock.Update(ctx, r)
	if err == nil {
		l.acquired.Store(true)
	}
	return apiErr(err)
}
