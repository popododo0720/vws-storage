package storage_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// replica is one running service of a shared Kube store, with its own backend.
type replica struct {
	svc  *storage.Service
	blk  *fake.Block
	stop context.CancelFunc
	done chan struct{}
}

func startReplica(t *testing.T, path, ns, id string) *replica {
	t.Helper()
	blk := fake.NewBlock()
	o := kubeOpts(storage.KubeReplica(path, ns, id), blk, fake.NewObjects(fake.ObjectOptions{}))
	svc, err := storage.Open(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &replica{svc: svc, blk: blk, stop: cancel, done: make(chan struct{})}
	go func() {
		_ = r.svc.Run(ctx)
		close(r.done)
	}()
	t.Cleanup(func() {
		cancel()
		<-r.done
		svc.Close()
	})
	return r
}

func holder(t *testing.T, ns string) string {
	t.Helper()
	l, err := kc.CoordinationV1().Leases(ns).Get(context.Background(), "vws-storage-run", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	if l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

// waitHolder returns how long the Lease took to name want.
func waitHolder(t *testing.T, ns, want string, limit time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < limit {
		if holder(t, ns) == want {
			return time.Since(start)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("lease holder %q, want %q within %s", holder(t, ns), want, limit)
	return 0
}

func creates(blk *fake.Block) int {
	n := 0
	for _, c := range blk.Calls() {
		if c.Method == "CreateVolume" {
			n++
		}
	}
	return n
}

// Both replicas serve submits, and only the Lease holder drives (#141 AC 5).
func TestOnlyTheLeaseHolderDrivesOperations(t *testing.T) {
	path, ns := kubeEnv(t)
	a := startReplica(t, path, ns, "a")
	waitHolder(t, ns, "a", 5*time.Second)
	b := startReplica(t, path, ns, "b")

	var ids []model.OperationID
	for i := range 4 {
		via := []*replica{a, b}[i%2]
		req := labeled(t, fmt.Sprintf("data-%d", i), nil)
		op, err := via.svc.Submit(context.Background(), token(t, fmt.Sprintf("vol-%d", i)), req)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, op.ID)
	}
	for _, id := range ids {
		if got := waitDone(t, b.svc, id); got.Phase() != model.PhaseSucceeded {
			t.Fatalf("%s %s %+v", id, got.Phase(), got.Err())
		}
	}
	if n := creates(a.blk); n != 4 {
		t.Fatalf("holder CreateVolume calls %d, want 4", n)
	}
	if calls := b.blk.Calls(); len(calls) != 0 {
		t.Fatalf("follower backend calls %v, want none", calls)
	}
	if h := holder(t, ns); h != "a" {
		t.Fatalf("holder %q, want a", h)
	}
}

// A holder that stops renewing in the middle of a backend call keeps the
// lease until it expires; the other replica then finishes the operation
// once, and the old holder writes nothing when its call returns (#141 AC 5).
func TestKilledLeaseHolderOperationFinishesOnceOnTheOther(t *testing.T) {
	path, ns := kubeEnv(t)
	a := startReplica(t, path, ns, "a")
	waitHolder(t, ns, "a", 5*time.Second)
	entered, hang := make(chan struct{}), make(chan struct{})
	var once sync.Once
	a.blk.AfterCall(func(c fake.Call) {
		if c.Method == "CreateVolume" {
			once.Do(func() { close(entered) })
			<-hang
		}
	})
	b := startReplica(t, path, ns, "b")

	op, err := b.svc.Submit(context.Background(), token(t, "vol"), labeled(t, "data", nil))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("holder did not reach the backend")
	}
	a.stop()
	if h := holder(t, ns); h != "a" {
		t.Fatalf("holder %q while its call hangs, want a until the lease expires", h)
	}
	if took := waitHolder(t, ns, "b", 3*storage.LeaseDuration); took < storage.LeaseDuration/2 {
		t.Fatalf("takeover after %s, want the lease (%s) to expire first", took, storage.LeaseDuration)
	}
	if got := waitDone(t, b.svc, op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("taken over %s %+v", got.Phase(), got.Err())
	}
	if n := creates(b.blk); n != 1 {
		t.Fatalf("new holder CreateVolume calls %d, want 1", n)
	}

	before := read(t, b.svc, model.Project{})
	close(hang)
	select {
	case <-a.done:
	case <-time.After(5 * time.Second):
		t.Fatal("old holder did not return after its call")
	}
	if after := read(t, b.svc, model.Project{}); !reflect.DeepEqual(before, after) {
		t.Fatalf("old holder wrote after the takeover\n%+v\nwant\n%+v", after, before)
	}
	if n := records(t, ns, "volume"); n != 1 {
		t.Fatalf("volume ConfigMaps %d, want 1", n)
	}
	if h := holder(t, ns); h != "b" {
		t.Fatalf("holder %q after the old holder returned, want b", h)
	}
	again, err := b.svc.Submit(context.Background(), token(t, "vol"), labeled(t, "data", nil))
	if err != nil || again.ID != op.ID || again.Phase() != model.PhaseSucceeded {
		t.Fatalf("replay %+v %v, want %s succeeded", again, err, op.ID)
	}
}

// A clean stop releases the lease, so the other replica leads within a
// retry period instead of a lease duration.
func TestCleanStopHandsTheLeaseOver(t *testing.T) {
	path, ns := kubeEnv(t)
	a := startReplica(t, path, ns, "a")
	waitHolder(t, ns, "a", 5*time.Second)
	b := startReplica(t, path, ns, "b")
	a.stop()
	<-a.done
	if took := waitHolder(t, ns, "b", storage.LeaseDuration); took >= storage.LeaseDuration/2 {
		t.Fatalf("takeover after %s, want well under the lease (%s)", took, storage.LeaseDuration)
	}
	op, err := b.svc.Submit(context.Background(), token(t, "vol"), labeled(t, "data", nil))
	if err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, b.svc, op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("%s %+v", got.Phase(), got.Err())
	}
	if na, nb := creates(a.blk), creates(b.blk); na != 0 || nb != 1 {
		t.Fatalf("CreateVolume calls a %d b %d, want 0 and 1", na, nb)
	}
}
