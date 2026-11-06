package kube

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/storetest"
)

var kc kubernetes.Interface

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Stderr.WriteString("KUBEBUILDER_ASSETS unset: skipping envtest tests\n")
		os.Exit(0)
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		panic(err)
	}
	kc = kubernetes.NewForConfigOrDie(cfg)
	code := m.Run()
	_ = env.Stop()
	os.Exit(code)
}

var nsSeq atomic.Int64

func namespace(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("kubestore-%d", nsSeq.Add(1))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if _, err := kc.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	return name
}

func open(t *testing.T, o Options) *Store {
	t.Helper()
	st, err := Open(context.Background(), kc, o)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestKubeStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store { return open(t, Options{Namespace: namespace(t)}) })
}

func admit(t *testing.T, token string) model.Admitted {
	t.Helper()
	zone, err := model.ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	tok, err := model.ParseClientToken(token)
	if err != nil {
		t.Fatal(err)
	}
	req := model.CreateVolumeRequest{
		Spec: model.VolumeSpec{Size: model.GiB, Zone: zone, Type: model.VolumeTypeGP2, Source: model.NoSource()},
	}
	claim, err := model.Claim(tok, req)
	if err != nil {
		t.Fatal(err)
	}
	a, err := model.NewVolume(
		model.Start{Op: model.NewOperationID(), Claim: claim, Now: time.Now()},
		model.NewVolumeID(),
		req,
	)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func volumeOf(t *testing.T, a model.Admitted) model.VolumeID {
	t.Helper()
	w, ok := a.Op.Work.(model.VolumeCreation)
	if !ok {
		t.Fatalf("work is %T", a.Op.Work)
	}
	return w.Volume
}

// stageWrites runs a change through Apply without committing it.
func stageWrites(t *testing.T, s *Store, ch model.Change) []write {
	t.Helper()
	st := &stage{s: s, slots: map[string]*slot{}}
	if err := store.Apply(context.Background(), st, ch); err != nil {
		t.Fatal(err)
	}
	return st.writes()
}

func TestCrashAfterCommitIsFinishedByTheNextWriter(t *testing.T) {
	ctx := context.Background()
	ns := namespace(t)
	crashed := open(t, Options{Namespace: ns})
	a := admit(t, "crash")
	head, err := crashed.clearHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	in := intent{Txn: newTxn(), Start: time.Now(), Writes: stageWrites(t, crashed, a.Change)}
	if len(in.Writes) < 2 {
		t.Fatalf("want a multi-record change, got %d writes", len(in.Writes))
	}
	committed, err := crashed.commit(ctx, head, in)
	if err != nil {
		t.Fatal(err)
	}
	// Roll only the first write, as if the process died between two writes.
	if err := crashed.roll(ctx, in.Txn, in.Writes[0]); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	next := open(t, Options{Namespace: ns, TakeoverAfter: 300 * time.Millisecond})
	if waited := time.Since(start); waited < 200*time.Millisecond {
		t.Fatalf("took over a young intent after %v", waited)
	}
	vol, err := next.Volume(ctx, volumeOf(t, a))
	if err != nil {
		t.Fatalf("volume after repair: %v", err)
	}
	if _, ok := vol.AsCreating(); !ok {
		t.Fatalf("state %v", vol.State)
	}
	if _, err := next.Operation(ctx, a.Op.ID); err != nil {
		t.Fatalf("operation after repair: %v", err)
	}
	h, err := next.head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := h.Data[dataIntent]; ok {
		t.Fatal("head still holds the intent")
	}

	// The crashed writer wakes up and finishes too: nothing changes.
	if err := crashed.finish(ctx, committed, in); err != nil {
		t.Fatalf("late finish: %v", err)
	}
	again, err := next.Volume(ctx, vol.ID)
	if err != nil || !model.SameRecord(again, vol) {
		t.Fatalf("late finish changed the volume: %v", err)
	}
	due, err := next.Runnable(ctx, time.Now().Add(time.Hour), 10)
	if err != nil || len(due) != 1 || due[0].ID != a.Op.ID {
		t.Fatalf("runnable after repair %v %v", due, err)
	}
}

func TestConcurrentWritersCommitEachChangeOnce(t *testing.T) {
	ctx := context.Background()
	ns := namespace(t)
	stores := []*Store{open(t, Options{Namespace: ns}), open(t, Options{Namespace: ns})}

	same := admit(t, "same")
	var ok, stale atomic.Int32
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := stores[i%2].Update(ctx, func(store.Reader) (model.Change, error) { return same.Change, nil })
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, model.ErrStale):
				stale.Add(1)
			default:
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || stale.Load() != 3 {
		t.Fatalf("same change: %d committed, %d stale", ok.Load(), stale.Load())
	}

	const n = 10
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a := admit(t, fmt.Sprintf("tok-%d", i))
			if err := stores[i%2].Update(ctx, func(store.Reader) (model.Change, error) { return a.Change, nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	vols, err := stores[0].ListVolumes(ctx, model.VolumeID{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(vols) != n+1 {
		t.Fatalf("volumes %d, want %d", len(vols), n+1)
	}
	page, err := stores[1].ListVolumes(ctx, vols[2].ID, 3)
	if err != nil || len(page) != 3 || page[0].ID != vols[3].ID {
		t.Fatalf("page after %s: %v %v", vols[2].ID, page, err)
	}
}

func TestRemovedRecordIsATombstoneUntilPurged(t *testing.T) {
	ctx := context.Background()
	ns := namespace(t)
	s := open(t, Options{Namespace: ns})
	a := admit(t, "gone")
	if err := s.Update(ctx, func(store.Reader) (model.Change, error) { return a.Change, nil }); err != nil {
		t.Fatal(err)
	}
	id := volumeOf(t, a)
	removeAll(t, s, a.Change)
	if _, err := s.Volume(ctx, id); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("removed volume err = %v", err)
	}
	if vols, err := s.ListVolumes(ctx, model.VolumeID{}, 0); err != nil || len(vols) != 0 {
		t.Fatalf("list after remove %v %v", vols, err)
	}
	name, err := recordName(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kc.CoreV1().ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	// Re-creating the same keys updates the tombstones.
	if err := s.Update(ctx, func(store.Reader) (model.Change, error) { return a.Change, nil }); err != nil {
		t.Fatalf("recreate over tombstone: %v", err)
	}
	if _, err := s.Volume(ctx, id); err != nil {
		t.Fatal(err)
	}
	removeAll(t, s, a.Change)

	open(t, Options{Namespace: ns, TombstoneTTL: time.Nanosecond})
	if _, err := kc.CoreV1().ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Fatal("tombstone survived the purge")
	}
}

// removeAll removes every record ch puts.
func removeAll(t *testing.T, s *Store, ch model.Change) {
	t.Helper()
	if err := s.transact(context.Background(), func(ctx context.Context) ([]write, error) {
		st := &stage{s: s, slots: map[string]*slot{}}
		for _, w := range ch.Writes() {
			if put, ok := w.(model.Put); ok {
				if err := st.Delete(ctx, put.Next.RecordKey()); err != nil {
					return nil, err
				}
			}
		}
		return st.writes(), nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestObjectRecordsAreNotKept(t *testing.T) {
	ctx := context.Background()
	s := open(t, Options{Namespace: namespace(t)})
	if _, err := s.Object(ctx, model.ObjectPath{}); !errors.Is(err, errObjects) {
		t.Fatalf("Object err = %v", err)
	}
	if _, err := s.HasObjects(ctx, model.BucketName{}); !errors.Is(err, errObjects) {
		t.Fatalf("HasObjects err = %v", err)
	}
	st := &stage{s: s, slots: map[string]*slot{}}
	if err := st.Put(ctx, model.Object{}); !errors.Is(err, errObjects) {
		t.Fatalf("Put object err = %v", err)
	}
}

func TestRecordNamesAreValidForAnyKey(t *testing.T) {
	tok, err := model.ParseClientToken(strings.Repeat("Z", 64))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []model.Key{model.NewVolumeID(), model.TokenKey{Token: tok}, model.NewOperationID()} {
		name, err := recordName(key)
		if err != nil {
			t.Fatal(err)
		}
		if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
			t.Fatalf("%q: %v", name, errs)
		}
	}
	if _, err := recordName(model.TokenKey{}); !errors.Is(err, model.ErrInvalidArgument) {
		t.Fatalf("empty key err = %v", err)
	}
}

func TestOpenNeedsANamespace(t *testing.T) {
	if _, err := Open(context.Background(), kc, Options{}); !errors.Is(err, model.ErrInvalidArgument) {
		t.Fatalf("err = %v", err)
	}
}
