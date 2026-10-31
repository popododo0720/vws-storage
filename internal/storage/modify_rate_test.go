package storage_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// clock is a settable Options.Now.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// EC2 ModifyVolume allows four modifications per volume in a rolling 24
// hours (#157). Size, type and performance changes count, through Expand and
// Modify alike; label changes (EC2 tags) do not. The fifth is refused with
// ErrModificationRateExceeded naming when the next one is allowed, a resent
// request with the same token is the same operation and is not refused, the
// count survives a restart, and the window rolls from the oldest one.
func TestModifyVolumeRateLimitIsFourPerRolling24Hours(t *testing.T) {
	blk := fake.NewBlock()
	obj := fake.NewObjects(fake.ObjectOptions{})
	path := filepath.Join(t.TempDir(), "vws.db")
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	clk := &clock{t: t0}
	o := opts(storage.SQLite(path), blk, obj)
	o.Now = clk.now
	svc, stop := openWith(t, o)
	ctx := context.Background()
	volOp, err := svc.Submit(ctx, token(t, "vol"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	id := waitDone(t, svc, volOp.ID).Target().(model.VolumeID)
	labels, err := model.ParseLabels(map[string]string{"team": "storage"})
	if err != nil {
		t.Fatal(err)
	}
	mustSucceed := func(svc *storage.Service, tok string, req model.Request) storage.OperationStatus {
		t.Helper()
		op, err := svc.Submit(context.Background(), token(t, tok), req)
		if err != nil {
			t.Fatalf("%s: %v", tok, err)
		}
		done := waitDone(t, svc, op.ID)
		if done.Phase() != model.PhaseSucceeded {
			t.Fatalf("%s: phase %v", tok, done.Phase())
		}
		return done
	}
	mustSucceed(svc, "m1", model.ExpandVolumeRequest{Volume: id, Size: 2 * model.GiB})
	clk.set(t0.Add(time.Hour))
	mustSucceed(svc, "labels-1", model.ModifyVolumeRequest{Volume: id, SetLabels: true, Labels: labels})
	mustSucceed(svc, "m2", model.ModifyVolumeRequest{Volume: id, Type: model.VolumeTypeGP3, Size: 3 * model.GiB})
	clk.set(t0.Add(2 * time.Hour))
	mustSucceed(svc, "m3", model.ModifyVolumeRequest{
		Volume: id, Type: model.VolumeTypeGP3, SetIops: true, Iops: 3000, SetThroughput: true, Throughput: 250,
	})
	clk.set(t0.Add(3 * time.Hour))
	fourth := model.ModifyVolumeRequest{Volume: id, Type: model.VolumeTypeGP2}
	m4 := mustSucceed(svc, "m4", fourth)

	vol, err := svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if n := vol.Modified.Count(clk.now()); n != model.ModificationLimit {
		t.Fatalf("counted %d modifications, want %d (labels must not count)", n, model.ModificationLimit)
	}
	wait := "wait until 2026-11-02T00:00:00.000Z"
	refused := func(svc *storage.Service, tok string, req model.Request) {
		t.Helper()
		_, err := svc.Submit(ctx, token(t, tok), req)
		if !errors.Is(err, model.ErrModificationRateExceeded) || !strings.Contains(err.Error(), wait) {
			t.Fatalf("%s: want rate exceeded with %q, got %v", tok, wait, err)
		}
	}
	refused(svc, "m5-type", model.ModifyVolumeRequest{Volume: id, Type: model.VolumeTypeGP3})
	refused(svc, "m5-size", model.ExpandVolumeRequest{Volume: id, Size: 4 * model.GiB})
	again, err := svc.Submit(ctx, token(t, "m4"), fourth)
	if err != nil || again.ID != m4.ID {
		t.Fatalf("resent fourth: %s err %v, want %s", again.ID, err, m4.ID)
	}
	mustSucceed(svc, "labels-2", model.ModifyVolumeRequest{Volume: id, Type: model.VolumeTypeGP2, SetLabels: true})
	mustSucceed(svc, "same-size", model.ExpandVolumeRequest{Volume: id, Size: 3 * model.GiB})

	stop()
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	svc, stop = openWith(t, o)
	defer func() {
		stop()
		_ = svc.Close()
	}()
	refused(svc, "m5-after-restart", model.ModifyVolumeRequest{Volume: id, Type: model.VolumeTypeGP3})

	// The first modification leaves the window 24 hours after it started.
	clk.set(t0.Add(model.ModificationWindow - time.Microsecond))
	refused(svc, "m5-just-before", model.ModifyVolumeRequest{Volume: id, Type: model.VolumeTypeGP3})
	clk.set(t0.Add(model.ModificationWindow))
	mustSucceed(svc, "m5", model.ModifyVolumeRequest{Volume: id, Type: model.VolumeTypeGP3})
	wait = "wait until 2026-11-02T01:00:00.000Z"
	refused(svc, "m6", model.ModifyVolumeRequest{Volume: id, Type: model.VolumeTypeGP2})
	got, err := svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Type != model.VolumeTypeGP3 || got.Spec.Size != 3*model.GiB || got.Labels != (model.Labels{}) {
		t.Fatalf("volume %+v", got)
	}
}

func openWith(t *testing.T, o storage.Options) (*storage.Service, func()) {
	t.Helper()
	svc, err := storage.Open(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = svc.Run(ctx)
		close(done)
	}()
	return svc, func() {
		cancel()
		<-done
	}
}

// A growth that fails changes nothing, so it does not count against the
// EC2 limit; the next one is still admitted (#157).
func TestFailedExpansionDoesNotCountAgainstTheModificationLimit(t *testing.T) {
	blk := fake.NewBlock()
	svc, stop := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer stop()
	ctx := context.Background()
	volOp, err := svc.Submit(ctx, token(t, "vol"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	id := waitDone(t, svc, volOp.ID).Target().(model.VolumeID)
	blk.FailNext("Expand", backend.Permanent(backend.ErrInvalid))
	op, err := svc.Submit(ctx, token(t, "grow-fails"), model.ExpandVolumeRequest{Volume: id, Size: 2 * model.GiB})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitDone(t, svc, op.ID); done.Phase() != model.PhaseFailed {
		t.Fatalf("phase %v", done.Phase())
	}
	vol, err := svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if n := vol.Modified.Count(time.Now()); n != 0 || vol.Spec.Size != model.GiB {
		t.Fatalf("after failed growth: counted %d, size %d", n, vol.Spec.Size)
	}
	op, err = svc.Submit(ctx, token(t, "grow"), model.ExpandVolumeRequest{Volume: id, Size: 2 * model.GiB})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitDone(t, svc, op.ID); done.Phase() != model.PhaseSucceeded {
		t.Fatalf("phase %v", done.Phase())
	}
	vol, err = svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if n := vol.Modified.Count(time.Now()); n != 1 {
		t.Fatalf("after growth: counted %d", n)
	}
}
