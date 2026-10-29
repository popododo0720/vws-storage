package storage_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// attachingBlock is the fake block backend plus attachments by claim name,
// the way the kube backend reads them from the VirtualMachine spec that
// vws-compute writes on AttachVolume and clears on DetachVolume.
type attachingBlock struct {
	*fake.Block
	mu        sync.Mutex
	vms       map[string]string
	fail      error
	afterRead func()
	// beforeRead runs first in Attachments, without the lock held, so a
	// test can stall the read the way a slow API server does.
	beforeRead func(context.Context)
}

func newAttachingBlock() *attachingBlock {
	return &attachingBlock{Block: fake.NewBlock(), vms: map[string]string{}}
}

func (b *attachingBlock) Attachments(ctx context.Context) (map[string]string, error) {
	b.mu.Lock()
	before := b.beforeRead
	b.mu.Unlock()
	if before != nil {
		before(ctx)
		// A real list cut off by its context fails with the context error.
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("list virtualmachines: %w", err)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail != nil {
		return nil, b.fail
	}
	out := make(map[string]string, len(b.vms))
	for k, v := range b.vms {
		out[k] = v
	}
	if hook := b.afterRead; hook != nil {
		b.mu.Unlock()
		hook()
		b.mu.Lock()
	}
	return out, nil
}

func (b *attachingBlock) setAfterRead(fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.afterRead = fn
}

func (b *attachingBlock) setBeforeRead(fn func(context.Context)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.beforeRead = fn
}

func (b *attachingBlock) attach(id model.VolumeID, vm string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.vms[b.ClaimName(id)] = vm
}

func (b *attachingBlock) detach(id model.VolumeID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.vms, b.ClaimName(id))
}

func (b *attachingBlock) setFail(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fail = err
}

func (b *attachingBlock) deletes() int {
	n := 0
	for _, c := range b.Calls() {
		if c.Method == "DeleteVolume" {
			n++
		}
	}
	return n
}

// openAttaching opens a running service over blk.
func openAttaching(t *testing.T, blk *attachingBlock, edit ...func(*storage.Options)) *storage.Service {
	t.Helper()
	o := opts(storage.Memory(), blk.Block, fake.NewObjects(fake.ObjectOptions{}))
	o.Block = blk
	for _, fn := range edit {
		fn(&o)
	}
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
	t.Cleanup(func() {
		cancel()
		<-done
		svc.Close()
	})
	return svc
}

func createAvailable(t *testing.T, svc *storage.Service, tok string) model.Volume {
	t.Helper()
	op, err := svc.Submit(context.Background(), token(t, tok), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("create %s %+v", got.Phase(), got.Err())
	}
	vol, err := svc.Volume(context.Background(), reqTarget(t, op))
	if err != nil {
		t.Fatal(err)
	}
	return vol
}

// DeleteVolume on an attached volume is DEPENDENCY_MISSING and leaves the
// row and the backend object as they were (#152).
func TestDeleteVolumeRefusesAnAttachedVolume(t *testing.T) {
	blk := newAttachingBlock()
	svc := openAttaching(t, blk)
	ctx := context.Background()
	vol := createAvailable(t, svc, "vol")
	blk.attach(vol.ID, "vm-1")

	_, err := svc.Submit(ctx, token(t, "del"), model.DeleteVolumeRequest{Volume: vol.ID})
	if !errors.Is(err, model.ErrDependencyMissing) {
		t.Fatalf("delete of an attached volume: %v, want DEPENDENCY_MISSING", err)
	}
	if !strings.Contains(err.Error(), "vm-1") {
		t.Fatalf("error %q does not name the instance", err)
	}
	after, err := svc.Volume(ctx, vol.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := after.AsAvailable(); !ok || after.Meta.Version != vol.Meta.Version {
		t.Fatalf("volume changed: state %T version %d, was %d", after.State, after.Meta.Version, vol.Meta.Version)
	}
	if n := blk.deletes(); n != 0 {
		t.Fatalf("backend DeleteVolume called %d times", n)
	}
	// The refusal recorded no operation, so the same token is free for a
	// retry once the volume is detached.
	blk.detach(vol.ID)
	op, err := svc.Submit(ctx, token(t, "del"), model.DeleteVolumeRequest{Volume: vol.ID})
	if err != nil {
		t.Fatalf("delete after detach with the same token: %v", err)
	}
	if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("delete %s %+v", got.Phase(), got.Err())
	}
}

// Detach clears the attachment, so the next DeleteVolume removes the row and
// the backend object (#152).
func TestDeleteAfterDetachSucceeds(t *testing.T) {
	blk := newAttachingBlock()
	svc := openAttaching(t, blk)
	ctx := context.Background()
	vol := createAvailable(t, svc, "vol")
	blk.attach(vol.ID, "vm-1")
	_, err := svc.Submit(ctx, token(t, "del-1"), model.DeleteVolumeRequest{Volume: vol.ID})
	if !errors.Is(err, model.ErrDependencyMissing) {
		t.Fatalf("attached: %v", err)
	}
	blk.detach(vol.ID)
	op, err := svc.Submit(ctx, token(t, "del-2"), model.DeleteVolumeRequest{Volume: vol.ID})
	if err != nil {
		t.Fatalf("delete after detach: %v", err)
	}
	if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("delete %s %+v", got.Phase(), got.Err())
	}
	if _, err := svc.Volume(ctx, vol.ID); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("volume after delete: %v", err)
	}
	if n := blk.deletes(); n != 1 {
		t.Fatalf("backend DeleteVolume called %d times, want 1", n)
	}
}

// An attachment list the backend cannot read refuses the delete: the engine
// does not guess that the volume is detached.
func TestDeleteVolumeRefusesWhenAttachmentsAreUnreadable(t *testing.T) {
	blk := newAttachingBlock()
	svc := openAttaching(t, blk)
	ctx := context.Background()
	vol := createAvailable(t, svc, "vol")
	blk.setFail(errors.New("forbidden: list virtualmachines"))
	_, err := svc.Submit(ctx, token(t, "del"), model.DeleteVolumeRequest{Volume: vol.ID})
	if !errors.Is(err, model.ErrFailedPrecondition) || errors.Is(err, model.ErrDependencyMissing) {
		t.Fatalf("delete with unreadable attachments: %v, want FAILED_PRECONDITION (not ready)", err)
	}
	if strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("backend text leaked to the client: %q", err)
	}
	after, err := svc.Volume(ctx, vol.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Meta.Version != vol.Meta.Version {
		t.Fatalf("volume changed: version %d, was %d", after.Meta.Version, vol.Meta.Version)
	}
}

// An attach that lands after admission committed Deleting, before the
// backend delete runs, holds the backend object: the delete step reads the
// attachments again and retries while the instance uses the volume. Once
// the instance lets go, the delete finishes (#152).
func TestDeleteWaitsForAnAttachRacingTheAdmission(t *testing.T) {
	blk := newAttachingBlock()
	svc := openAttaching(t, blk)
	ctx := context.Background()
	vol := createAvailable(t, svc, "vol")

	// The instance spec gains the claim right after admission read the
	// attachments: the window vws-compute's own check cannot see.
	var once sync.Once
	blk.setAfterRead(func() { once.Do(func() { blk.attach(vol.ID, "vm-race") }) })
	op, err := svc.Submit(ctx, token(t, "del"), model.DeleteVolumeRequest{Volume: vol.ID})
	if err != nil {
		t.Fatalf("admission saw no attachment and must admit: %v", err)
	}
	blk.setAfterRead(nil)

	// Several retry rounds (RetryMax is 5ms) must leave the object alone.
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		cur, err := svc.Operation(ctx, op.ID)
		if err != nil {
			t.Fatal(err)
		}
		if cur.Terminal() {
			t.Fatalf("delete finished while attached: %s %+v", cur.Phase(), cur.Err())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := blk.deletes(); n != 0 {
		t.Fatalf("backend DeleteVolume called %d times while attached", n)
	}
	cur, err := svc.Volume(ctx, vol.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cur.AsDeleting(); !ok {
		t.Fatalf("volume state %T, want Deleting", cur.State)
	}

	blk.detach(vol.ID)
	if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("delete after detach %s %+v", got.Phase(), got.Err())
	}
	if n := blk.deletes(); n != 1 {
		t.Fatalf("backend DeleteVolume called %d times, want 1", n)
	}
	if _, err := svc.Volume(ctx, vol.ID); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("volume after delete: %v", err)
	}
}

// A slow attachment list (a stalled API server) must not hold the store:
// an unrelated Submit finishes while a DeleteVolume waits on the list, and
// the delete is admitted once the list answers (#152).
func TestSlowAttachmentsDoNotBlockOtherSubmits(t *testing.T) {
	blk := newAttachingBlock()
	svc := openAttaching(t, blk)
	ctx := context.Background()
	vol := createAvailable(t, svc, "vol")
	delTok, otherTok, other := token(t, "del"), token(t, "other"), volumeReq(t)

	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	blk.setBeforeRead(func(ctx context.Context) {
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	type result struct {
		op  storage.OperationStatus
		err error
	}
	delDone := make(chan result, 1)
	go func() {
		op, err := svc.Submit(ctx, delTok, model.DeleteVolumeRequest{Volume: vol.ID})
		delDone <- result{op, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteVolume never read the attachments")
	}

	otherDone := make(chan error, 1)
	go func() {
		_, err := svc.Submit(ctx, otherTok, other)
		otherDone <- err
	}()
	select {
	case err := <-otherDone:
		if err != nil {
			close(release)
			t.Fatalf("unrelated CreateVolume: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(release)
		<-otherDone
		t.Fatal("unrelated CreateVolume waited for the attachment list")
	}

	close(release)
	res := <-delDone
	if res.err != nil {
		t.Fatalf("delete after the list answered: %v", res.err)
	}
	if got := waitDone(t, svc, res.op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("delete %s %+v", got.Phase(), got.Err())
	}
}

// An attachment list that never answers is cut off by the attachment
// timeout: the delete is refused as not ready instead of hanging, and the
// volume is unchanged (#152).
func TestDeleteVolumeAttachmentReadIsBounded(t *testing.T) {
	blk := newAttachingBlock()
	svc := openAttaching(t, blk, func(o *storage.Options) { o.AttachmentTimeout = 50 * time.Millisecond })
	ctx := context.Background()
	vol := createAvailable(t, svc, "vol")
	blk.setBeforeRead(func(ctx context.Context) { <-ctx.Done() })

	start := time.Now()
	_, err := svc.Submit(ctx, token(t, "del"), model.DeleteVolumeRequest{Volume: vol.ID})
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("delete waited %s for a stalled list", took)
	}
	if !errors.Is(err, model.ErrFailedPrecondition) || errors.Is(err, model.ErrDependencyMissing) {
		t.Fatalf("delete with a stalled list: %v, want FAILED_PRECONDITION (not ready)", err)
	}
	after, err := svc.Volume(ctx, vol.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := after.AsAvailable(); !ok || after.Meta.Version != vol.Meta.Version {
		t.Fatalf("volume changed: state %T version %d, was %d", after.State, after.Meta.Version, vol.Meta.Version)
	}
}
