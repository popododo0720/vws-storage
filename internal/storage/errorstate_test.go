package storage_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

const gaveUp = "the CDI DataVolume failed 4 times (retry limit 3)"

// failVolume runs a create whose backend call fails permanently and returns
// the volume id.
func failVolume(t *testing.T, svc *storage.Service, tok string) model.VolumeID {
	t.Helper()
	op, err := svc.Submit(context.Background(), token(t, tok), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, svc, op.ID)
	if got.Phase() != model.PhaseFailed || got.Err().Code != model.CodeDeadlineExceeded {
		t.Fatalf("phase %s err %+v", got.Phase(), got.Err())
	}
	return reqTarget(t, got)
}

// A permanent create failure keeps the volume, in error, with the reason,
// and leaves the backend resources for DeleteVolume.
func TestPermanentCreateFailureKeepsTheVolumeInError(t *testing.T) {
	blk := fake.NewBlock()
	blk.FailNext("CreateVolume", backend.Permanent(fmt.Errorf("%w: %s", backend.ErrGaveUp, gaveUp)))
	svc, cancel := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	id := failVolume(t, svc, "err-1")
	vol, err := svc.Volume(context.Background(), id)
	if err != nil {
		t.Fatalf("volume after permanent failure: %v", err)
	}
	if _, ok := vol.AsAvailable(); ok {
		t.Fatal("volume in error reads as available")
	}
	if _, ok := vol.AsCreating(); ok {
		t.Fatal("volume in error reads as creating")
	}
	for _, c := range blk.Calls() {
		if c.Method == "DeleteVolume" {
			t.Fatal("a permanent create failure deleted the backend volume")
		}
	}
	if got := svc.ClaimName(vol); got != "" {
		t.Fatalf("claim name of a volume in error %q: it must not be attachable", got)
	}
}

// A volume in error refuses snapshot and expand, and DeleteVolume removes it.
func TestVolumeInErrorRefusesWorkAndDeletes(t *testing.T) {
	blk := fake.NewBlock()
	blk.FailNext("CreateVolume", backend.Permanent(fmt.Errorf("%w: %s", backend.ErrGaveUp, gaveUp)))
	svc, cancel := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	ctx := context.Background()
	id := failVolume(t, svc, "err-2")
	if _, err := svc.Submit(ctx, token(t, "snap"), model.CreateSnapshotRequest{Source: id}); !errors.Is(err, model.ErrFailedPrecondition) {
		t.Fatalf("snapshot of a volume in error: %v", err)
	}
	if _, err := svc.Submit(ctx, token(t, "grow"), model.ExpandVolumeRequest{Volume: id, Size: 2 * model.GiB}); !errors.Is(err, model.ErrFailedPrecondition) {
		t.Fatalf("expand of a volume in error: %v", err)
	}
	again := volumeReq(t)
	again.ID = id
	if _, err := svc.Submit(ctx, token(t, "again"), again); !errors.Is(err, model.ErrFailedPrecondition) {
		t.Fatalf("create replay of a volume in error: %v", err)
	}
	del, err := svc.Submit(ctx, token(t, "del"), model.DeleteVolumeRequest{Volume: id})
	if err != nil {
		t.Fatalf("delete of a volume in error: %v", err)
	}
	if got := waitDone(t, svc, del.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("delete phase %s %+v", got.Phase(), got.Err())
	}
	if _, err := svc.Volume(ctx, id); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("volume after delete: %v", err)
	}
}

// A failed delete of a volume in error leaves it in error, not available.
func TestFailedDeleteOfAVolumeInErrorStaysInError(t *testing.T) {
	blk := fake.NewBlock()
	blk.FailNext("CreateVolume", backend.Permanent(fmt.Errorf("%w: %s", backend.ErrGaveUp, gaveUp)))
	svc, cancel := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	ctx := context.Background()
	id := failVolume(t, svc, "err-3")
	blk.FailNext("DeleteVolume", backend.Permanent(errors.New("cleanup failed")))
	del, err := svc.Submit(ctx, token(t, "del"), model.DeleteVolumeRequest{Volume: id})
	if err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, del.ID); got.Phase() != model.PhaseFailed {
		t.Fatalf("delete phase %s", got.Phase())
	}
	vol, err := svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := vol.AsAvailable(); ok {
		t.Fatal("a failed delete made a volume in error available")
	}
}

// The error state and its reason survive a restart.
func TestVolumeErrorSurvivesRestart(t *testing.T) {
	blk := fake.NewBlock()
	obj := fake.NewObjects(fake.ObjectOptions{})
	path := filepath.Join(t.TempDir(), "vws.db")
	blk.FailNext("CreateVolume", backend.Permanent(fmt.Errorf("%w: %s", backend.ErrGaveUp, gaveUp)))
	svc, stop := openRunning(t, storage.SQLite(path), blk, obj)
	id := failVolume(t, svc, "err-4")
	before, err := svc.Volume(context.Background(), id)
	if err != nil {
		t.Fatalf("volume before restart: %v", err)
	}
	stop()
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	svc, err = storage.Open(context.Background(), opts(storage.SQLite(path), blk, obj))
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	after, err := svc.Volume(context.Background(), id)
	if err != nil {
		t.Fatalf("volume after restart: %v", err)
	}
	if fmt.Sprintf("%#v", after.State) != fmt.Sprintf("%#v", before.State) {
		t.Fatalf("state after restart %#v want %#v", after.State, before.State)
	}
	if _, ok := after.AsAvailable(); ok {
		t.Fatal("volume in error reads as available after restart")
	}
}
