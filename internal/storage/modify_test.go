package storage_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// Labels and type change in the admission write, the operation is already
// succeeded, no backend object is touched, and the values survive a restart
// (#87). The same request_id is the same operation; a stale etag is refused.
func TestModifyVolumeSetsLabelsAndTypeAndSurvivesRestart(t *testing.T) {
	blk := fake.NewBlock()
	obj := fake.NewObjects(fake.ObjectOptions{})
	path := filepath.Join(t.TempDir(), "vws.db")
	svc, stop := openRunning(t, storage.SQLite(path), blk, obj)
	ctx := context.Background()
	volOp, err := svc.Submit(ctx, token(t, "vol"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	id := waitDone(t, svc, volOp.ID).Target().(model.VolumeID)
	before, err := svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	calls := len(blk.Calls())
	labels, err := model.ParseLabels(map[string]string{"team": "storage"})
	if err != nil {
		t.Fatal(err)
	}
	req := model.ModifyVolumeRequest{Volume: id, SetLabels: true, Labels: labels, Type: model.VolumeTypeStandard, Expected: before.Meta.Version}
	first, err := svc.Submit(ctx, token(t, "mod"), req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.Submit(ctx, token(t, "mod"), req)
	if err != nil || again.ID != first.ID || first.Phase() != model.PhaseSucceeded {
		t.Fatalf("first %s %s again %s err %v", first.ID, first.Phase(), again.ID, err)
	}
	if len(blk.Calls()) != calls {
		t.Fatalf("backend called: %v", blk.Calls()[calls:])
	}
	if _, err := svc.Submit(ctx, token(t, "stale"), req); !errors.Is(err, model.ErrETagMismatch) {
		t.Fatalf("stale etag: %v", err)
	}
	stop()
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	svc, err = storage.Open(ctx, opts(storage.SQLite(path), blk, obj))
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	after, err := svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if after.Labels != labels || after.Spec.Type != model.VolumeTypeStandard || after.Spec.Size != before.Spec.Size || after.Meta.Version != before.Meta.Version+1 {
		t.Fatalf("after restart %+v", after)
	}
	if _, ok := after.AsAvailable(); !ok {
		t.Fatalf("state %v", after.State)
	}
}

// Clearing labels keeps the type, and a missing volume is not found.
func TestModifyVolumeClearsLabelsAndRejectsMissing(t *testing.T) {
	svc, cancel := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	ctx := context.Background()
	create := volumeReq(t)
	labels, err := model.ParseLabels(map[string]string{"a": "b"})
	if err != nil {
		t.Fatal(err)
	}
	create.Labels = labels
	volOp, err := svc.Submit(ctx, token(t, "vol"), create)
	if err != nil {
		t.Fatal(err)
	}
	id := waitDone(t, svc, volOp.ID).Target().(model.VolumeID)
	if _, err := svc.Submit(ctx, token(t, "clear"), model.ModifyVolumeRequest{Volume: id, SetLabels: true}); err != nil {
		t.Fatal(err)
	}
	vol, err := svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if vol.Labels != (model.Labels{}) || vol.Spec.Type != model.VolumeTypeGP2 {
		t.Fatalf("volume %+v", vol)
	}
	if _, err := svc.Submit(ctx, token(t, "missing"), model.ModifyVolumeRequest{Volume: model.NewVolumeID(), SetLabels: true}); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

// Size, type, performance and labels in one request grow the volume through
// the backend and store everything in the write that finishes the growth. A
// crash after the backend resize resumes the same operation and still stores
// the carried values (#87). The crash here is a stop between admission and
// the runner: the stored operation alone must carry the change.
func TestModifyVolumeWithGrowthResumesAfterCrash(t *testing.T) {
	blk := fake.NewBlock()
	obj := fake.NewObjects(fake.ObjectOptions{})
	path := filepath.Join(t.TempDir(), "vws.db")
	ctx := context.Background()
	svc, stop := openRunning(t, storage.SQLite(path), blk, obj)
	volOp, err := svc.Submit(ctx, token(t, "vol"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	id := waitDone(t, svc, volOp.ID).Target().(model.VolumeID)
	stop()
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	svc, err = storage.Open(ctx, opts(storage.SQLite(path), blk, obj))
	if err != nil {
		t.Fatal(err)
	}
	labels, err := model.ParseLabels(map[string]string{"team": "storage"})
	if err != nil {
		t.Fatal(err)
	}
	req := model.ModifyVolumeRequest{
		Volume: id, SetLabels: true, Labels: labels, Type: model.VolumeTypeIO2, Size: 8 * model.GiB,
		SetIops: true, Iops: 8000,
	}
	op, err := svc.Submit(ctx, token(t, "grow-retype"), req)
	if err != nil {
		t.Fatal(err)
	}
	if mid, err := svc.Volume(ctx, id); err != nil || mid.Spec.Type != model.VolumeTypeGP2 || mid.Spec.Size != model.GiB {
		t.Fatalf("admission changed the volume: %+v err %v", mid, err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	svc, stop = openRunning(t, storage.SQLite(path), blk, obj)
	defer func() {
		stop()
		svc.Close()
	}()
	if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("phase %s %+v", got.Phase(), got.Err())
	}
	vol, err := svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if s := vol.Spec; s.Size != 8*model.GiB || s.Type != model.VolumeTypeIO2 || s.Iops != 8000 || vol.Labels != labels {
		t.Fatalf("volume %+v", vol)
	}
	var expands int
	for _, c := range blk.Calls() {
		if c.Method == "Expand" {
			expands++
		}
	}
	if expands != 1 {
		t.Fatalf("backend Expand calls %d, want 1", expands)
	}
	again, err := svc.Submit(ctx, token(t, "grow-retype"), req)
	if err != nil || again.ID != op.ID {
		t.Fatalf("replay %s want %s err %v", again.ID, op.ID, err)
	}
}
