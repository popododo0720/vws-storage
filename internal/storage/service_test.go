package storage_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

func TestCreateVolumeIsIdempotent(t *testing.T) {
	svc, cancel := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	req := volumeReq(t)
	tok := token(t, "vol-create-1")
	first, err := svc.Submit(context.Background(), tok, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Submit(context.Background(), tok, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("ids %s %s", first.ID, second.ID)
	}
	other := req
	other.Spec.Size = 2 * model.GiB
	_, err = svc.Submit(context.Background(), tok, other)
	if !errors.Is(err, model.ErrTokenReused) {
		t.Fatalf("err %v", err)
	}
	got := waitDone(t, svc, first.ID)
	if got.Phase() != model.PhaseSucceeded {
		t.Fatalf("phase %s err %+v", got.Phase(), got.Err())
	}
	vol, err := svc.Volume(context.Background(), reqTarget(t, got))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := vol.AsAvailable(); !ok {
		t.Fatalf("state %v", vol.State)
	}
}

func TestCreateVolumeResumesAfterCrash(t *testing.T) {
	blk := fake.NewBlock()
	obj := fake.NewObjects(fake.ObjectOptions{})
	path := filepath.Join(t.TempDir(), "vws.db")
	runCtx, kill := context.WithCancel(context.Background())
	blk.AfterCall(func(c fake.Call) {
		if c.Method == "CreateVolume" {
			kill()
		}
	})
	svc, err := storage.Open(context.Background(), opts(storage.SQLite(path), blk, obj))
	if err != nil {
		t.Fatal(err)
	}
	op, err := svc.Submit(context.Background(), token(t, "crash-1"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Run(runCtx); err != nil {
		t.Fatal(err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	blk.AfterCall(nil)

	svc, err = storage.Open(context.Background(), opts(storage.SQLite(path), blk, obj))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = svc.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
		svc.Close()
	}()
	got := waitDone(t, svc, op.ID)
	if got.Phase() != model.PhaseSucceeded {
		t.Fatalf("phase %s %+v", got.Phase(), got.Err())
	}
	var n int
	for _, c := range blk.Calls() {
		if c.Method == "CreateVolume" {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("CreateVolume calls = %d", n)
	}
	again, err := svc.Submit(context.Background(), token(t, "crash-1"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != op.ID {
		t.Fatalf("replay id %s want %s", again.ID, op.ID)
	}
}

func TestFailedCreateLeavesNoVolume(t *testing.T) {
	blk := fake.NewBlock()
	blk.FailNext("CreateVolume", backend.Permanent(errors.New("pool full")))
	svc, cancel := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	op, err := svc.Submit(context.Background(), token(t, "fail-1"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, svc, op.ID)
	if got.Phase() != model.PhaseFailed {
		t.Fatalf("phase %s", got.Phase())
	}
	if _, err := svc.Volume(context.Background(), got.Target().(model.VolumeID)); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("volume err %v", err)
	}
}

func TestPermanentUndoDropsVolume(t *testing.T) {
	blk := fake.NewBlock()
	blk.FailNext("CreateVolume", backend.Permanent(errors.New("pool full")))
	blk.FailNext("DeleteVolume", backend.Permanent(errors.New("cleanup failed")))
	svc, cancel := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	op, err := svc.Submit(context.Background(), token(t, "undo-1"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, svc, op.ID)
	if got.Phase() != model.PhaseFailed {
		t.Fatalf("phase %s %+v", got.Phase(), got.Err())
	}
	if _, err := svc.Volume(context.Background(), got.Target().(model.VolumeID)); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("volume err %v", err)
	}
}

func TestListBucketsSecondPage(t *testing.T) {
	svc, cancel := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	for _, name := range []string{"bkt-a", "bkt-b", "bkt-c"} {
		bucket, err := model.ParseBucketName(name)
		if err != nil {
			t.Fatal(err)
		}
		op, err := svc.Submit(context.Background(), token(t, name), model.CreateBucketRequest{Bucket: bucket})
		if err != nil {
			t.Fatal(err)
		}
		if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
			t.Fatal(got.Err())
		}
	}
	page, err := svc.ListBuckets(context.Background(), model.PageRequest{Size: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.NextToken == "" {
		t.Fatalf("page %+v", page)
	}
	next, err := svc.ListBuckets(context.Background(), model.PageRequest{Size: 2, Token: page.NextToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Items) != 1 || next.NextToken != "" {
		t.Fatalf("next %+v", next)
	}
}

func TestSnapshotBlocksVolumeDelete(t *testing.T) {
	svc, stop := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	volOp, err := svc.Submit(context.Background(), token(t, "vol"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	vol := waitDone(t, svc, volOp.ID)
	id := vol.Target().(model.VolumeID)
	stop()
	snap, err := svc.Submit(context.Background(), token(t, "snap"), model.CreateSnapshotRequest{Source: id})
	if err != nil {
		t.Fatal(err)
	}
	if snap.Terminal() {
		t.Fatal("snapshot finished before run")
	}
	_, err = svc.Submit(context.Background(), token(t, "del"), model.DeleteVolumeRequest{Volume: id})
	if !errors.Is(err, model.ErrFailedPrecondition) {
		t.Fatalf("err %v", err)
	}
	svc.Close()
}

func TestPutAndGetObject(t *testing.T) {
	obj := fake.NewObjects(fake.ObjectOptions{Now: time.Now})
	svc, cancel := openRunning(t, storage.Memory(), fake.NewBlock(), obj)
	defer cancel()
	bucket, err := model.ParseBucketName("logs")
	if err != nil {
		t.Fatal(err)
	}
	bop, err := svc.Submit(context.Background(), token(t, "bkt"), model.CreateBucketRequest{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, bop.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatal(got.Err())
	}
	key, err := model.ParseObjectKey("day.bin")
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("hello")
	put, err := svc.Submit(context.Background(), token(t, "put"), model.PutObjectRequest{
		Path: model.ObjectPath{Bucket: bucket, Key: key}, Size: model.Bytes(len(body)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if put.Upload == nil {
		t.Fatal("missing upload locator")
	}
	if err := obj.Accept(*put.Upload, body); err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, put.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("%s %+v", got.Phase(), got.Err())
	}
	head, err := svc.Object(context.Background(), model.ObjectPath{Bucket: bucket, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	if head.Size != model.Bytes(len(body)) {
		t.Fatalf("size %d", head.Size)
	}
	_, loc, err := svc.LocateObject(context.Background(), head.Path)
	if err != nil || loc.Method != "GET" || loc.URL == "" {
		t.Fatalf("locator %+v %v", loc, err)
	}
	_, err = svc.Submit(context.Background(), token(t, "del-bkt"), model.DeleteBucketRequest{Bucket: bucket})
	if !errors.Is(err, model.ErrFailedPrecondition) {
		t.Fatalf("delete bucket %v", err)
	}
}

func TestListMissingBucket(t *testing.T) {
	svc, cancel := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	bucket, err := model.ParseBucketName("missing")
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.ListObjects(context.Background(), bucket, "", model.PageRequest{})
	if !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("err %v", err)
	}
}

func TestUploadDeadlineFails(t *testing.T) {
	obj := fake.NewObjects(fake.ObjectOptions{})
	opt := opts(storage.Memory(), fake.NewBlock(), obj)
	opt.UploadWindow = 40 * time.Millisecond
	svc, err := storage.Open(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = svc.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
		svc.Close()
	}()
	bucket, _ := model.ParseBucketName("logs")
	bop, err := svc.Submit(context.Background(), token(t, "bkt"), model.CreateBucketRequest{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, svc, bop.ID)
	key, _ := model.ParseObjectKey("late")
	put, err := svc.Submit(context.Background(), token(t, "late"), model.PutObjectRequest{
		Path: model.ObjectPath{Bucket: bucket, Key: key}, Size: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, svc, put.ID)
	if got.Phase() != model.PhaseFailed || got.Err().Code != model.CodeDeadlineExceeded {
		t.Fatalf("phase %s %+v", got.Phase(), got.Err())
	}
	if _, err := svc.Object(context.Background(), model.ObjectPath{Bucket: bucket, Key: key}); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("object %v", err)
	}
}

func openRunning(t *testing.T, store storage.StoreConfig, blk *fake.Block, obj *fake.Objects) (*storage.Service, func()) {
	t.Helper()
	svc, err := storage.Open(context.Background(), opts(store, blk, obj))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = svc.Run(ctx)
		close(done)
	}()
	stop := func() {
		cancel()
		<-done
	}
	t.Cleanup(func() {
		stop()
		svc.Close()
	})
	return svc, stop
}

func opts(store storage.StoreConfig, blk *fake.Block, obj *fake.Objects) storage.Options {
	zone, _ := model.ParseZone("az-1")
	return storage.Options{
		Store: store, Block: blk, Object: obj, Signer: obj, Zones: []model.Zone{zone},
		Workers: 2, ScanInterval: 5 * time.Millisecond,
		RetryBase: time.Millisecond, RetryMax: 5 * time.Millisecond, UploadPoll: 5 * time.Millisecond,
	}
}

func volumeReq(t *testing.T) model.CreateVolumeRequest {
	t.Helper()
	zone, err := model.ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	return model.CreateVolumeRequest{Spec: model.VolumeSpec{Size: model.GiB, Zone: zone, Type: model.VolumeTypeSSD, Source: model.NoSource()}}
}

func token(t *testing.T, s string) model.ClientToken {
	t.Helper()
	tok, err := model.ParseClientToken(s)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func waitDone(t *testing.T, svc *storage.Service, id model.OperationID) storage.OperationStatus {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		op, err := svc.Operation(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if op.Terminal() {
			return op
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("operation did not finish")
	return storage.OperationStatus{}
}

func reqTarget(t *testing.T, op storage.OperationStatus) model.VolumeID {
	t.Helper()
	id, ok := op.Target().(model.VolumeID)
	if !ok {
		t.Fatalf("target %T", op.Target())
	}
	return id
}
