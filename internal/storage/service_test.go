package storage_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
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

func TestCreateVolumeFromImageIsIdempotent(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		assertImageVolume(t, storage.Memory())
	})
	t.Run("sqlite", func(t *testing.T) {
		assertImageVolume(t, storage.SQLite(filepath.Join(t.TempDir(), "vws.db")))
	})
}

func assertImageVolume(t *testing.T, store storage.StoreConfig) {
	t.Helper()
	svc, cancel := openRunning(t, store, fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	req := imageVolumeReq(t, "ubuntu-24-04")
	tok := token(t, "image-1")
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
	other := imageVolumeReq(t, "ubuntu-22-04")
	_, err = svc.Submit(context.Background(), tok, other)
	if !errors.Is(err, model.ErrTokenReused) {
		t.Fatalf("token err %v", err)
	}
	_, err = svc.Submit(context.Background(), token(t, "image-2"), other)
	if !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("exists err %v", err)
	}
	got := waitDone(t, svc, first.ID)
	if got.Phase() != model.PhaseSucceeded {
		t.Fatalf("phase %s %+v", got.Phase(), got.Err())
	}
	again, err := svc.Submit(context.Background(), token(t, "image-3"), req)
	if err != nil {
		t.Fatal(err)
	}
	if again.Phase() != model.PhaseSucceeded {
		t.Fatalf("replay phase %s", again.Phase())
	}
	vol, err := svc.Volume(context.Background(), req.ID)
	if err != nil {
		t.Fatal(err)
	}
	img, ok := vol.Spec.Source.Image()
	if !ok || img.String() != "projects/shared/regions/vws-1/images/ubuntu-24-04" {
		t.Fatalf("image %v %v", img, ok)
	}
	if _, ok := vol.AsAvailable(); !ok {
		t.Fatalf("state %v", vol.State)
	}
}

func imageVolumeReq(t *testing.T, imageID string) model.CreateVolumeRequest {
	t.Helper()
	zone, err := model.ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	project, err := model.ParseProject("a123")
	if err != nil {
		t.Fatal(err)
	}
	id, err := model.ParseVolumeID("boot-1")
	if err != nil {
		t.Fatal(err)
	}
	img, err := model.ParseImageName("projects/shared/regions/vws-1/images/" + imageID)
	if err != nil {
		t.Fatal(err)
	}
	src, err := model.FromImage(img)
	if err != nil {
		t.Fatal(err)
	}
	return model.CreateVolumeRequest{
		ID:   id.WithProject(project),
		Spec: model.VolumeSpec{Size: model.GiB, Zone: zone, Type: model.VolumeTypeSSD, Source: src},
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

// errNth cancels its parent on the n-th Err call. Run samples the context
// more than once per loop, and a cancel between those samples used to drop
// ctx.Done from the select while no worker remained.
type errNth struct {
	context.Context
	cancel context.CancelFunc
	n, at  int
}

func (c *errNth) Err() error {
	c.n++
	if c.n == c.at {
		c.cancel()
	}
	return c.Context.Err() //nolint:wrapcheck // the test double must return the context's own error
}

func TestRunReturnsWhenCancelLandsBetweenChecks(t *testing.T) {
	for at := 1; at <= 12; at++ {
		t.Run(strconv.Itoa(at), func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			ctx := &errNth{Context: parent, cancel: cancel, at: at}
			svc, err := storage.Open(context.Background(), opts(storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{})))
			if err != nil {
				t.Fatal(err)
			}
			errc := make(chan error, 1)
			go func() { errc <- svc.Run(ctx) }()
			select {
			case err := <-errc:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				cancel()
				t.Fatalf("Run did not return when Err cancelled on call %d", at)
			}
			if err := svc.Close(); err != nil {
				t.Fatal(err)
			}
		})
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
	page, err := svc.ListBuckets(context.Background(), model.Project{}, model.PageRequest{Size: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.NextToken == "" {
		t.Fatalf("page %+v", page)
	}
	next, err := svc.ListBuckets(context.Background(), model.Project{}, model.PageRequest{Size: 2, Token: page.NextToken})
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

func TestUpdateObjectReplacesGeneration(t *testing.T) {
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
	path := model.ObjectPath{Bucket: bucket, Key: key}
	first, err := svc.Submit(context.Background(), token(t, "put"), model.PutObjectRequest{Path: path, Size: 5})
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.Accept(*first.Upload, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, first.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatal(got.Err())
	}
	head, err := svc.Object(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("world!!")
	sum := sha256.Sum256(body)
	upd, err := svc.Submit(context.Background(), token(t, "upd"), model.UpdateObjectRequest{
		Path: path, Size: model.Bytes(len(body)), SHA256: sum, HasContents: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if upd.Upload == nil {
		t.Fatal("missing upload locator")
	}
	if err := obj.Accept(*upd.Upload, body); err != nil {
		t.Fatal(err)
	}
	if err := obj.Accept(*upd.Upload, body); err == nil {
		t.Fatal("generation was written twice")
	}
	if got := waitDone(t, svc, upd.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("%s %+v", got.Phase(), got.Err())
	}
	up, ok := upd.Work.(model.ObjectUpload)
	if !ok || up.Replaces != head.Generation {
		t.Fatalf("work %+v", upd.Work)
	}
	next, err := svc.Object(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if next.Generation == head.Generation || next.Size != model.Bytes(len(body)) || next.Meta.Version == head.Meta.Version {
		t.Fatalf("head %+v", next)
	}
	_, err = svc.Submit(context.Background(), token(t, "again"), model.PutObjectRequest{Path: path, Size: 1})
	if !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("create %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	old := model.ObjectRef{Path: path, Generation: head.Generation}
	for {
		_, err := obj.Stat(context.Background(), old)
		if errors.Is(err, backend.ErrNotFound) {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("old generation %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestScopedVolumeSettlesOnSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vws.db")
	svc, cancel := openRunning(t, storage.SQLite(path), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	project, err := model.ParseProject("a123")
	if err != nil {
		t.Fatal(err)
	}
	id, err := model.ParseVolumeID("data-1")
	if err != nil {
		t.Fatal(err)
	}
	req := volumeReq(t)
	req.ID = id.WithProject(project)
	op, err := svc.Submit(context.Background(), token(t, "scoped"), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("phase %s %+v", got.Phase(), got.Err())
	}
	vol, err := svc.Volume(context.Background(), req.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := vol.AsAvailable(); !ok || vol.ID.Project() != project {
		t.Fatalf("volume %+v", vol)
	}
}

func TestReclaimDoesNotCoverTheHead(t *testing.T) {
	gate := make(chan struct{})
	obj := fake.NewObjects(fake.ObjectOptions{Now: time.Now, DeleteWait: gate})
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
	path := model.ObjectPath{Bucket: bucket, Key: key}
	first, err := svc.Submit(context.Background(), token(t, "put"), model.PutObjectRequest{Path: path, Size: 5})
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.Accept(*first.Upload, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, first.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatal(got.Err())
	}
	body := []byte("world!!")
	sum := sha256.Sum256(body)
	upd, err := svc.Submit(context.Background(), token(t, "upd"), model.UpdateObjectRequest{
		Path: path, Size: model.Bytes(len(body)), SHA256: sum, HasContents: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.Accept(*upd.Upload, body); err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, upd.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("update %s %+v", got.Phase(), got.Err())
	}
	nextBody := []byte("again!!!")
	nextSum := sha256.Sum256(nextBody)
	upd2, err := svc.Submit(context.Background(), token(t, "upd-2"), model.UpdateObjectRequest{
		Path: path, Size: model.Bytes(len(nextBody)), SHA256: nextSum, HasContents: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if upd2.Upload == nil {
		t.Fatal("reclaim blocked the next overwrite")
	}
	if err := obj.Accept(*upd2.Upload, nextBody); err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, upd2.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("second update %s %+v", got.Phase(), got.Err())
	}
	head, err := svc.Object(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if head.Size != model.Bytes(len(nextBody)) {
		t.Fatalf("size %d", head.Size)
	}
	del, err := svc.Submit(context.Background(), token(t, "del"), model.DeleteObjectRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	purge, ok := del.Work.(model.ObjectPurge)
	if !ok || purge.Reclaim || purge.Object.Generation != head.Generation {
		t.Fatalf("delete %+v head %s", del.Work, head.Generation)
	}
	if del.Terminal() {
		t.Fatal("delete finished while the backend was held")
	}
	close(gate)
	if got := waitDone(t, svc, del.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("delete %s %+v", got.Phase(), got.Err())
	}
	if _, err := svc.Object(context.Background(), path); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("object %v", err)
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

func TestInvalidBackendArgumentFailsWithInvalidArgument(t *testing.T) {
	blk := fake.NewBlock()
	blk.FailNext("CreateVolume", backend.Permanent(backend.ErrInvalid))
	svc, cancel := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	op, err := svc.Submit(context.Background(), token(t, "invalid-1"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, svc, op.ID)
	if got.Phase() != model.PhaseFailed || got.Err().Code != model.CodeInvalidArgument {
		t.Fatalf("phase %s err %+v", got.Phase(), got.Err())
	}
}

func TestVolumeDeleteKeepsItsAvailableSnapshot(t *testing.T) {
	svc, cancel := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	ctx := context.Background()
	volOp, err := svc.Submit(ctx, token(t, "vol"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	vol := waitDone(t, svc, volOp.ID).Target().(model.VolumeID)
	snapOp, err := svc.Submit(ctx, token(t, "snap"), model.CreateSnapshotRequest{Source: vol})
	if err != nil {
		t.Fatal(err)
	}
	snap := waitDone(t, svc, snapOp.ID).Target().(model.SnapshotID)
	delOp, err := svc.Submit(ctx, token(t, "del"), model.DeleteVolumeRequest{Volume: vol})
	if err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, delOp.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("delete phase %s", got.Phase())
	}
	if _, err := svc.Snapshot(ctx, snap); err != nil {
		t.Fatalf("snapshot after volume delete: %v", err)
	}
	src, err := model.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	req := volumeReq(t)
	req.Spec.Source = src
	restoreOp, err := svc.Submit(ctx, token(t, "restore"), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, restoreOp.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("restore phase %s", got.Phase())
	}
}

func TestExpandVolumeGrowsOnceAndRejectsShrink(t *testing.T) {
	blk := fake.NewBlock()
	svc, cancel := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
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
	grow := model.ExpandVolumeRequest{Volume: id, Size: before.Spec.Size + model.GiB}
	first, err := svc.Submit(ctx, token(t, "grow"), grow)
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.Submit(ctx, token(t, "grow"), grow)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != again.ID {
		t.Fatalf("same request_id gave operations %s and %s", first.ID, again.ID)
	}
	if got := waitDone(t, svc, first.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("phase %s err %+v", got.Phase(), got.Err())
	}
	after, err := svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if after.Spec.Size != grow.Size || after.Meta.Version != before.Meta.Version+1 {
		t.Fatalf("size %d version %d", after.Spec.Size, after.Meta.Version)
	}
	expands := 0
	for _, c := range blk.Calls() {
		if c.Method == "Expand" {
			expands++
		}
	}
	if expands != 1 {
		t.Fatalf("backend Expand ran %d times", expands)
	}
	_, err = svc.Submit(ctx, token(t, "shrink"), model.ExpandVolumeRequest{Volume: id, Size: before.Spec.Size})
	if !errors.Is(err, model.ErrInvalidArgument) {
		t.Fatalf("shrink: %v", err)
	}
	same, err := svc.Submit(ctx, token(t, "same"), model.ExpandVolumeRequest{Volume: id, Size: grow.Size})
	if err != nil || same.Phase() != model.PhaseSucceeded {
		t.Fatalf("same size: %v %s", err, same.Phase())
	}
}

func TestExpandVolumeBackendRejectionKeepsSize(t *testing.T) {
	blk := fake.NewBlock()
	blk.FailNext("Expand", backend.Permanent(backend.ErrInvalid))
	svc, cancel := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
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
	op, err := svc.Submit(ctx, token(t, "grow"), model.ExpandVolumeRequest{Volume: id, Size: before.Spec.Size + model.GiB})
	if err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, svc, op.ID)
	if got.Phase() != model.PhaseFailed || got.Err().Code != model.CodeInvalidArgument {
		t.Fatalf("phase %s err %+v", got.Phase(), got.Err())
	}
	after, err := svc.Volume(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if after.Spec.Size != before.Spec.Size || after.Meta.Version != before.Meta.Version {
		t.Fatalf("volume changed: %+v", after)
	}
}

func TestExpandVolumeBlocksDeleteWhileInFlight(t *testing.T) {
	svc, stop := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	volOp, err := svc.Submit(context.Background(), token(t, "vol"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	id := waitDone(t, svc, volOp.ID).Target().(model.VolumeID)
	stop()
	grow, err := svc.Submit(context.Background(), token(t, "grow"), model.ExpandVolumeRequest{Volume: id, Size: 2 * model.GiB})
	if err != nil {
		t.Fatal(err)
	}
	if grow.Terminal() {
		t.Fatal("expansion finished before run")
	}
	_, err = svc.Submit(context.Background(), token(t, "del"), model.DeleteVolumeRequest{Volume: id})
	if !errors.Is(err, model.ErrFailedPrecondition) {
		t.Fatalf("err %v", err)
	}
	svc.Close()
}

func TestBackendObjectNamesAreEmptyWhileCreating(t *testing.T) {
	svc, cancel := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	vid, err := model.ParseVolumeID("data-1")
	if err != nil {
		t.Fatal(err)
	}
	sid, err := model.ParseSnapshotID("snap-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		state model.Lifecycle
		want  string
	}{
		{model.Creating{}, ""},
		{model.Available{}, "fake-data-1"},
		{model.Deleting{}, "fake-data-1"},
	} {
		if got := svc.ClaimName(model.Volume{ID: vid, State: tc.state}); got != tc.want {
			t.Errorf("volume %T: %q want %q", tc.state, got, tc.want)
		}
	}
	if got := svc.SnapshotObjectName(model.Snapshot{ID: sid, State: model.Creating{}}); got != "" {
		t.Errorf("creating snapshot: %q", got)
	}
	if got := svc.SnapshotObjectName(model.Snapshot{ID: sid, State: model.Available{}}); got != "fake-snap-1" {
		t.Errorf("available snapshot: %q", got)
	}
}

func TestPanicInAnOperationIsRetriedNotFatal(t *testing.T) {
	blk := fake.NewBlock()
	var once sync.Once
	blk.AfterCall(func(fake.Call) {
		once.Do(func() { panic("backend bug") })
	})
	svc, cancel := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	op, err := svc.Submit(context.Background(), token(t, "panic-1"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("phase %s err %+v", got.Phase(), got.Err())
	}
}

func TestHealthyNeedsARunningEngine(t *testing.T) {
	svc, err := storage.Open(context.Background(), opts(storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{})))
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	if err := svc.Healthy(context.Background()); err == nil {
		t.Fatal("healthy before Run")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = svc.Run(ctx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for svc.Healthy(context.Background()) != nil {
		if time.Now().After(deadline) {
			t.Fatal("not healthy while running")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if err := svc.Healthy(context.Background()); err == nil {
		t.Fatal("healthy after Run returned")
	}
}
