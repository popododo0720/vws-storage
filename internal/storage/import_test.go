package storage_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

const fixture010 = "../../testdata/upgrade/0.1.0/storage.db"

func configMaps(t *testing.T, ns string) int {
	t.Helper()
	list, err := kc.CoreV1().ConfigMaps(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return len(list.Items)
}

func importInto(t *testing.T, from, path, ns string, mode storage.ImportMode) (storage.ImportReport, error) {
	t.Helper()
	return storage.ImportSQLite(context.Background(), from, storage.ImportTarget{Kubeconfig: path, Namespace: ns}, mode)
}

func sha(t *testing.T, path string) [32]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(b)
}

// The 0.1.0 file imports, verifies, and a second run writes nothing. A dry
// run leaves the API server as it was, and the file is never changed (#141 AC 7).
func TestImport010FixtureIntoKubeStore(t *testing.T) {
	path, ns := kubeEnv(t)
	before := sha(t, fixture010)

	dry, err := importInto(t, fixture010, path, ns, storage.ImportDryRun)
	if err != nil {
		t.Fatal(err)
	}
	if n := configMaps(t, ns); n != 0 {
		t.Fatalf("dry run left %d ConfigMaps, want 0", n)
	}
	if dry.Records != 12 || len(dry.Create) != 12 {
		t.Fatalf("dry run %d records, %d to create, want 12 and 12: %v", dry.Records, len(dry.Create), dry.Create)
	}

	first, err := importInto(t, fixture010, path, ns, storage.ImportWrite)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Create, dry.Create) {
		t.Fatalf("created %v, dry run said %v", first.Create, dry.Create)
	}
	if _, err := importInto(t, fixture010, path, ns, storage.ImportVerify); err != nil {
		t.Fatal(err)
	}
	count := configMaps(t, ns)
	second, err := importInto(t, fixture010, path, ns, storage.ImportWrite)
	if err != nil || len(second.Create) != 0 {
		t.Fatalf("second import created %v, %v; want nothing", second.Create, err)
	}
	if n := configMaps(t, ns); n != count {
		t.Fatalf("second import: %d ConfigMaps, want %d", n, count)
	}
	again, err := importInto(t, fixture010, path, ns, storage.ImportDryRun)
	if err != nil || len(again.Create) != 0 {
		t.Fatalf("dry run after import would create %v, %v", again.Create, err)
	}
	if sha(t, fixture010) != before {
		t.Fatal("the import changed the fixture")
	}
}

// A file written by this build (labels, a snapshot, a bucket and an operation
// left running) imports into the Kubernetes store, every Get and List result
// is the same there, and the running operation finishes once (#141 AC 7).
func TestImportCurrentFileThenResume(t *testing.T) {
	file := filepath.Join(t.TempDir(), "storage.db")
	blk := fake.NewBlock()
	obj := fake.NewObjects(fake.ObjectOptions{})
	svc, stop := openRunning(t, storage.SQLite(file), blk, obj)
	vol := submitDone(t, svc, "vol-a", labeled(t, "data-a", map[string]string{"team": "db"}))
	submitDone(t, svc, "vol-b", labeled(t, "data-b", map[string]string{"team": "web"}))
	submitDone(t, svc, "snap", model.CreateSnapshotRequest{Source: vol.Target().(model.VolumeID), Description: "nightly"})
	bucket, err := model.ParseBucketName("logs")
	if err != nil {
		t.Fatal(err)
	}
	submitDone(t, svc, "bkt", model.CreateBucketRequest{Bucket: bucket})
	stop()
	pending, err := svc.Submit(context.Background(), token(t, "vol-c"), labeled(t, "data-c", nil))
	if err != nil {
		t.Fatal(err)
	}
	want := read(t, svc, model.Project{})
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	path, ns := kubeEnv(t)
	rep, err := importInto(t, file, path, ns, storage.ImportWrite)
	if err != nil {
		t.Fatal(err)
	}
	// 3 volumes, a snapshot, a bucket, 5 operations and their 5 tokens.
	if rep.Records != 15 || len(rep.Create) != 15 {
		t.Fatalf("import %d records, created %d, want 15 and 15", rep.Records, len(rep.Create))
	}
	if _, err := importInto(t, file, path, ns, storage.ImportVerify); err != nil {
		t.Fatal(err)
	}

	next, err := storage.Open(context.Background(), kubeOpts(storage.Kube(path, ns), blk, obj))
	if err != nil {
		t.Fatal(err)
	}
	if got := read(t, next, model.Project{}); !reflect.DeepEqual(got, want) {
		t.Fatalf("imported\n%+v\nwant\n%+v", got, want)
	}
	creates := len(blk.Calls())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = next.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		next.Close()
	})
	if got := waitDone(t, next, pending.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("resumed %s %+v", got.Phase(), got.Err())
	}
	calls := 0
	for _, c := range blk.Calls()[creates:] {
		if c.Method == "CreateVolume" {
			calls++
		}
	}
	if calls != 1 {
		t.Fatalf("CreateVolume calls after the import %d, want 1", calls)
	}
	replay := labeled(t, "data-a", map[string]string{"team": "db"})
	again, err := next.Submit(context.Background(), token(t, "vol-a"), replay)
	if err != nil || again.ID != vol.ID {
		t.Fatalf("replay %+v %v, want %s", again, err, vol.ID)
	}
}

// Before anything runs on the imported store, every Get and List result
// equals the file's.
func TestImportedStoreReadsLikeTheFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "storage.db")
	if err := os.WriteFile(file, mustRead(t, fixture010), 0o600); err != nil {
		t.Fatal(err)
	}
	blk := fake.NewBlock()
	obj := fake.NewObjects(fake.ObjectOptions{})
	src, err := storage.Open(context.Background(), opts(storage.SQLite(file), blk, obj))
	if err != nil {
		t.Fatal(err)
	}
	project, err := model.ParseProject("a123456789012")
	if err != nil {
		t.Fatal(err)
	}
	want := read(t, src, project)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	path, ns := kubeEnv(t)
	if _, err := importInto(t, fixture010, path, ns, storage.ImportWrite); err != nil {
		t.Fatal(err)
	}
	dst, err := storage.Open(context.Background(), kubeOpts(storage.Kube(path, ns), blk, obj))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if got := read(t, dst, project); !reflect.DeepEqual(got, want) {
		t.Fatalf("imported\n%+v\nwant\n%+v", got, want)
	}
}

// A record the store holds otherwise than the file is never overwritten; the
// import and -verify both report it and fail.
func TestImportNeverOverwritesAndVerifyFindsTheDifference(t *testing.T) {
	path, ns := kubeEnv(t)
	if _, err := importInto(t, fixture010, path, ns, storage.ImportWrite); err != nil {
		t.Fatal(err)
	}
	svc, _ := startService(t, kubeOpts(storage.Kube(path, ns), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{})))
	project, err := model.ParseProject("a123456789012")
	if err != nil {
		t.Fatal(err)
	}
	vols, err := svc.ListVolumes(context.Background(), project, model.Zone{}, model.PageRequest{Size: 10})
	if err != nil || len(vols.Items) == 0 {
		t.Fatalf("volumes %v %v", vols, err)
	}
	v := vols.Items[0]
	labels, err := model.ParseLabels(map[string]string{"changed": "after-import"})
	if err != nil {
		t.Fatal(err)
	}
	relabel := model.ModifyVolumeRequest{Volume: v.ID, Zone: v.Spec.Zone, SetLabels: true, Labels: labels}
	submitDone(t, svc, "relabel", relabel)
	changed, err := svc.Volume(context.Background(), v.ID)
	if err != nil {
		t.Fatal(err)
	}

	count := configMaps(t, ns)
	for _, mode := range []storage.ImportMode{storage.ImportVerify, storage.ImportWrite, storage.ImportDryRun} {
		rep, err := importInto(t, fixture010, path, ns, mode)
		if n := configMaps(t, ns); n != count {
			t.Fatalf("mode %d wrote: %d ConfigMaps, want %d", mode, n, count)
		}
		if !errors.Is(err, storage.ErrImportDiffers) {
			t.Fatalf("mode %d: %v, want ErrImportDiffers", mode, err)
		}
		if len(rep.Differences) != 1 || !strings.Contains(rep.Differences[0], v.ID.String()) {
			t.Fatalf("mode %d differences %v, want only %s", mode, rep.Differences, v.ID)
		}
	}
	if got, err := svc.Volume(context.Background(), v.ID); err != nil || !reflect.DeepEqual(got, changed) {
		t.Fatalf("after import %+v %v, want the relabelled volume kept", got, err)
	}
}

// A pending operation of the file never lands beside a record the store
// holds otherwise: Run would act on the store's resource. A conflict writes
// nothing at all.
func TestImportWithAConflictWritesNothing(t *testing.T) {
	ctx := context.Background()
	file := filepath.Join(t.TempDir(), "storage.db")
	src, stopSrc := openRunning(t, storage.SQLite(file), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	made := submitDone(t, src, "src-create", labeled(t, "shared", map[string]string{"owner": "file"}))
	stopSrc()
	del := model.DeleteVolumeRequest{Volume: made.Target().(model.VolumeID)}
	deletion, err := src.Submit(ctx, token(t, "src-delete"), del)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	path, ns := kubeEnv(t)
	blk := fake.NewBlock()
	dst, _ := startService(t, kubeOpts(storage.Kube(path, ns), blk, fake.NewObjects(fake.ObjectOptions{})))
	submitDone(t, dst, "dst-create", labeled(t, "shared", map[string]string{"owner": "store"}))
	count := configMaps(t, ns)
	rep, err := importInto(t, file, path, ns, storage.ImportWrite)
	if !errors.Is(err, storage.ErrImportDiffers) || len(rep.Create) != 0 {
		t.Fatalf("import created %v, %v; want ErrImportDiffers and nothing", rep.Create, err)
	}
	if n := configMaps(t, ns); n != count {
		t.Fatalf("conflicting import: %d ConfigMaps, want %d", n, count)
	}
	if _, err := dst.Operation(ctx, deletion.ID); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("the file's deletion is in the store: %v", err)
	}
	for _, c := range blk.Calls() {
		if c.Method == "DeleteVolume" {
			t.Fatal("the store's volume was deleted")
		}
	}
}

// A record over the store's commit limit is refused before anything is
// written, in every mode, so a dry run does not promise an import that fails.
func TestImportRefusesARecordTooLargeForTheStore(t *testing.T) {
	file := filepath.Join(t.TempDir(), "storage.db")
	blk := fake.NewBlock()
	huge := strings.Repeat("x", 930000)
	blk.FailNext("CreateVolume", backend.Permanent(fmt.Errorf("%w: %s", backend.ErrInvalid, huge)))
	svc, stop := openRunning(t, storage.SQLite(file), blk, fake.NewObjects(fake.ObjectOptions{}))
	op, err := svc.Submit(context.Background(), token(t, "huge"), labeled(t, "huge", nil))
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, svc, op.ID)
	stop()
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	path, ns := kubeEnv(t)
	for _, mode := range []storage.ImportMode{storage.ImportDryRun, storage.ImportWrite, storage.ImportVerify} {
		if _, err := importInto(t, file, path, ns, mode); !errors.Is(err, model.ErrInvalidArgument) {
			t.Fatalf("mode %d: %v, want the record refused", mode, err)
		}
	}
	if n := configMaps(t, ns); n != 0 {
		t.Fatalf("refused import left %d ConfigMaps", n)
	}
}

// A file with object records is refused before the API server is touched:
// the Kubernetes store does not keep objects yet (#141 AC 6).
func TestImportRefusesObjectRecords(t *testing.T) {
	file := filepath.Join(t.TempDir(), "storage.db")
	obj := fake.NewObjects(fake.ObjectOptions{})
	svc, stop := openRunning(t, storage.SQLite(file), fake.NewBlock(), obj)
	bucket, err := model.ParseBucketName("logs")
	if err != nil {
		t.Fatal(err)
	}
	submitDone(t, svc, "bkt", model.CreateBucketRequest{Bucket: bucket})
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
	if err := obj.Accept(*put.Upload, body); err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, put.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("%s %+v", got.Phase(), got.Err())
	}
	stop()
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	path, ns := kubeEnv(t)
	for _, mode := range []storage.ImportMode{storage.ImportWrite, storage.ImportDryRun, storage.ImportVerify} {
		if _, err := importInto(t, file, path, ns, mode); !errors.Is(err, storage.ErrImportObjects) {
			t.Fatalf("mode %d: %v, want ErrImportObjects", mode, err)
		}
	}
	if n := configMaps(t, ns); n != 0 {
		t.Fatalf("refused import left %d ConfigMaps", n)
	}
}

// A record the store removed after an import stays removed: a second import
// reports it and writes nothing, so a later delete is never undone.
func TestImportDoesNotBringBackARemovedRecord(t *testing.T) {
	path, ns := kubeEnv(t)
	if _, err := importInto(t, fixture010, path, ns, storage.ImportWrite); err != nil {
		t.Fatal(err)
	}
	svc, _ := startService(t, kubeOpts(storage.Kube(path, ns), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{})))
	project, err := model.ParseProject("a123456789012")
	if err != nil {
		t.Fatal(err)
	}
	snaps, err := svc.ListSnapshots(context.Background(), project, model.Region{}, model.PageRequest{Size: 10})
	if err != nil || len(snaps.Items) != 1 {
		t.Fatalf("snapshots %v %v", snaps, err)
	}
	snap := snaps.Items[0].ID
	submitDone(t, svc, "del-snap", model.DeleteSnapshotRequest{Snapshot: snap})
	count := configMaps(t, ns)
	for _, mode := range []storage.ImportMode{storage.ImportDryRun, storage.ImportWrite, storage.ImportVerify} {
		rep, err := importInto(t, fixture010, path, ns, mode)
		if !errors.Is(err, storage.ErrImportDiffers) || len(rep.Create) != 0 {
			t.Fatalf("mode %d: created %v, %v; want ErrImportDiffers and nothing", mode, rep.Create, err)
		}
		if len(rep.Differences) != 1 || rep.Differences[0] != "removed snapshot "+snap.String() {
			t.Fatalf("mode %d differences %v", mode, rep.Differences)
		}
	}
	if n := configMaps(t, ns); n != count {
		t.Fatalf("import wrote: %d ConfigMaps, want %d", n, count)
	}
	if _, err := svc.Snapshot(context.Background(), snap); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("snapshot after import: %v, want not found", err)
	}
}

// The guard outlives the tombstone: past the TTL, a record the store
// removed after a finished import is still a difference, never re-created.
func TestImportDoesNotBringBackARecordPastItsTombstone(t *testing.T) {
	ctx := context.Background()
	path, ns := kubeEnv(t)
	if _, err := importInto(t, fixture010, path, ns, storage.ImportWrite); err != nil {
		t.Fatal(err)
	}
	svc, stop := startService(t, kubeOpts(storage.Kube(path, ns), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{})))
	project, err := model.ParseProject("a123456789012")
	if err != nil {
		t.Fatal(err)
	}
	snaps, err := svc.ListSnapshots(ctx, project, model.Region{}, model.PageRequest{Size: 10})
	if err != nil || len(snaps.Items) != 1 {
		t.Fatalf("snapshots %v %v", snaps, err)
	}
	snap := snaps.Items[0].ID
	submitDone(t, svc, "del-snap", model.DeleteSnapshotRequest{Snapshot: snap})
	stop()
	withTombstone := configMaps(t, ns)
	if err := storage.PurgeTombstonesAsOf(ctx, path, ns, 48*time.Hour); err != nil {
		t.Fatal(err)
	}
	count := configMaps(t, ns)
	if count != withTombstone-1 {
		t.Fatalf("purge: %d ConfigMaps, want %d", count, withTombstone-1)
	}
	for _, mode := range []storage.ImportMode{storage.ImportDryRun, storage.ImportWrite} {
		rep, err := importInto(t, fixture010, path, ns, mode)
		if !errors.Is(err, storage.ErrImportDiffers) || len(rep.Create) != 0 {
			t.Fatalf("mode %d: created %v, %v; want ErrImportDiffers and nothing", mode, rep.Create, err)
		}
		if len(rep.Differences) != 1 || rep.Differences[0] != "removed snapshot "+snap.String() {
			t.Fatalf("mode %d differences %v", mode, rep.Differences)
		}
	}
	if n := configMaps(t, ns); n != count {
		t.Fatalf("import wrote: %d ConfigMaps, want %d", n, count)
	}
}

// An import cut off before it finished is resumed by running it again: the
// finished mark is written only after the last record.
func TestImportCutOffIsResumed(t *testing.T) {
	ctx := context.Background()
	path, ns := kubeEnv(t)
	first, err := importInto(t, fixture010, path, ns, storage.ImportWrite)
	if err != nil {
		t.Fatal(err)
	}
	// Leave the store as a run cut off after its first records: no mark,
	// and the last records never written.
	cms := kc.CoreV1().ConfigMaps(ns)
	list, err := cms.List(ctx, metav1.ListOptions{LabelSelector: "vws.io/record=token"})
	if err != nil || len(list.Items) == 0 {
		t.Fatalf("tokens %v %v", list, err)
	}
	for _, name := range append([]string{"vws-storage-import"}, list.Items[0].Name, list.Items[1].Name) {
		if err := cms.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	again, err := importInto(t, fixture010, path, ns, storage.ImportWrite)
	if err != nil || len(again.Create) != 2 {
		t.Fatalf("resumed import created %v, %v; want the 2 missing tokens", again.Create, err)
	}
	if _, err := importInto(t, fixture010, path, ns, storage.ImportVerify); err != nil {
		t.Fatal(err)
	}
	if len(first.Create) != 12 {
		t.Fatalf("first import created %d, want 12", len(first.Create))
	}
}

// An upload admitted but not finished has no object row yet. Its operation
// would read object rows when it resumes, so the file is refused too.
func TestImportRefusesAnUnfinishedUpload(t *testing.T) {
	file := filepath.Join(t.TempDir(), "storage.db")
	svc, stop := openRunning(t, storage.SQLite(file), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	bucket, err := model.ParseBucketName("logs")
	if err != nil {
		t.Fatal(err)
	}
	submitDone(t, svc, "bkt", model.CreateBucketRequest{Bucket: bucket})
	key, err := model.ParseObjectKey("day.bin")
	if err != nil {
		t.Fatal(err)
	}
	put := model.PutObjectRequest{Path: model.ObjectPath{Bucket: bucket, Key: key}, Size: 5}
	if _, err := svc.Submit(context.Background(), token(t, "put"), put); err != nil {
		t.Fatal(err)
	}
	stop()
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	path, ns := kubeEnv(t)
	if _, err := importInto(t, file, path, ns, storage.ImportWrite); !errors.Is(err, storage.ErrImportObjects) {
		t.Fatalf("import %v, want ErrImportObjects", err)
	}
	if n := configMaps(t, ns); n != 0 {
		t.Fatalf("refused import left %d ConfigMaps", n)
	}
}

// Service.Import creates an absent record as it is, skips an equal one, and
// refuses a different one without writing.
func TestServiceImportWritesOnlyAbsentKeys(t *testing.T) {
	ctx := context.Background()
	src, _ := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	made := submitDone(t, src, "vol", labeled(t, "data", map[string]string{"team": "db"}))
	vol, err := src.Volume(ctx, made.Target().(model.VolumeID))
	if err != nil {
		t.Fatal(err)
	}
	dst, _ := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	if created, err := dst.Import(ctx, vol); err != nil || !created {
		t.Fatalf("first import created %v, %v", created, err)
	}
	if got, err := dst.Volume(ctx, vol.ID); err != nil || !reflect.DeepEqual(got, vol) {
		t.Fatalf("imported %+v %v, want %+v", got, err, vol)
	}
	if created, err := dst.Import(ctx, vol); err != nil || created {
		t.Fatalf("second import created %v, %v; want nothing", created, err)
	}
	other := vol
	other.Meta.Version++
	if _, err := dst.Import(ctx, other); !errors.Is(err, storage.ErrImportDiffers) {
		t.Fatalf("different record: %v, want ErrImportDiffers", err)
	}
	if got, err := dst.Volume(ctx, vol.ID); err != nil || !reflect.DeepEqual(got, vol) {
		t.Fatalf("after refused import %+v %v, want it unchanged", got, err)
	}
	if _, err := dst.Import(ctx, nil); !errors.Is(err, model.ErrInvalidArgument) {
		t.Fatalf("nil record: %v", err)
	}
}

// A file with a journal beside it is still held, or was not closed cleanly.
func TestImportRefusesAFileWithAJournal(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "storage.db")
	if err := os.WriteFile(file, mustRead(t, fixture010), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file+"-journal", []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := storage.ImportTarget{Namespace: "unused"}
	_, err := storage.ImportSQLite(context.Background(), file, target, storage.ImportDryRun)
	if err == nil || !strings.Contains(err.Error(), "-journal exists") {
		t.Fatalf("import %v, want the journal refused", err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
