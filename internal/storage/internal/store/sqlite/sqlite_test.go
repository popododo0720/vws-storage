package sqlite_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/sqlite"
	"github.com/popododo0720/vws-storage/internal/storage/internal/store/storetest"
)

func TestStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		st, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "vws.db"))
		if err != nil {
			t.Fatal(err)
		}
		return st
	})
}

func TestOpenRelativePath(t *testing.T) {
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	const rel = "nested/storage.db"
	st, err := sqlite.Open(context.Background(), rel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, rel)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, err := sqlite.Open(ctx, rel)
	if err == nil {
		second.Close()
		st.Close()
		t.Fatal("second open succeeded")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	again, err := sqlite.Open(context.Background(), rel)
	if err != nil {
		t.Fatal(err)
	}
	again.Close()
}

func TestSecondOpenFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vws.db")
	st, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, err := sqlite.Open(ctx, path)
	if err == nil {
		second.Close()
		t.Fatal("second open succeeded")
	}
}

// A terminal operation row this build cannot decode must not fail a list of
// pending work. The queue sample is such a list and runs once per scan, so one
// such row made it report the same error on every scan (#119).
func TestListOperationsNonTerminalSkipsUndecodableTerminalRow(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vws.db")
	st, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	project, err := model.ParseProject("a123456789012")
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := model.ParseBucketName("projects/a123456789012/buckets/b-1")
	if err != nil {
		t.Fatal(err)
	}
	key, err := model.ParseObjectKey("k/absent.txt")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	meta := model.Meta{Version: 1, CreateTime: now, UpdateTime: now}
	objPath := model.ObjectPath{Bucket: bucket, Key: key}

	// An upload op without a generation cannot decode. No live writer makes
	// one, but a row from another build or a corrupt row can still be stored.
	bad := model.Operation{
		ID:    model.NewOperationID().WithProject(project),
		Work:  model.ObjectUpload{Object: model.ObjectRef{Path: objPath}},
		State: model.OpSucceeded{},
		Meta:  meta,
	}
	pending := model.Operation{
		ID:    model.NewOperationID().WithProject(project),
		Work:  model.ObjectPurge{Object: model.ObjectRef{Path: objPath, Generation: model.NewGeneration()}},
		State: model.OpRunning{},
		Meta:  meta,
	}

	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		op       model.Operation
		terminal int
	}{{bad, 1}, {pending, 0}} {
		payload, err := model.MarshalRecord(row.op)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO records(kind, key, payload) VALUES('operation', ?, ?)`, row.op.ID.String(), payload); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO op_sched(id, terminal, not_before) VALUES(?, ?, 0)`, row.op.ID.String(), row.terminal); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	got, err := st.ListOperations(ctx, store.OperationFilter{NonTerminal: true}, model.OperationID{}, 10)
	if err != nil {
		t.Fatalf("list pending operations: %v", err)
	}
	if len(got) != 1 || got[0].ID.String() != pending.ID.String() {
		t.Fatalf("pending = %v, want only %s", got, pending.ID)
	}

	if _, err := st.ListOperations(ctx, store.OperationFilter{}, model.OperationID{}, 10); err == nil {
		t.Fatal("listing every operation did not report the undecodable row")
	}
}
