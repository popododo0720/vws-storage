package storage_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// DeleteObject with allow_missing on an absent object records a finished
// purge with no generation. On SQLite that row must decode: every operation
// list (the queue sample each scan, ListOperations) reads all of them, and
// one undecodable row failed them all with "invalid argument: generation".
func TestAllowMissingDeleteObjectLeavesOperationsReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vws.db")
	svc, cancel := openRunning(t, storage.SQLite(path), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	ctx := context.Background()
	bucket, err := model.ParseBucketName("projects/a123456789012/buckets/b-1")
	if err != nil {
		t.Fatal(err)
	}
	key, err := model.ParseObjectKey("k/absent.txt")
	if err != nil {
		t.Fatal(err)
	}
	op, err := svc.Submit(ctx, token(t, "del-missing"), model.DeleteObjectRequest{Path: model.ObjectPath{Bucket: bucket, Key: key}, AllowMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.Operation(ctx, op.ID)
	if err != nil {
		t.Fatalf("read the purge operation: %v", err)
	}
	if got.Phase() != model.PhaseSucceeded {
		t.Fatalf("phase %s", got.Phase())
	}
	if _, err := svc.ListOperations(ctx, storage.OperationQuery{NonTerminal: true}, model.PageRequest{}); err != nil {
		t.Fatalf("list operations: %v", err)
	}
}
