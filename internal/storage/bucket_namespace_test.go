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

func region(t *testing.T, s string) model.Region {
	t.Helper()
	r, err := model.ParseRegion(s)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// An account-regional create (#170): the owner's suffix is admitted and the
// namespace is stored and survives a reopen; another account's suffix is
// INVALID_BUCKET_NAMESPACE and stores nothing; a resent create with the same
// token replays; the same token with the other namespace is a conflict.
func TestAccountRegionalBucketNamespace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vws.db")
	ctx := context.Background()
	const name = "logs-111122223333-r1-an"
	ar := model.BucketNamespaceAccountRegional
	own := model.CreateBucketRequest{Bucket: bucketIn(t, "a111122223333", name), Region: region(t, "r1"), Namespace: ar}
	other := model.CreateBucketRequest{Bucket: bucketIn(t, "a999999999999", name), Region: region(t, "r1"), Namespace: ar}

	svc, cancel := openRunning(t, storage.SQLite(path), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	_, err := svc.Submit(ctx, token(t, "other-create"), other)
	if !errors.Is(err, model.ErrInvalidBucketNamespace) {
		t.Fatalf("create with another account's suffix: %v, want ErrInvalidBucketNamespace", err)
	}
	if _, err := svc.Bucket(ctx, other.Bucket); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("bucket after refused create: %v, want NOT_FOUND", err)
	}
	ops, err := svc.ListOperations(ctx, storage.OperationQuery{Project: other.Bucket.Project()}, model.PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ops.Items) != 0 {
		t.Fatalf("refused create stored %d operations", len(ops.Items))
	}

	op, err := svc.Submit(ctx, token(t, "own-create"), own)
	if err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("own create: %s %+v", got.Phase(), got.Err())
	}
	again, err := svc.Submit(ctx, token(t, "own-create"), own)
	if err != nil || again.ID != op.ID {
		t.Fatalf("resent create = %v, %v; want the original operation %v", again.ID, err, op.ID)
	}
	asGlobal := own
	asGlobal.Namespace = model.BucketNamespaceGlobal
	if _, err := svc.Submit(ctx, token(t, "own-create"), asGlobal); !errors.Is(err, model.ErrTokenReused) {
		t.Fatalf("same token, global namespace: %v, want ErrTokenReused", err)
	}
	if _, err := svc.Submit(ctx, token(t, "own-create-global"), asGlobal); !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("new token, same name, global namespace: %v, want ErrAlreadyExists", err)
	}
	cancel()
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	svc, cancel = openRunning(t, storage.SQLite(path), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	b, err := svc.Bucket(ctx, own.Bucket)
	if err != nil {
		t.Fatal(err)
	}
	if b.Namespace != model.BucketNamespaceAccountRegional || b.Region != own.Region {
		t.Fatalf("reopened bucket namespace %v region %v, want account-regional r1", b.Namespace, b.Region)
	}
}

// A global create keeps working for ordinary names and refuses a new name
// ending with "-an", in both stores.
func TestGlobalBucketRefusesTheAccountRegionalSuffix(t *testing.T) {
	for name, cfg := range map[string]func(t *testing.T) storage.StoreConfig{
		"memory": func(*testing.T) storage.StoreConfig { return storage.Memory() },
		"sqlite": func(t *testing.T) storage.StoreConfig {
			return storage.SQLite(filepath.Join(t.TempDir(), "vws.db"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			svc, cancel := openRunning(t, cfg(t), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
			defer cancel()
			ctx := context.Background()
			req := model.CreateBucketRequest{
				Bucket: bucketIn(t, "a111122223333", "logs-111122223333-r1-an"), Region: region(t, "r1"),
			}
			if _, err := svc.Submit(ctx, token(t, "g-an"), req); !errors.Is(err, model.ErrInvalidBucketNamespace) {
				t.Fatalf("global create of an -an name: %v, want ErrInvalidBucketNamespace", err)
			}
			if _, err := svc.Bucket(ctx, req.Bucket); !errors.Is(err, model.ErrNotFound) {
				t.Fatalf("bucket after refused create: %v, want NOT_FOUND", err)
			}
			ok := model.CreateBucketRequest{Bucket: bucketIn(t, "a111122223333", "logs"), Region: region(t, "r1")}
			op, err := svc.Submit(ctx, token(t, "g-ok"), ok)
			if err != nil {
				t.Fatal(err)
			}
			if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
				t.Fatalf("global create: %s %+v", got.Phase(), got.Err())
			}
			b, err := svc.Bucket(ctx, ok.Bucket)
			if err != nil || b.Namespace != model.BucketNamespaceGlobal {
				t.Fatalf("global bucket = %+v, %v", b, err)
			}
		})
	}
}
