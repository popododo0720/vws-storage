package storage_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// S3 bucket names are one namespace shared by all accounts (#122). A name
// another project holds is ALREADY_EXISTS in every state, the failed create
// stores nothing, and the name is free again once the bucket is deleted.
func TestBucketNameIsGlobalAcrossProjects(t *testing.T) {
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
			a := bucketIn(t, "a111111111111", "logs")
			b := bucketIn(t, "a222222222222", "logs")

			op, err := svc.Submit(ctx, token(t, "a-create"), model.CreateBucketRequest{Bucket: a})
			if err != nil {
				t.Fatal(err)
			}
			if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
				t.Fatalf("create a: %s %+v", got.Phase(), got.Err())
			}

			// Another project: ALREADY_EXISTS, and nothing stored.
			_, err = svc.Submit(ctx, token(t, "b-create"), model.CreateBucketRequest{Bucket: b})
			if !errors.Is(err, model.ErrAlreadyExists) {
				t.Fatalf("other project create: %v, want ALREADY_EXISTS", err)
			}
			if _, err := svc.Bucket(ctx, b); !errors.Is(err, model.ErrNotFound) {
				t.Fatalf("other project bucket after refused create: %v, want NOT_FOUND", err)
			}
			ops, err := svc.ListOperations(ctx, storage.OperationQuery{Project: b.Project()}, model.PageRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if len(ops.Items) != 0 {
				t.Fatalf("refused create stored %d operations", len(ops.Items))
			}

			// The owner creating its own name again keeps the Create replay rules.
			again, err := svc.Submit(ctx, token(t, "a-create-2"), model.CreateBucketRequest{Bucket: a})
			if err != nil {
				t.Fatalf("owner re-create: %v", err)
			}
			if got := waitDone(t, svc, again.ID); got.Phase() != model.PhaseSucceeded {
				t.Fatalf("owner re-create: %s %+v", got.Phase(), got.Err())
			}

			// Fully deleted: the name is free for any project.
			del, err := svc.Submit(ctx, token(t, "a-delete"), model.DeleteBucketRequest{Bucket: a})
			if err != nil {
				t.Fatal(err)
			}
			if got := waitDone(t, svc, del.ID); got.Phase() != model.PhaseSucceeded {
				t.Fatalf("delete a: %s %+v", got.Phase(), got.Err())
			}
			op, err = svc.Submit(ctx, token(t, "b-create-2"), model.CreateBucketRequest{Bucket: b})
			if err != nil {
				t.Fatalf("create after delete: %v", err)
			}
			if got := waitDone(t, svc, op.ID); got.Phase() != model.PhaseSucceeded {
				t.Fatalf("create b: %s %+v", got.Phase(), got.Err())
			}
		})
	}
}

// A name is held while its bucket is still creating and while it is deleting.
func TestBucketNameIsHeldWhileCreatingAndDeleting(t *testing.T) {
	svc, stop := openRunning(t, storage.Memory(), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	ctx := context.Background()
	a := bucketIn(t, "a111111111111", "logs")
	b := bucketIn(t, "a222222222222", "logs")
	op, err := svc.Submit(ctx, token(t, "a-create"), model.CreateBucketRequest{Bucket: a})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, svc, op.ID)
	stop() // no workers: the next operation stays in flight.

	if _, err := svc.Submit(ctx, token(t, "a-delete"), model.DeleteBucketRequest{Bucket: a}); err != nil {
		t.Fatal(err)
	}
	_, err = svc.Submit(ctx, token(t, "b-create"), model.CreateBucketRequest{Bucket: b})
	if !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("create while deleting: %v, want ALREADY_EXISTS", err)
	}

	c := bucketIn(t, "a111111111111", "audit")
	d := bucketIn(t, "a222222222222", "audit")
	if _, err := svc.Submit(ctx, token(t, "c-create"), model.CreateBucketRequest{Bucket: c}); err != nil {
		t.Fatal(err)
	}
	_, err = svc.Submit(ctx, token(t, "d-create"), model.CreateBucketRequest{Bucket: d})
	if !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("create while creating: %v, want ALREADY_EXISTS", err)
	}
}

// Projects racing for one name: exactly one wins.
func TestConcurrentCreateBucketAcrossProjectsHasOneWinner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vws.db")
	svc, cancel := openRunning(t, storage.SQLite(path), fake.NewBlock(), fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	ctx := context.Background()
	const n = 16
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		won     []model.OperationID
		refused int
		other   []error
	)
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bkt := bucketIn(t, fmt.Sprintf("a%012d", i+1), "race")
			<-start
			op, err := svc.Submit(ctx, token(t, fmt.Sprintf("race-%d", i)), model.CreateBucketRequest{Bucket: bkt})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won = append(won, op.ID)
			case errors.Is(err, model.ErrAlreadyExists):
				refused++
			default:
				other = append(other, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(other) != 0 || len(won) != 1 || refused != n-1 {
		t.Fatalf("won %d refused %d other %v", len(won), refused, other)
	}
	if got := waitDone(t, svc, won[0]); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("winner: %s %+v", got.Phase(), got.Err())
	}
}

func bucketIn(t *testing.T, project, name string) model.BucketName {
	t.Helper()
	b, err := model.ParseBucketName("projects/" + project + "/buckets/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
