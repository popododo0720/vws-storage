package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// CreateObject is a strict create (S3 If-None-Match: *, #64): identical bytes
// from another request are ALREADY_EXISTS, both while the first create is in
// flight and after it committed. Only the same request_id replays.
func TestCreateObjectIsStrictEvenForIdenticalBytes(t *testing.T) {
	obj := fake.NewObjects(fake.ObjectOptions{Now: time.Now})
	svc, cancel := openRunning(t, storage.Memory(), fake.NewBlock(), obj)
	defer cancel()
	ctx := context.Background()
	bucket, err := model.ParseBucketName("logs")
	if err != nil {
		t.Fatal(err)
	}
	bop, err := svc.Submit(ctx, token(t, "bkt"), model.CreateBucketRequest{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, svc, bop.ID)
	key, err := model.ParseObjectKey("once.bin")
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("write once")
	req := model.PutObjectRequest{Path: model.ObjectPath{Bucket: bucket, Key: key}, Size: model.Bytes(len(body))}

	first, err := svc.Submit(ctx, token(t, "put-a"), req)
	if err != nil {
		t.Fatal(err)
	}
	// A second writer with the same bytes while the first is in flight.
	if _, err := svc.Submit(ctx, token(t, "put-b"), req); !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("concurrent identical create: %v, want ALREADY_EXISTS", err)
	}
	// The same request_id is a replay of the same operation.
	again, err := svc.Submit(ctx, token(t, "put-a"), req)
	if err != nil || again.ID != first.ID {
		t.Fatalf("replay: %v %v", again.ID, err)
	}
	if err := obj.Accept(*first.Upload, body); err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, svc, first.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatalf("%s %+v", got.Phase(), got.Err())
	}
	// After the commit, identical bytes from another request still fail.
	if _, err := svc.Submit(ctx, token(t, "put-c"), req); !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("identical create of a live object: %v, want ALREADY_EXISTS", err)
	}
	if again, err := svc.Submit(ctx, token(t, "put-a"), req); err != nil || again.ID != first.ID {
		t.Fatalf("replay after commit: %v %v", again.ID, err)
	}
}
