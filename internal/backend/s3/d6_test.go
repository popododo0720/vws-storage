package s3_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// rgwEcho behaves like Ceph RGW with a wrong x-amz-checksum-sha256
// (vws-platform C): the PUT is accepted, the value is kept as sent, and HEAD
// and GET report it back without checking it against the bytes.
func rgwEcho(t *testing.T) string {
	t.Helper()
	inner := gofakes3.New(s3mem.New()).Server()
	var mu sync.Mutex
	claims := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			if v := r.Header.Get("x-amz-checksum-sha256"); v != "" {
				mu.Lock()
				claims[r.URL.Path] = v
				mu.Unlock()
			}
		}
		for k := range r.Header {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-checksum-") {
				r.Header.Del(k)
			}
		}
		mu.Lock()
		claim := claims[r.URL.Path]
		mu.Unlock()
		if claim != "" && (r.Method == http.MethodHead || r.Method == http.MethodGet) {
			w.Header().Set("x-amz-checksum-sha256", claim)
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// The endpoint reports the client's wrong SHA-256 back. Stat must return the
// digest of the stored bytes, and the upload check must fail.
func TestStatIgnoresAnEchoedWrongChecksum(t *testing.T) {
	d := openOn(t, rgwEcho(t), "")
	ctx := context.Background()
	r := ref(t, "logs", "a.txt")
	if err := d.CreateBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatal(err)
	}
	body := []byte("hello\n")
	wrong := model.Digest(sha256.Sum256([]byte("not the body")))
	loc, err := d.PresignPut(ctx, r, model.Bytes(len(body)), wrong, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := send(t, loc, body); status != http.StatusOK {
		t.Fatalf("put %d: the fake must accept the wrong checksum like RGW", status)
	}
	obs, err := d.Stat(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if obs.SHA256 != model.Digest(sha256.Sum256(body)) {
		t.Fatalf("Stat SHA-256 = %x, want the digest of the stored bytes", obs.SHA256)
	}
	if err := (model.ObjectUpload{Object: r, Size: obs.Size, SHA256: wrong}).Check(obs); err == nil {
		t.Fatal("an upload with a wrong client SHA-256 passed the check")
	}
}

// End to end through the engine: a presigned upload whose claimed SHA-256 is
// wrong fails, leaves no object record, and its bytes are removed.
func TestEngineRejectsAWrongClientSHA256(t *testing.T) {
	d := openOn(t, rgwEcho(t), "")
	zone, err := model.ParseZone("az-1")
	if err != nil {
		t.Fatal(err)
	}
	svc, err := storage.Open(context.Background(), storage.Options{
		Store: storage.Memory(), Block: fake.NewBlock(), Object: d, Signer: d,
		Zones: []model.Zone{zone}, Workers: 2, ScanInterval: 5 * time.Millisecond,
		RetryBase: time.Millisecond, RetryMax: 5 * time.Millisecond, UploadPoll: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = svc.Run(ctx); close(runDone) }()
	t.Cleanup(func() { cancel(); <-runDone; svc.Close() })

	bucket, err := model.ParseBucketName("projects/p1/buckets/logs")
	if err != nil {
		t.Fatal(err)
	}
	wait := func(id model.OperationID) storage.OperationStatus {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			st, err := svc.Operation(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if st.Terminal() {
				return st
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("operation did not finish")
		return storage.OperationStatus{}
	}
	bop, err := svc.Submit(context.Background(), clientToken(t, "bkt"), model.CreateBucketRequest{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	if got := wait(bop.ID); got.Phase() != model.PhaseSucceeded {
		t.Fatal(got.Err())
	}
	key, err := model.ParseObjectKey("a.txt")
	if err != nil {
		t.Fatal(err)
	}
	path := model.ObjectPath{Bucket: bucket, Key: key}
	body := []byte("hello\n")
	put, err := svc.Submit(context.Background(), clientToken(t, "put"), model.PutObjectRequest{
		Path: path, Size: model.Bytes(len(body)), SHA256: model.Digest(sha256.Sum256([]byte("not the body"))),
	})
	if err != nil {
		t.Fatal(err)
	}
	if put.Upload == nil {
		t.Fatal("no upload locator")
	}
	if status, _ := send(t, *put.Upload, body); status != http.StatusOK {
		t.Fatalf("put %d", status)
	}
	got := wait(put.ID)
	if got.Phase() == model.PhaseSucceeded {
		t.Fatal("an upload with a wrong client SHA-256 succeeded")
	}
	if _, err := svc.Object(context.Background(), path); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("object record after a rejected upload: %v", err)
	}
	ref := model.ObjectRef{Path: path, Generation: got.Work.(model.ObjectUpload).Object.Generation}
	if _, err := d.Stat(context.Background(), ref); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stored bytes after a rejected upload: %v", err)
	}
}

func clientToken(t *testing.T, s string) model.ClientToken {
	t.Helper()
	sum := sha256.Sum256([]byte(s))
	tok, err := model.ParseClientToken(base64.RawURLEncoding.EncodeToString(sum[:16]))
	if err != nil {
		t.Fatal(err)
	}
	return tok
}
