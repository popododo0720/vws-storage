package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/s3"
	"github.com/popododo0720/vws-storage/internal/model"
)

func open(t *testing.T) *s3.Driver {
	t.Helper()
	srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(srv.Close)
	cfg, err := s3.ProfileConfig(s3.MinIO)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Endpoint, cfg.AccessKey, cfg.SecretKey = srv.URL, "key", "secret"
	d, err := s3.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func ref(t *testing.T, bucket, key string) model.ObjectRef {
	t.Helper()
	b, err := model.ParseBucketName("projects/p1/buckets/" + bucket)
	if err != nil {
		t.Fatal(err)
	}
	k, err := model.ParseObjectKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return model.ObjectRef{Path: model.ObjectPath{Bucket: b, Key: k}, Generation: model.NewGeneration()}
}

func send(t *testing.T, loc model.Locator, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(loc.Method, loc.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range loc.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestBucketLifecycleIsIdempotent(t *testing.T) {
	d := open(t)
	ctx := context.Background()
	r := ref(t, "logs", "a.txt")
	if err := d.CreateBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatal(err)
	}
	if err := d.CreateBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatalf("repeat create: %v", err)
	}
	loc, err := d.PresignPut(ctx, r, 2, model.Digest{}, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if resp := send(t, loc, []byte("hi")); resp.StatusCode != http.StatusOK {
		t.Fatalf("put status %d", resp.StatusCode)
	}
	if err := d.DeleteBucket(ctx, r.Path.Bucket); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("non-empty delete: %v", err)
	}
	if err := d.DeleteObject(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteObject(ctx, r); err != nil {
		t.Fatalf("repeat object delete: %v", err)
	}
	if err := d.DeleteBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatalf("repeat bucket delete: %v", err)
	}
}

func TestStatReportsUploadedBytes(t *testing.T) {
	d := open(t)
	ctx := context.Background()
	r := ref(t, "logs", "dir/a.txt")
	if err := d.CreateBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Stat(ctx, r); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("before upload: %v", err)
	}
	body := []byte("hello world")
	loc, err := d.PresignPut(ctx, r, model.Bytes(len(body)), model.Digest{}, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if resp := send(t, loc, body); resp.StatusCode != http.StatusOK {
		t.Fatalf("put status %d", resp.StatusCode)
	}
	obs, err := d.Stat(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if obs.Size != model.Bytes(len(body)) || obs.MD5 != md5.Sum(body) || obs.UploadedAt.IsZero() || obs.ETag == "" {
		t.Fatalf("observed %+v", obs)
	}
	get, err := d.PresignGet(ctx, r, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	resp := send(t, get, nil)
	got, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("get %q err %v", got, err)
	}
	other := r
	other.Generation = model.NewGeneration()
	if _, err := d.Stat(ctx, other); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("other generation: %v", err)
	}
}

func TestPresignPutBindsLengthChecksumAndExpiry(t *testing.T) {
	d := open(t)
	r := ref(t, "logs", "a.txt")
	sum := sha256.Sum256([]byte("hello"))
	expires := time.Now().Add(90*time.Second + 500*time.Millisecond)
	loc, err := d.PresignPut(context.Background(), r, 5, sum, expires)
	if err != nil {
		t.Fatal(err)
	}
	if loc.Method != http.MethodPut || loc.ExpiresAt.After(expires) {
		t.Fatalf("locator %+v", loc)
	}
	if loc.Headers["X-Amz-Checksum-Sha256"] != base64.StdEncoding.EncodeToString(sum[:]) || loc.Headers["If-None-Match"] != "*" {
		t.Fatalf("headers %v", loc.Headers)
	}
	u, err := url.Parse(loc.URL)
	if err != nil {
		t.Fatal(err)
	}
	signed := u.Query().Get("X-Amz-SignedHeaders")
	for _, h := range []string{"content-length", "x-amz-checksum-sha256", "if-none-match"} {
		if !bytes.Contains([]byte(signed), []byte(h)) {
			t.Fatalf("%s not signed: %s", h, signed)
		}
	}
	secs, err := strconv.Atoi(u.Query().Get("X-Amz-Expires"))
	if err != nil || secs != 90 {
		t.Fatalf("X-Amz-Expires %q err %v", u.Query().Get("X-Amz-Expires"), err)
	}
	_, err = d.PresignPut(context.Background(), r, 5, sum, time.Now())
	if !errors.Is(err, backend.ErrInvalid) || !backend.IsPermanent(err) {
		t.Fatalf("past expiry: %v", err)
	}
}

func TestProfilesAndOpenValidation(t *testing.T) {
	for _, p := range []s3.Profile{s3.RGW, s3.MinIO} {
		cfg, err := s3.ProfileConfig(p)
		if err != nil || !cfg.PathStyle || cfg.Region == "" {
			t.Fatalf("%s: %+v %v", p, cfg, err)
		}
	}
	if _, err := s3.ProfileConfig("ceph"); !errors.Is(err, model.ErrInvalidArgument) {
		t.Fatalf("unknown profile: %v", err)
	}
	if _, err := s3.Open(s3.Config{}); err == nil {
		t.Fatal("empty config opened")
	}
}
