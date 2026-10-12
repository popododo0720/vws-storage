package s3_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/s3"
	"github.com/popododo0720/vws-storage/internal/model"
)

func openOn(t *testing.T, url, prefix string) *s3.Driver {
	t.Helper()
	cfg, err := s3.ProfileConfig(s3.RGW)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.BucketPrefix = url, "key", "secret", prefix
	d, err := s3.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestBucketPrefixIsOnlyOnTheS3Name(t *testing.T) {
	mem := s3mem.New()
	srv := httptest.NewServer(gofakes3.New(mem).Server())
	t.Cleanup(srv.Close)
	ctx := context.Background()
	acme, plain := openOn(t, srv.URL, "acme"), openOn(t, srv.URL, "")
	r := ref(t, "logs", "a.txt")

	if err := acme.CreateBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatal(err)
	}
	buckets, err := mem.ListBuckets()
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 1 || !strings.HasPrefix(buckets[0].Name, "acme-vws-") || len(buckets[0].Name) != len("acme-")+44 {
		t.Fatalf("S3 buckets %+v", buckets)
	}

	// Presign, upload and Stat with the prefixed driver use the prefixed bucket.
	body := []byte("hello")
	put, err := acme.PresignPut(ctx, r, model.Bytes(len(body)), model.Digest{}, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(put.URL, "/"+buckets[0].Name+"/") {
		t.Fatalf("put url %s", put.URL)
	}
	if status, _ := send(t, put, body); status != http.StatusOK {
		t.Fatalf("put status %d", status)
	}
	if _, err := acme.Stat(ctx, r); err != nil {
		t.Fatalf("stat: %v", err)
	}
	get, err := acme.PresignGet(ctx, r, time.Now().Add(time.Minute))
	if err != nil || !strings.Contains(get.URL, "/"+buckets[0].Name+"/") {
		t.Fatalf("get url %v %v", get.URL, err)
	}

	// The same VWS name without the prefix is a different S3 bucket.
	if _, err := plain.Stat(ctx, r); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("unprefixed stat: %v", err)
	}

	if err := acme.DeleteObject(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := acme.DeleteBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatal(err)
	}
	if buckets, _ := mem.ListBuckets(); len(buckets) != 0 {
		t.Fatalf("left %+v", buckets)
	}
}

func TestInvalidBucketPrefixFailsOpen(t *testing.T) {
	for _, p := range []string{
		strings.Repeat("a", 19), // "<19>-vws-<40>" is 64 characters
		"Acme", "acme_1", "-acme", "acme.x", "xn--acme", "sthree-x", "ac me",
	} {
		cfg, _ := s3.ProfileConfig(s3.RGW)
		cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.BucketPrefix = "http://127.0.0.1:1", "key", "secret", p
		if _, err := s3.Open(cfg); !errors.Is(err, model.ErrInvalidArgument) {
			t.Errorf("prefix %q: err %v", p, err)
		}
	}
	for _, p := range []string{"", "a", "acme", "acme-", "vws-test-01", strings.Repeat("a", 18)} {
		cfg, _ := s3.ProfileConfig(s3.RGW)
		cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.BucketPrefix = "http://127.0.0.1:1", "key", "secret", p
		if _, err := s3.Open(cfg); err != nil {
			t.Errorf("prefix %q: %v", p, err)
		}
	}
}

// Keys come from the caller and must never appear in an error, including a
// transport failure against a prefixed bucket.
func TestErrorsDoNotCarryKeys(t *testing.T) {
	const ak, sk = "AKIAKEYMUSTNOTLEAK", "secret-must-not-leak"
	cfg, _ := s3.ProfileConfig(s3.RGW)
	cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.BucketPrefix = "http://127.0.0.1:1", ak, sk, "Bad_Prefix"
	_, err := s3.Open(cfg)
	if err == nil || strings.Contains(err.Error(), ak) || strings.Contains(err.Error(), sk) {
		t.Fatalf("open err %v", err)
	}
	srv := httptest.NewServer(http.NotFoundHandler())
	cfg.Endpoint, cfg.BucketPrefix = srv.URL, "acme"
	d, err := s3.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := ref(t, "logs", "a.txt")
	errs := []error{d.CreateBucket(ctx, r.Path.Bucket), d.DeleteBucket(ctx, r.Path.Bucket), d.DeleteObject(ctx, r)}
	_, statErr := d.Stat(ctx, r)
	errs = append(errs, statErr)
	for _, e := range errs {
		if e == nil || strings.Contains(e.Error(), ak) || strings.Contains(e.Error(), sk) {
			t.Fatalf("err %v", e)
		}
	}
}
