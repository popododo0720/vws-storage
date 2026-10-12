package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/s3"
	"github.com/popododo0720/vws-storage/internal/model"
)

// TestLiveEndpoint runs against a real RGW or MinIO. Set VWS_S3_TEST_ENDPOINT,
// VWS_S3_TEST_PROFILE (rgw or minio), VWS_S3_ACCESS_KEY, and VWS_S3_SECRET_KEY.
func TestLiveEndpoint(t *testing.T) {
	endpoint := os.Getenv("VWS_S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("VWS_S3_TEST_ENDPOINT unset")
	}
	cfg, err := s3.ProfileConfig(s3.Profile(os.Getenv("VWS_S3_TEST_PROFILE")))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Endpoint, cfg.AccessKey, cfg.SecretKey = endpoint, os.Getenv("VWS_S3_ACCESS_KEY"), os.Getenv("VWS_S3_SECRET_KEY")
	d, err := s3.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r := ref(t, "live"+model.NewGeneration().String()[:8], "dir/a.txt")
	if err := d.CreateBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatal(err)
	}
	if err := d.CreateBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatalf("repeat create: %v", err)
	}
	body := []byte("hello live")
	sum := sha256.Sum256(body)
	exp := time.Now().Add(time.Minute)
	loc, err := d.PresignPut(ctx, r, model.Bytes(len(body)), sum, exp)
	if err != nil {
		t.Fatal(err)
	}
	if resp := send(t, loc, []byte("short")); resp.StatusCode < 400 {
		t.Fatalf("wrong length accepted: %d", resp.StatusCode)
	}
	bad := bytes.Repeat([]byte("x"), len(body))
	if resp := send(t, loc, bad); resp.StatusCode < 400 {
		t.Fatalf("wrong checksum accepted: %d", resp.StatusCode)
	}
	if resp := send(t, loc, body); resp.StatusCode != http.StatusOK {
		t.Fatalf("put status %d", resp.StatusCode)
	}
	obs, err := d.Stat(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if obs.Size != model.Bytes(len(body)) || obs.SHA256 != sum || obs.MD5 != md5.Sum(body) {
		t.Fatalf("observed %+v", obs)
	}
	again, err := d.PresignPut(ctx, r, model.Bytes(len(body)), sum, exp)
	if err != nil {
		t.Fatal(err)
	}
	if resp := send(t, again, body); resp.StatusCode < 400 {
		t.Fatalf("rewrite of one generation accepted: %d", resp.StatusCode)
	}
	if err := d.DeleteBucket(ctx, r.Path.Bucket); !errors.Is(err, backend.ErrBusy) {
		t.Fatalf("non-empty delete: %v", err)
	}
	if err := d.DeleteObject(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatal(err)
	}
}
