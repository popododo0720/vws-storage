package s3_test

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/popododo0720/vws-storage/internal/model"
)

// noChecksum drops every x-amz-checksum-* response header, as Ceph RGW 19.2.6
// does on PUT and HEAD (vws-test 17g).
type noChecksum struct{ http.ResponseWriter }

func (w noChecksum) strip() {
	for k := range w.Header() {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-checksum-") {
			w.Header().Del(k)
		}
	}
}

func (w noChecksum) WriteHeader(code int) {
	w.strip()
	w.ResponseWriter.WriteHeader(code)
}

func (w noChecksum) Write(b []byte) (int, error) {
	w.strip()
	return w.ResponseWriter.Write(b) //nolint:wrapcheck // test double passes the writer's error through
}

func TestUploadWithoutReportedChecksumPassesTheCheck(t *testing.T) {
	inner := gofakes3.New(s3mem.New()).Server()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// RGW 19.2.6 keeps no checksum to report, so the fake never sees one.
		for k := range r.Header {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-checksum-") {
				r.Header.Del(k)
			}
		}
		inner.ServeHTTP(noChecksum{w}, r)
	}))
	t.Cleanup(srv.Close)
	d := openOn(t, srv.URL, "")
	ctx := context.Background()
	r := ref(t, "logs", "a.txt")
	if err := d.CreateBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatal(err)
	}
	body := []byte("hello\n")
	sha := model.Digest(sha256.Sum256(body))
	loc, err := d.PresignPut(ctx, r, model.Bytes(len(body)), sha, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := send(t, loc, body); status != http.StatusOK {
		t.Fatalf("put %d", status)
	}
	obs, err := d.Stat(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if !obs.SHA256.IsZero() || obs.MD5 != model.MD5(md5.Sum(body)) {
		t.Fatalf("observed %+v", obs)
	}
	w := model.ObjectUpload{Object: r, Size: model.Bytes(len(body)), SHA256: sha}
	if err := w.Check(obs); err != nil {
		t.Fatalf("check: %v", err)
	}
}
