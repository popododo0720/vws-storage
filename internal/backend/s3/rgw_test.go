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

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/s3"
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

// flipGet corrupts the last byte of a GET body: stored bytes that no longer
// match their ETag.
type flipGet struct{ http.ResponseWriter }

func (w flipGet) Write(b []byte) (int, error) {
	c := append([]byte(nil), b...)
	if len(c) > 0 {
		c[len(c)-1] ^= 0xff
	}
	return w.ResponseWriter.Write(c) //nolint:wrapcheck // test double passes the writer's error through
}

// rgwLike serves gofakes3 the way Ceph RGW 19.2.6 behaves: it keeps no
// checksum, so it reports none. corrupt flips GET bodies.
func rgwLike(t *testing.T, corrupt bool) string {
	t.Helper()
	inner := gofakes3.New(s3mem.New()).Server()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k := range r.Header {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-checksum-") {
				r.Header.Del(k)
			}
		}
		var out http.ResponseWriter = noChecksum{w}
		if corrupt && r.Method == http.MethodGet {
			out = flipGet{out}
		}
		inner.ServeHTTP(out, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func putVia(t *testing.T, url string, body []byte) (*s3.Driver, model.ObjectRef) {
	t.Helper()
	d := openOn(t, url, "")
	ctx := context.Background()
	r := ref(t, "logs", "a.txt")
	if err := d.CreateBucket(ctx, r.Path.Bucket); err != nil {
		t.Fatal(err)
	}
	loc, err := d.PresignPut(ctx, r, model.Bytes(len(body)), model.Digest(sha256.Sum256(body)), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := send(t, loc, body); status != http.StatusOK {
		t.Fatalf("put %d", status)
	}
	return d, r
}

// Without a reported checksum, Stat computes the SHA-256 from the stored
// bytes, so the record keeps a digest this process computed.
func TestStatComputesSHA256WhenTheEndpointReportsNone(t *testing.T) {
	body := []byte("hello\n")
	d, r := putVia(t, rgwLike(t, false), body)
	obs, err := d.Stat(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	sha := model.Digest(sha256.Sum256(body))
	if obs.SHA256 != sha || obs.MD5 != model.MD5(md5.Sum(body)) {
		t.Fatalf("observed %+v", obs)
	}
	if err := (model.ObjectUpload{Object: r, Size: obs.Size, SHA256: sha}).Check(obs); err != nil {
		t.Fatalf("check: %v", err)
	}
	// A client that claimed another digest fails the check.
	if err := (model.ObjectUpload{Object: r, Size: obs.Size, SHA256: model.Digest(sha256.Sum256([]byte("x")))}).Check(obs); err == nil {
		t.Fatal("a false client digest passed")
	}
}

func TestStatRejectsBytesThatDoNotMatchTheMD5ETag(t *testing.T) {
	d, r := putVia(t, rgwLike(t, true), []byte("hello\n"))
	_, err := d.Stat(context.Background(), r)
	if err == nil || !backend.IsPermanent(err) {
		t.Fatalf("err = %v, want a permanent md5 mismatch", err)
	}
}
