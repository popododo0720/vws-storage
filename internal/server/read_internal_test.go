package server

import (
	"crypto/md5" //nolint:gosec // md5 is the S3 ETag
	"crypto/sha256"
	"testing"

	"github.com/popododo0720/vws-storage/internal/model"
)

func TestStoredBytes(t *testing.T) {
	body := []byte("hello\n")
	sha, sum := model.Digest(sha256.Sum256(body)), model.MD5(md5.Sum(body)) //nolint:gosec // md5 is the S3 ETag
	tampered := []byte("hellO\n")
	for _, c := range []struct {
		name string
		obj  model.Object
		body []byte
		want bool
	}{
		{"sha256 record", model.Object{Size: 6, SHA256: sha, MD5: sum}, body, true},
		{"zero sha256 record read by md5", model.Object{Size: 6, MD5: sum}, body, true},
		{"tampered against sha256", model.Object{Size: 6, SHA256: sha, MD5: sum}, tampered, false},
		{"tampered against md5", model.Object{Size: 6, MD5: sum}, tampered, false},
		{"no digest at all", model.Object{Size: 6}, body, false},
		{"size differs", model.Object{Size: 7, SHA256: sha, MD5: sum}, body, false},
	} {
		if got := storedBytes(c.obj, c.body); got != c.want {
			t.Errorf("%s: storedBytes = %v, want %v", c.name, got, c.want)
		}
	}
}
