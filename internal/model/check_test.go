package model_test

import (
	"crypto/md5"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
)

func TestUploadCheckFallsBackToASinglePartMD5(t *testing.T) {
	body := []byte("hello\n")
	sha := model.Digest(sha256.Sum256(body))
	md := model.MD5(md5.Sum(body))
	w := model.ObjectUpload{Size: model.Bytes(len(body)), SHA256: sha}
	at := time.Now()
	for name, tc := range map[string]struct {
		obs model.ObservedObject
		ok  bool
	}{
		"sha256 reported and equal":     {model.ObservedObject{Size: 6, ETag: "e", SHA256: sha, UploadedAt: at}, true},
		"sha256 reported and different": {model.ObservedObject{Size: 6, ETag: "e", SHA256: model.Digest{1}, MD5: md, UploadedAt: at}, false},
		// Ceph RGW 19.2: HEAD has no checksum, the ETag is the MD5.
		"no sha256, single-part md5": {model.ObservedObject{Size: 6, ETag: model.ETag(md.String()), MD5: md, UploadedAt: at}, true},
		"no sha256, multipart etag":  {model.ObservedObject{Size: 6, ETag: "b1946ac92492d2347c6235b4d2611184-2", UploadedAt: at}, false},
		"no sha256, size differs":    {model.ObservedObject{Size: 5, ETag: model.ETag(md.String()), MD5: md, UploadedAt: at}, false},
		"no sha256, incomplete":      {model.ObservedObject{Size: 6, MD5: md}, false},
	} {
		if err := w.Check(tc.obs); (err == nil) != tc.ok {
			t.Errorf("%s: err %v", name, err)
		}
	}
	// An upload that asked for no SHA-256 needs none.
	if err := (model.ObjectUpload{Size: 6}).Check(model.ObservedObject{Size: 6, ETag: "x-3", UploadedAt: at}); err != nil {
		t.Fatalf("no sha256 requested: %v", err)
	}
}
