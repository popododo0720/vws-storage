package fake

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/popododo0720/vws-storage/internal/model"
)

func TestGenerationIsWrittenOnce(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	obj := NewObjects(ObjectOptions{Now: func() time.Time { return now }})
	bucket, err := model.ParseBucketName("logs")
	if err != nil {
		t.Fatal(err)
	}
	key, err := model.ParseObjectKey("a")
	if err != nil {
		t.Fatal(err)
	}
	ref := model.ObjectRef{Path: model.ObjectPath{Bucket: bucket, Key: key}, Generation: model.NewGeneration()}
	loc, err := obj.PresignPut(context.Background(), ref, 5, model.Digest{}, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(loc.URL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("exp", strconv.FormatInt(now.Add(time.Hour).UnixMicro(), 10))
	u.RawQuery = q.Encode()
	if err := obj.Accept(model.Locator{URL: u.String()}, []byte("hello")); err == nil {
		t.Fatal("extended expiry was accepted")
	}
	if err := obj.Accept(loc, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := obj.Accept(loc, []byte("hello")); err == nil {
		t.Fatal("generation was rewritten")
	}
}
