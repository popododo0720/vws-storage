package server_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"
)

// A dotted S3 bucket name round-trips through the gRPC API (#160): create,
// get, list, an object inside it, then delete.
func TestDottedBucketRoundTrip(t *testing.T) {
	_, _, buckets, objects, ops := dial(t, "http://127.0.0.1")
	ctx := context.Background()
	const name = "projects/local/buckets/example.com"

	bop, err := buckets.CreateBucket(ctx, &storagev1.CreateBucketRequest{
		Parent: "projects/local", BucketId: "example.com",
		Bucket: &storagev1.Bucket{Spec: &storagev1.BucketSpec{Region: "local"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, bop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}

	got, err := buckets.GetBucket(ctx, &storagev1.GetBucketRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetName() != name {
		t.Fatalf("GetBucket name %q, want %q", got.GetName(), name)
	}
	list, err := buckets.ListBuckets(ctx, &storagev1.ListBucketsRequest{Parent: "projects/local"})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetBuckets()) != 1 || list.GetBuckets()[0].GetName() != name {
		t.Fatalf("ListBuckets = %v", list.GetBuckets())
	}

	oop, err := objects.CreateObject(ctx, &storagev1.CreateObjectRequest{
		Parent: name, ObjectId: "index.html",
		Object:   &storagev1.Object{Spec: &storagev1.ObjectSpec{ContentType: "text/html"}},
		Contents: []byte("<p>hi</p>"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, oop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	obj, err := objects.GetObject(ctx, &storagev1.GetObjectRequest{Name: name + "/objects/index.html"})
	if err != nil {
		t.Fatal(err)
	}
	if obj.GetName() != name+"/objects/index.html" {
		t.Fatalf("GetObject name %q", obj.GetName())
	}
	dop, err := objects.DeleteObject(ctx, &storagev1.DeleteObjectRequest{Name: obj.GetName()})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, dop.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}

	del, err := buckets.DeleteBucket(ctx, &storagev1.DeleteBucketRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if done := waitOp(t, ops, del.GetName()); done.GetError() != nil {
		t.Fatal(done.GetError())
	}
	_, err = buckets.GetBucket(ctx, &storagev1.GetBucketRequest{Name: name})
	if st, _ := status.FromError(err); st.Code() != codes.NotFound {
		t.Fatalf("GetBucket after delete = %v, want NotFound", err)
	}
}

// Bad dotted names are refused at the API with INVALID_ARGUMENT and store nothing.
func TestCreateBucketRefusesBadDottedNames(t *testing.T) {
	_, _, buckets, _, _ := dial(t, "")
	ctx := context.Background()
	for _, id := range []string{".example.com", "example.com.", "example..com", "192.168.5.4"} {
		_, err := buckets.CreateBucket(ctx, &storagev1.CreateBucketRequest{
			Parent: "projects/local", BucketId: id,
			Bucket: &storagev1.Bucket{Spec: &storagev1.BucketSpec{Region: "local"}},
		})
		if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
			t.Errorf("CreateBucket(%q) = %v, want InvalidArgument", id, err)
		}
	}
	list, err := buckets.ListBuckets(ctx, &storagev1.ListBucketsRequest{Parent: "projects/local"})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(list.GetBuckets()); n != 0 {
		t.Fatalf("refused creates stored %d buckets", n)
	}
}
