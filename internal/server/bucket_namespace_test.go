package server_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"
)

// A global CreateBucket with a name ending in "-an" is refused with
// INVALID_ARGUMENT / INVALID_BUCKET_NAMESPACE (S3 InvalidBucketNamespace),
// and nothing is stored (#170).
func TestCreateGlobalBucketWithAccountRegionalSuffixIsInvalidBucketNamespace(t *testing.T) {
	_, _, buckets, _, _ := dial(t, "http://127.0.0.1")
	ctx := context.Background()
	_, err := buckets.CreateBucket(ctx, &storagev1.CreateBucketRequest{
		Parent: "projects/local", BucketId: "logs-111122223333-local-an",
		Bucket: &storagev1.Bucket{Spec: &storagev1.BucketSpec{Region: "local"}},
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument || reasonOf(st) != "INVALID_BUCKET_NAMESPACE" {
		t.Fatalf("CreateBucket = %v / %q, want INVALID_ARGUMENT / INVALID_BUCKET_NAMESPACE", st.Code(), reasonOf(st))
	}
	_, err = buckets.GetBucket(ctx, &storagev1.GetBucketRequest{Name: "projects/local/buckets/logs-111122223333-local-an"})
	if st, _ := status.FromError(err); st.Code() != codes.NotFound {
		t.Fatalf("GetBucket after refused create = %v, want NOT_FOUND", st.Code())
	}
}
