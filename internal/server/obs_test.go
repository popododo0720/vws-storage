package server_test

import (
	"context"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/popododo0720/vws-lib/obs/report"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"
)

// countReports installs a process-wide report sink; these tests do not run in parallel.
func countReports(t *testing.T) *atomic.Int64 {
	t.Helper()
	n := new(atomic.Int64)
	report.Configure(report.Sink{CountError: func(context.Context, string) { n.Add(1) }})
	t.Cleanup(func() { report.Configure(report.Sink{}) })
	return n
}

// Through the vws-lib grpcobs interceptors the client still gets the ErrorInfo
// reason, and a caller's mistake is not reported as a server fault.
func TestGRPCObsKeepsReasonAndDoesNotReportCallerErrors(t *testing.T) {
	reports := countReports(t)
	vols, _, buckets, _, _ := dial(t, "")
	_, err := vols.GetVolume(context.Background(), &storagev1.GetVolumeRequest{Name: "projects/local/zones/az-1/volumes/missing"})
	st := status.Convert(err)
	if st.Code() != codes.NotFound || reasonOf(st) != "RESOURCE_NOT_FOUND" {
		t.Fatalf("code %v reason %q err %v", st.Code(), reasonOf(st), err)
	}
	_, err = buckets.GetBucket(context.Background(), &storagev1.GetBucketRequest{Name: "not a name"})
	st = status.Convert(err)
	if st.Code() != codes.InvalidArgument || reasonOf(st) == "" {
		t.Fatalf("code %v reason %q err %v", st.Code(), reasonOf(st), err)
	}
	if n := reports.Load(); n != 0 {
		t.Fatalf("caller errors were reported %d times", n)
	}
}
