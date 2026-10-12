package server

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/popododo0720/vws-lib/obs"
	"github.com/popododo0720/vws-lib/obs/report"

	"github.com/popododo0720/vws-storage/internal/model"
)

// countReports installs a report sink that counts errors and panics. The sink is
// process-wide, so these tests do not run in parallel.
func countReports(t *testing.T) (errs, panics *atomic.Int64) {
	t.Helper()
	errs, panics = new(atomic.Int64), new(atomic.Int64)
	report.Configure(report.Sink{
		CountError: func(context.Context, string) { errs.Add(1) },
		CountPanic: func(context.Context) { panics.Add(1) },
	})
	t.Cleanup(func() { report.Configure(report.Sink{}) })
	return errs, panics
}

var unaryInfo = &grpc.UnaryServerInfo{FullMethod: "/x/Y"}

func TestRecoveredPanicIsInternalAndLeaksNothing(t *testing.T) {
	_, panics := countReports(t)
	const secret = "dsn=file:/var/lib/secret.db"
	_, err := unaryObs(context.Background(), nil, unaryInfo, func(context.Context, any) (any, error) { panic(secret) })
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Internal || strings.Contains(st.Message(), "secret") {
		t.Fatalf("unary err = %v", err)
	}
	err = streamObs(nil, &ctxStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/x/Z"},
		func(any, grpc.ServerStream) error { panic(secret) })
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Internal || strings.Contains(st.Message(), "secret") {
		t.Fatalf("stream err = %v", err)
	}
	if n := panics.Load(); n != 2 {
		t.Fatalf("panics reported = %d, want 2", n)
	}
}

// The interceptor must hand back toStatus's error as is: the ErrorInfo reason is
// part of the API, and a caller's mistake is not a server fault.
func TestInterceptorKeepsStatusDetailsAndReportsOnlyServerFaults(t *testing.T) {
	errs, _ := countReports(t)
	notFound := toStatus(model.ErrNotFound)
	_, err := unaryObs(context.Background(), nil, unaryInfo, func(context.Context, any) (any, error) { return nil, notFound })
	st := status.Convert(err)
	if st.Code() != codes.NotFound || len(st.Details()) != 1 {
		t.Fatalf("status = %v, details %v", st, st.Details())
	}
	if info, ok := st.Details()[0].(*errdetails.ErrorInfo); !ok || info.GetReason() != "RESOURCE_NOT_FOUND" {
		t.Fatalf("detail = %v", st.Details()[0])
	}
	if n := errs.Load(); n != 0 {
		t.Fatalf("a NotFound was reported %d times", n)
	}

	internal := toStatus(errors.New("disk on fire"))
	_, err = unaryObs(context.Background(), nil, unaryInfo, func(context.Context, any) (any, error) { return nil, internal })
	if !errors.Is(err, internal) {
		t.Fatalf("err = %v, want the handler's error", err)
	}
	if n := errs.Load(); n != 1 {
		t.Fatalf("an Internal was reported %d times, want 1", n)
	}
}

func TestInterceptorCarriesTheRequestID(t *testing.T) {
	countReports(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "req-1"))
	var got string
	_, _ = unaryObs(ctx, nil, unaryInfo, func(ctx context.Context, _ any) (any, error) {
		got = obs.RequestID(ctx)
		return struct{}{}, nil
	})
	if got != "req-1" {
		t.Fatalf("request id = %q", got)
	}
	_, _ = unaryObs(context.Background(), nil, unaryInfo, func(ctx context.Context, _ any) (any, error) {
		got = obs.RequestID(ctx)
		return struct{}{}, nil
	})
	if got == "" {
		t.Fatal("no request id was made for a call without one")
	}
}
