package server

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/popododo0720/vws-lib/obs"
	"github.com/popododo0720/vws-lib/obs/grpcobs"
	"github.com/popododo0720/vws-lib/obs/report"
)

// Service is the name vws-storage reports under.
const Service = "vws-storage"

// errPanic is what a client sees for a recovered panic. The panic value and
// stack go to report.Panic.
var errPanic = status.Error(codes.Internal, "internal error")

// Options instruments the gRPC server: otelgrpc traces, then the unary and
// stream interceptors below.
//
// They do what vws-lib grpcobs.UnaryServerInterceptor does (request id, panic
// recovery, one report per server fault, vws.rpc.server.*), except that the
// handler's status is returned unchanged. grpcobs passes every error through
// errs.GRPCStatus, which rebuilds a status from code and message: that drops the
// ErrorInfo reason toStatus attaches (RESOURCE_NOT_FOUND, GENERATION_CONFLICT, ...)
// and, because these errors are not errs.E, reports every NotFound as a server
// fault.
func Options() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithMeterProvider(noop.NewMeterProvider()))),
		grpc.ChainUnaryInterceptor(unaryObs),
		grpc.ChainStreamInterceptor(streamObs),
	}
}

func requestContext(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(grpcobs.RequestIDHeader); len(v) > 0 && v[0] != "" {
			return obs.WithRequestID(ctx, v[0])
		}
	}
	return obs.WithRequestID(ctx, obs.NewRequestID())
}

// serverFault reports whether the code is the server's fault. Only those are
// reported; a caller's mistake shows in the RPC metrics.
func serverFault(c codes.Code) bool {
	switch c {
	case codes.Internal, codes.Unavailable, codes.Unknown, codes.DataLoss:
		return true
	default:
		return false
	}
}

func finish(ctx context.Context, method string, start time.Time, err error) {
	c := status.Code(err)
	if serverFault(c) {
		_ = report.Error(ctx, err, slog.String(obs.FieldOp, method)) //nolint:errcheck // the marked error is not needed
	}
	obs.M().RPCServer(ctx, Service, method, c.String(), time.Since(start))
}

func unaryObs(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (resp any, err error) {
	start := time.Now()
	ctx = requestContext(ctx)
	defer func() {
		if r := recover(); r != nil {
			_ = report.Panic(ctx, r) //nolint:errcheck // the client gets errPanic
			resp, err = nil, errPanic
			obs.M().RPCServer(ctx, Service, info.FullMethod, codes.Internal.String(), time.Since(start))
		}
	}()
	resp, err = next(ctx, req)
	finish(ctx, info.FullMethod, start, err)
	return resp, err
}

func streamObs(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) (err error) {
	start := time.Now()
	ctx := requestContext(ss.Context())
	defer func() {
		if r := recover(); r != nil {
			_ = report.Panic(ctx, r) //nolint:errcheck // the client gets errPanic
			err = errPanic
			obs.M().RPCServer(ctx, Service, info.FullMethod, codes.Internal.String(), time.Since(start))
		}
	}()
	err = next(srv, &ctxStream{ServerStream: ss, ctx: ctx})
	finish(ctx, info.FullMethod, start, err)
	return err
}

type ctxStream struct {
	grpc.ServerStream
	ctx context.Context //nolint:containedctx // the stream carries its request context
}

func (s *ctxStream) Context() context.Context { return s.ctx }
