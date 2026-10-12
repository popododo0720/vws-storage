package server

import (
	"context"
	"log"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errPanic is what a client sees for a recovered panic. The panic value and
// stack stay in the log.
var errPanic = status.Error(codes.Internal, "internal error")

// RecoverUnary turns a panic in a unary handler into Internal.
func RecoverUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("vws-storage: panic in %s: %v\n%s", info.FullMethod, r, debug.Stack())
			resp, err = nil, errPanic
		}
	}()
	return next(ctx, req)
}

// RecoverStream turns a panic in a streaming handler into Internal.
func RecoverStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("vws-storage: panic in %s: %v\n%s", info.FullMethod, r, debug.Stack())
			err = errPanic
		}
	}()
	return next(srv, ss)
}
