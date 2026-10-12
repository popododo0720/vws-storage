package server_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/popododo0720/vws-storage/internal/server"
)

func TestRecoveredPanicIsInternalAndLeaksNothing(t *testing.T) {
	const secret = "dsn=file:/var/lib/secret.db"
	_, err := server.RecoverUnary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"},
		func(context.Context, any) (any, error) { panic(secret) })
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(st.Message(), "secret") {
		t.Fatalf("message leaks the panic value: %q", st.Message())
	}
	err = server.RecoverStream(nil, nil, &grpc.StreamServerInfo{FullMethod: "/x/Z"},
		func(any, grpc.ServerStream) error { panic(secret) })
	if st, _ := status.FromError(err); st.Code() != codes.Internal || strings.Contains(st.Message(), "secret") {
		t.Fatalf("stream err = %v", err)
	}
}
