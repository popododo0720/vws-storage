package server

import (
	"google.golang.org/grpc"

	"github.com/popododo0720/vws-lib/obs/grpcobs"
)

// Service is the name vws-storage reports under.
const Service = "vws-storage"

// Options instruments the gRPC server with vws-lib grpcobs: otelgrpc traces,
// request id, panic recovery, one report per server fault, and
// vws.rpc.server.*. From vws-lib v0.1.1 a handler's status passes through
// with its ErrorInfo, and a caller error such as NotFound is not a fault.
func Options() []grpc.ServerOption { return grpcobs.ServerOptions(Service) }
