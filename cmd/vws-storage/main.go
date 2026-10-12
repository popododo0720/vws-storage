// Command vws-storage serves the storage control plane over gRPC.
// -block-backend picks the fake or Kubernetes. -object-backend picks the
// in-process fake or an S3-compatible endpoint (Ceph RGW or MinIO).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/backend/s3"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/server"
	"github.com/popododo0720/vws-storage/internal/storage"
)

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("vws-storage", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:7400", "gRPC listen address")
	db := fs.String("db", "", "SQLite file; empty keeps state in memory")
	zonesFlag := fs.String("zones", defaultZones, "comma-separated zone names")
	projectsFlag := fs.String("projects", "", "comma-separated project ids; empty accepts any valid project")
	blockBackend := fs.String("block-backend", "fake", "block backend: fake or kube")
	kubeCfg := kube.DefaultConfig()
	fs.StringVar(&kubeCfg.Kubeconfig, "kube-config", "", "kubeconfig path for -block-backend=kube; empty uses in-cluster credentials")
	fs.StringVar(&kubeCfg.Namespace, "kube-namespace", kubeCfg.Namespace, "namespace for volume claims and snapshots")
	standardClass := fs.String("kube-class-standard", kube.DefaultClass, "StorageClass for standard volumes")
	ssdClass := fs.String("kube-class-ssd", kube.DefaultClass, "StorageClass for ssd volumes")
	fs.StringVar(&kubeCfg.SnapshotClass, "kube-snapshot-class", kubeCfg.SnapshotClass, "VolumeSnapshotClass for snapshots")
	objectBackend := fs.String("object-backend", "fake", "object backend: fake or s3")
	s3Profile := fs.String("s3-profile", string(s3.RGW), "endpoint profile for -object-backend=s3: rgw or minio")
	s3Endpoint := fs.String("s3-endpoint", "", "S3 endpoint URL for -object-backend=s3")
	s3Region := fs.String("s3-region", "", "S3 region; empty uses the profile default")
	objectAddr := fs.String("fake-object-addr", "127.0.0.1:7401", "fake object HTTP address")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	zones, err := parseZones(*zonesFlag)
	if err != nil {
		return err
	}
	projects, err := parseProjects(*projectsFlag)
	if err != nil {
		return err
	}
	kubeCfg.Classes = kube.Classes{model.VolumeTypeStandard: *standardClass, model.VolumeTypeSSD: *ssdClass}
	block, err := openBlock(*blockBackend, kubeCfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var lc net.ListenConfig

	objs, err := openObjects(*objectBackend, *objectAddr, *s3Profile, *s3Endpoint, *s3Region)
	if err != nil {
		return err
	}
	httpErr := make(chan error, 1)
	stopHTTP := func() {}
	objectAt := *s3Endpoint
	if objs.handler != nil {
		httpLn, err := lc.Listen(ctx, "tcp", *objectAddr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", *objectAddr, err)
		}
		httpSrv := &http.Server{Handler: objs.handler, ReadHeaderTimeout: 10 * time.Second}
		go func() { httpErr <- httpSrv.Serve(httpLn) }()
		stopHTTP = func() {
			shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := httpSrv.Shutdown(shut); err != nil {
				log.Printf("vws-storage: object http shutdown: %v", err)
			}
		}
		objectAt = httpLn.Addr().String()
	}

	store := storage.Memory()
	if *db != "" {
		store = storage.SQLite(*db)
	}
	svc, err := storage.Open(context.Background(), storage.Options{
		Store: store, Block: block, Object: objs.object, Signer: objs.signer, Zones: zones,
	})
	if err != nil {
		stopHTTP()
		return err
	}
	runErr := make(chan error, 1)
	go func() { runErr <- svc.Run(ctx) }()

	grpcLn, err := lc.Listen(ctx, "tcp", *listen)
	if err != nil {
		stop()
		<-runErr
		closeErr := svc.Close()
		stopHTTP()
		return errors.Join(fmt.Errorf("listen %s: %w", *listen, err), closeErr)
	}
	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(server.MaxRecvMsgSize),
		grpc.ChainUnaryInterceptor(server.RecoverUnary),
		grpc.ChainStreamInterceptor(server.RecoverStream),
	)
	server.Register(gs, svc, server.Config{Projects: projects})
	grpcErr := make(chan error, 1)
	go func() { grpcErr <- gs.Serve(grpcLn) }()
	log.Printf("grpc %s object %s", grpcLn.Addr(), objectAt)

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-grpcErr:
	case err := <-httpErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr = err
		}
	}
	stop()
	gs.GracefulStop()
	stopHTTP()
	<-runErr
	if err := svc.Close(); err != nil {
		serveErr = errors.Join(serveErr, err)
	}
	return serveErr
}

const defaultZones = "vws-1a"

type objects struct {
	object  backend.Object
	signer  backend.Signer
	handler http.Handler // serves the fake's presigned URLs. nil for s3.
}

// openObjects reads the S3 keys from VWS_S3_ACCESS_KEY and VWS_S3_SECRET_KEY,
// so a secret never appears in the process arguments.
func openObjects(name, fakeAddr, profile, endpoint, region string) (objects, error) {
	switch name {
	case "fake":
		o := fake.NewObjects(fake.ObjectOptions{BaseURL: "http://" + fakeAddr})
		return objects{object: o, signer: o, handler: o.Handler()}, nil
	case "s3":
		cfg, err := s3.ProfileConfig(s3.Profile(profile))
		if err != nil {
			return objects{}, err
		}
		cfg.Endpoint = endpoint
		if region != "" {
			cfg.Region = region
		}
		cfg.AccessKey, cfg.SecretKey = os.Getenv("VWS_S3_ACCESS_KEY"), os.Getenv("VWS_S3_SECRET_KEY")
		d, err := s3.Open(cfg)
		if err != nil {
			return objects{}, err
		}
		return objects{object: d, signer: d}, nil
	default:
		return objects{}, fmt.Errorf("%w: object backend %q", model.ErrInvalidArgument, name)
	}
}

func openBlock(name string, cfg kube.Config) (backend.Block, error) {
	switch name {
	case "fake":
		return fake.NewBlock(), nil
	case "kube":
		return kube.Open(cfg)
	default:
		return nil, fmt.Errorf("%w: block backend %q", model.ErrInvalidArgument, name)
	}
}

func parseZones(s string) ([]model.Zone, error) {
	var zones []model.Zone
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		z, err := model.ParseZone(part)
		if err != nil {
			return nil, err
		}
		zones = append(zones, z)
	}
	if len(zones) == 0 {
		return nil, fmt.Errorf("%w: zones", model.ErrInvalidArgument)
	}
	return zones, nil
}

func parseProjects(s string) ([]model.Project, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var projects []model.Project
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		p, err := model.ParseProject(part)
		if err != nil {
			return nil, err
		}
		projects = append(projects, p)
	}
	return projects, nil
}
