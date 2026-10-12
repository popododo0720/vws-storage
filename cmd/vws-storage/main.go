// Command vws-storage serves the storage control plane over gRPC.
// The object backend is the in-process fake. The block backend is the fake or
// Kubernetes, chosen by -block-backend.
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
	objectAddr := fs.String("fake-object-addr", "127.0.0.1:7401", "fake object HTTP address")
	if err := fs.Parse(args); err != nil {
		return err
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

	objs := fake.NewObjects(fake.ObjectOptions{BaseURL: "http://" + *objectAddr})
	httpLn, err := net.Listen("tcp", *objectAddr)
	if err != nil {
		return err
	}
	httpSrv := &http.Server{Handler: objs.Handler(), ReadHeaderTimeout: 10 * time.Second}
	httpErr := make(chan error, 1)
	go func() { httpErr <- httpSrv.Serve(httpLn) }()
	stopHTTP := func() {
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shut)
	}

	store := storage.Memory()
	if *db != "" {
		store = storage.SQLite(*db)
	}
	svc, err := storage.Open(context.Background(), storage.Options{
		Store: store, Block: block, Object: objs, Signer: objs, Zones: zones,
	})
	if err != nil {
		stopHTTP()
		return err
	}
	runErr := make(chan error, 1)
	go func() { runErr <- svc.Run(ctx) }()

	grpcLn, err := net.Listen("tcp", *listen)
	if err != nil {
		stop()
		<-runErr
		closeErr := svc.Close()
		stopHTTP()
		return errors.Join(err, closeErr)
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(server.MaxRecvMsgSize))
	server.Register(gs, svc, server.Config{Projects: projects})
	grpcErr := make(chan error, 1)
	go func() { grpcErr <- gs.Serve(grpcLn) }()
	log.Printf("grpc %s object %s", grpcLn.Addr(), httpLn.Addr())

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
