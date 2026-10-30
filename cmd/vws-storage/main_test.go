package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
	"github.com/popododo0720/vws-storage/internal/health"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/server"
	"github.com/popododo0720/vws-storage/internal/storage"
)

func TestDefaultZoneIsVWS1A(t *testing.T) {
	zones, err := parseZones(defaultZones)
	if err != nil {
		t.Fatal(err)
	}
	if len(zones) != 1 || zones[0].String() != "vws-1a" {
		t.Fatalf("zones %v", zones)
	}
}

func TestBlockBackendSelection(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	for name, args := range map[string][]string{
		"unknown backend":    {"-block-backend=nope"},
		"kube without creds": {"-block-backend=kube", "-kube-config=" + missing},
	} {
		if err := run(args); err == nil {
			t.Fatalf("%s: run succeeded", name)
		}
	}
	blk, err := openBlock("fake", kube.DefaultConfig())
	if err != nil || blk == nil {
		t.Fatalf("fake: %v", err)
	}
}

func TestObjectBackendSelection(t *testing.T) {
	t.Setenv("VWS_S3_ACCESS_KEY", "")
	t.Setenv("VWS_S3_SECRET_KEY", "")
	for name, args := range map[string][]string{
		"unknown backend": {"-object-backend=nope"},
		"unknown profile": {"-object-backend=s3", "-s3-profile=ceph", "-s3-endpoint=http://127.0.0.1:1"},
		"no endpoint":     {"-object-backend=s3"},
		"no keys":         {"-object-backend=s3", "-s3-endpoint=http://127.0.0.1:1"},
	} {
		if err := run(args); err == nil {
			t.Fatalf("%s: run succeeded", name)
		}
	}
	t.Setenv("VWS_S3_ACCESS_KEY", "k")
	t.Setenv("VWS_S3_SECRET_KEY", "s")
	o, err := openObjects("s3", "", "minio", "http://127.0.0.1:1", "", "")
	if err != nil || o.handler != nil || o.object == nil || o.signer == nil {
		t.Fatalf("s3: %+v %v", o, err)
	}
	f, err := openObjects("fake", "127.0.0.1:0", "", "", "", "")
	if err != nil || f.handler == nil {
		t.Fatalf("fake: %+v %v", f, err)
	}
}

func TestHealthTargetsCoverEveryRegisteredService(t *testing.T) {
	svc, err := storage.Open(context.Background(), storage.Options{
		Store: storage.Memory(), Block: fake.NewBlock(), Object: fake.NewObjects(fake.ObjectOptions{}),
		Signer: fake.NewObjects(fake.ObjectOptions{}), Zones: []model.Zone{mustZone(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	gs := grpc.NewServer()
	server.Register(gs, svc, server.Config{})
	var want []string
	for name := range gs.GetServiceInfo() {
		want = append(want, name)
	}
	var got []string
	for _, tg := range healthTargets(svc, fake.NewBlock()) {
		got = append(got, tg.Service)
	}
	sort.Strings(want)
	sort.Strings(got)
	if !slices.Equal(want, got) {
		t.Fatalf("health targets %v, registered services %v", got, want)
	}
}

// /readyz must not answer 200 from the empty handler while the process is
// still starting.
func TestReadyCheckStaysClosedUntilTheMonitorIsWired(t *testing.T) {
	var c monitorCheck
	if err := c.Ready(context.Background()); err == nil {
		t.Fatal("ready before the monitor exists")
	}
	mon := health.New(health.Options{Targets: []health.Target{{Service: "a.Block"}}})
	c.mon.Store(mon)
	if err := c.Ready(context.Background()); err == nil {
		t.Fatal("ready before the first evaluation")
	}
	mon.Evaluate(context.Background())
	if err := c.Ready(context.Background()); err != nil {
		t.Fatalf("ready after evaluation: %v", err)
	}
	mon.Shutdown()
	if err := c.Ready(context.Background()); err == nil {
		t.Fatal("ready after shutdown")
	}
}

func TestKubeBlockAddsTheClusterCheckToTheBlockServicesOnly(t *testing.T) {
	svc, err := storage.Open(context.Background(), storage.Options{
		Store: storage.Memory(), Block: fake.NewBlock(), Object: fake.NewObjects(fake.ObjectOptions{}),
		Signer: fake.NewObjects(fake.ObjectOptions{}), Zones: []model.Zone{mustZone(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	withKube := map[string]int{}
	for _, tg := range healthTargets(svc, readyBlock{fake.NewBlock()}) {
		withKube[tg.Service] = len(tg.Checks)
	}
	for _, name := range server.BlockServices {
		if withKube[name] != 2 {
			t.Errorf("%s has %d checks, want 2", name, withKube[name])
		}
	}
	for _, name := range server.ObjectServices {
		if withKube[name] != 1 {
			t.Errorf("%s has %d checks, want 1", name, withKube[name])
		}
	}
}

type readyBlock struct{ *fake.Block }

func (readyBlock) Ready(context.Context) error { return nil }

func mustZone(t *testing.T) model.Zone {
	t.Helper()
	z, err := model.ParseZone("vws-1a")
	if err != nil {
		t.Fatal(err)
	}
	return z
}

func TestBadBucketPrefixFailsStartWithoutKeys(t *testing.T) {
	const ak, sk = "AKIAKEYMUSTNOTLEAK", "secret-must-not-leak"
	t.Setenv("VWS_S3_ACCESS_KEY", ak)
	t.Setenv("VWS_S3_SECRET_KEY", sk)
	var logs bytes.Buffer
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	for _, p := range []string{"UPPER", strings.Repeat("a", 19), "x_y"} {
		err := run([]string{"-object-backend=s3", "-s3-endpoint=http://127.0.0.1:1", "-s3-bucket-prefix=" + p})
		if err == nil {
			t.Fatalf("prefix %q: run succeeded", p)
		}
		for _, s := range []string{err.Error(), logs.String()} {
			if strings.Contains(s, ak) || strings.Contains(s, sk) {
				t.Fatalf("prefix %q: key in %q", p, s)
			}
		}
	}
}
