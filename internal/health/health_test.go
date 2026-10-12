package health_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/popododo0720/vws-storage/internal/health"
)

type flag struct{ down atomic.Bool }

func (f *flag) check(context.Context) error {
	if f.down.Load() {
		return errors.New("down")
	}
	return nil
}

func serve(t *testing.T, m *health.Monitor) healthpb.HealthClient {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	healthpb.RegisterHealthServer(gs, m.Server())
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(gs.Stop)
	cc, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cc.Close(); err != nil {
			t.Error(err)
		}
	})
	return healthpb.NewHealthClient(cc)
}

func status(t *testing.T, c healthpb.HealthClient, service string) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	resp, err := c.Check(context.Background(), &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		t.Fatal(err)
	}
	return resp.GetStatus()
}

func TestReadinessIsPerServiceAndLivenessIsNot(t *testing.T) {
	block, other := &flag{}, &flag{}
	m := health.New(health.Options{Targets: []health.Target{
		{Service: "a.Block", Checks: []health.Check{other.check, block.check}},
		{Service: "a.Object", Checks: []health.Check{other.check}},
	}})
	c := serve(t, m)

	if got := status(t, c, "a.Block"); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("before the first evaluation: %v", got)
	}
	m.Evaluate(context.Background())
	for _, name := range []string{"", health.ReadyService, "a.Block", "a.Object"} {
		if got := status(t, c, name); got != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("%q = %v", name, got)
		}
	}

	block.down.Store(true)
	m.Evaluate(context.Background())
	want := map[string]healthpb.HealthCheckResponse_ServingStatus{
		"":                  healthpb.HealthCheckResponse_SERVING,
		health.ReadyService: healthpb.HealthCheckResponse_NOT_SERVING,
		"a.Block":           healthpb.HealthCheckResponse_NOT_SERVING,
		"a.Object":          healthpb.HealthCheckResponse_SERVING,
	}
	for name, w := range want {
		if got := status(t, c, name); got != w {
			t.Fatalf("%q = %v, want %v", name, got, w)
		}
	}

	block.down.Store(false)
	m.Evaluate(context.Background())
	if got := status(t, c, health.ReadyService); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("after recovery: %v", got)
	}
}

func TestWatchSeesAChange(t *testing.T) {
	f := &flag{}
	m := health.New(health.Options{Targets: []health.Target{{Service: "a.Block", Checks: []health.Check{f.check}}}})
	c := serve(t, m)
	m.Evaluate(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w, err := c.Watch(ctx, &healthpb.HealthCheckRequest{Service: "a.Block"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := w.Recv()
	if err != nil || first.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("first = %v, %v", first, err)
	}
	f.down.Store(true)
	m.Evaluate(ctx)
	next, err := w.Recv()
	if err != nil || next.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("next = %v, %v", next, err)
	}
}

func TestSlowCheckIsCutAtTheTimeout(t *testing.T) {
	m := health.New(health.Options{Timeout: 50 * time.Millisecond, Targets: []health.Target{{
		Service: "a.Block",
		Checks:  []health.Check{func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
	}}})
	c := serve(t, m)
	done := make(chan struct{})
	go func() { m.Evaluate(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Evaluate did not return")
	}
	if got := status(t, c, "a.Block"); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("a.Block = %v", got)
	}
}

func TestShutdownTakesEverythingOutIncludingLiveness(t *testing.T) {
	m := health.New(health.Options{Targets: []health.Target{{Service: "a.Block"}}})
	c := serve(t, m)
	m.Evaluate(context.Background())
	m.Shutdown()
	m.Evaluate(context.Background())
	for _, name := range []string{"", health.ReadyService, "a.Block"} {
		if got := status(t, c, name); got != healthpb.HealthCheckResponse_NOT_SERVING {
			t.Fatalf("%q = %v", name, got)
		}
	}
}

func TestRunEvaluatesAtOnceAndStopsWithItsContext(t *testing.T) {
	m := health.New(health.Options{Period: time.Hour, Targets: []health.Target{{Service: "a.Block"}}})
	c := serve(t, m)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for status(t, c, "a.Block") != healthpb.HealthCheckResponse_SERVING {
		if time.Now().After(deadline) {
			t.Fatal("not serving after Run started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
}
