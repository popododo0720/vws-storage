package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/popododo0720/vws-lib/obs"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close() //nolint:errcheck // test read
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

// startProcess runs the real process with the fake backends until the test
// ends and waits until it answers gRPC health. It returns the gRPC address.
// /readyz is not the signal: the process-wide health handler stays NotReady
// after an earlier run in this test binary shut down.
func startProcess(t *testing.T) string {
	t.Helper()
	listen, objects := freeAddr(t), freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runContext(ctx, []string{"-listen=" + listen, "-fake-object-addr=" + objects}) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
	conn, err := grpc.NewClient(listen, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := healthpb.NewHealthClient(conn)
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := client.Check(t.Context(), &healthpb.HealthCheckRequest{})
		if err == nil {
			return listen
		}
		if time.Now().After(deadline) {
			t.Fatalf("gRPC health never answered: %v", err)
		}
		select {
		case err := <-done:
			t.Fatalf("run exited before serving: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestMetricsServesVWSSeriesWhenPrometheusIsEnabled(t *testing.T) {
	healthAddr := freeAddr(t)
	t.Setenv(obs.EnvHealthAddr, healthAddr)
	t.Setenv(obs.EnvMetricsExporter, "prometheus")
	startProcess(t) // its gRPC health call is a vws_rpc_server_requests_total sample

	code, body := httpGet(t, "http://"+healthAddr+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics status %d: %s", code, body)
	}
	for _, want := range []string{
		`vws_build_info{`, `vws_queue_depth{`, `vws_rpc_server_requests_total{`, `service="vws-storage"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics has no %s", want)
		}
	}
	for _, l := range strings.Split(body, "\n") {
		if l != "" && !strings.HasPrefix(l, "#") && !strings.HasPrefix(l, "vws_") {
			t.Errorf("non-vws series on /metrics: %q", l)
		}
	}
}

func TestMetricsIsOffByDefault(t *testing.T) {
	healthAddr := freeAddr(t)
	t.Setenv(obs.EnvHealthAddr, healthAddr)
	t.Setenv(obs.EnvMetricsExporter, "")
	startProcess(t)
	if code, _ := httpGet(t, "http://"+healthAddr+"/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz status %d", code)
	}
	if code, _ := httpGet(t, "http://"+healthAddr+"/metrics"); code != http.StatusNotFound {
		t.Fatalf("/metrics status %d without OTEL_METRICS_EXPORTER=prometheus, want 404", code)
	}
}

// Prometheus without a health address has nowhere to serve: the process
// refuses to start instead of silently dropping metrics.
func TestPrometheusWithoutHealthAddrFailsStart(t *testing.T) {
	t.Setenv(obs.EnvHealthAddr, "")
	t.Setenv(obs.EnvMetricsExporter, "prometheus")
	err := runContext(t.Context(), []string{"-listen=" + freeAddr(t), "-fake-object-addr=" + freeAddr(t)})
	if err == nil || !strings.Contains(err.Error(), obs.EnvHealthAddr) {
		t.Fatalf("run: %v, want an error naming %s", err, obs.EnvHealthAddr)
	}
}
