// Package health answers the standard grpc.health.v1 service.
//
// The empty service name is liveness: SERVING while the process runs, with no
// dependency checked, so a dependency outage never restarts the pod. Each gRPC
// service name is readiness for that service. ReadyService is SERVING only
// when every service is, for a probe that names one service.
package health

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// ReadyService is the aggregate readiness name. It is not a registered gRPC service.
const ReadyService = "vws.storage.v1.Ready"

// Check reports why a dependency is not usable, or nil.
type Check func(ctx context.Context) error

// Target is one service name and the checks that must pass for it to serve.
type Target struct {
	Service string
	Checks  []Check
}

// Options configures a Monitor.
type Options struct {
	Targets []Target
	// Period is the time between evaluations. Zero is 10s.
	Period time.Duration
	// Timeout bounds one evaluation. Zero is 3s.
	Timeout time.Duration
}

// Monitor evaluates the targets in the background. Check answers from memory.
type Monitor struct {
	srv     *health.Server
	targets []Target
	period  time.Duration
	timeout time.Duration

	mu    sync.Mutex
	last  map[string]string // service -> failure text, "" when serving
	ready bool
	shut  bool
}

// errNotReady is all Ready says: the failure text stays in the log.
var errNotReady = errors.New("not ready")

// Ready answers the aggregate readiness (ReadyService) from the last
// Evaluate, without running a check. It is the HTTP /readyz check.
func (m *Monitor) Ready(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.ready || m.shut {
		return errNotReady
	}
	return nil
}

// New returns a Monitor. Every service starts NOT_SERVING until the first Evaluate.
func New(o Options) *Monitor {
	if o.Period <= 0 {
		o.Period = 10 * time.Second
	}
	if o.Timeout <= 0 {
		o.Timeout = 3 * time.Second
	}
	m := &Monitor{srv: health.NewServer(), targets: o.Targets, period: o.Period, timeout: o.Timeout, last: map[string]string{}}
	m.srv.SetServingStatus(ReadyService, healthpb.HealthCheckResponse_NOT_SERVING)
	for _, t := range o.Targets {
		m.srv.SetServingStatus(t.Service, healthpb.HealthCheckResponse_NOT_SERVING)
	}
	return m
}

// Server is the grpc.health.v1 implementation to register.
func (m *Monitor) Server() healthpb.HealthServer { return m.srv }

// Evaluate runs every check once and publishes the result. A service passes
// when all its checks do. The first failure of a service is logged, and so is
// its recovery, so a steady outage is not logged every period.
func (m *Monitor) Evaluate(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	all := true
	for _, t := range m.targets {
		var errs []error
		for _, c := range t.Checks {
			if err := c(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		err := errors.Join(errs...)
		m.publish(t.Service, err)
		all = all && err == nil
	}
	m.srv.SetServingStatus(ReadyService, status(all))
	m.mu.Lock()
	m.ready = all
	m.mu.Unlock()
}

func (m *Monitor) publish(service string, err error) {
	m.srv.SetServingStatus(service, status(err == nil))
	text := ""
	if err != nil {
		text = err.Error()
	}
	m.mu.Lock()
	prev, seen := m.last[service]
	m.last[service] = text
	m.mu.Unlock()
	switch {
	case text != "" && prev == "":
		log.Printf("vws-storage: health: %s NOT_SERVING: %s", service, text)
	case text == "" && seen && prev != "":
		log.Printf("vws-storage: health: %s SERVING", service)
	}
}

func status(ok bool) healthpb.HealthCheckResponse_ServingStatus {
	if ok {
		return healthpb.HealthCheckResponse_SERVING
	}
	return healthpb.HealthCheckResponse_NOT_SERVING
}

// retryWhileDown is how soon a Monitor re-evaluates while something is down,
// so a pod does not wait a whole Period after its dependencies come up.
const retryWhileDown = 2 * time.Second

// Run evaluates at once, then every Period (or sooner while not ready), until
// ctx is done.
func (m *Monitor) Run(ctx context.Context) {
	for {
		m.Evaluate(ctx)
		wait := m.period
		m.mu.Lock()
		if !m.ready && wait > retryWhileDown {
			wait = retryWhileDown
		}
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// Shutdown answers NOT_SERVING for every name, including liveness, and
// ignores later updates. Call it before GracefulStop.
func (m *Monitor) Shutdown() {
	m.mu.Lock()
	m.shut = true
	m.mu.Unlock()
	m.srv.Shutdown()
}

// Describe wraps a check's error with what it checks.
func Describe(what string, c Check) Check {
	return func(ctx context.Context) error {
		if err := c(ctx); err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		return nil
	}
}
