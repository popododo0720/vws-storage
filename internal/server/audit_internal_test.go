package server

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-storage/internal/model"
)

// The audit log and vws.audit.events come from the same call, so a mutation
// cannot be recorded in one and missing from the other.
func TestAuditCountsTheEvent(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	old := slog.Default()
	shutdown, err := obs.Setup(context.Background(), obs.ForService(Service),
		obs.WithOverride(func(c *obs.Config) {
			c.MetricReaders = append(c.MetricReaders, reader)
			c.HealthAddr = ""
			c.LogOutput = io.Discard
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = shutdown(context.Background())
		slog.SetDefault(old)
	})

	(&Server{}).audit(context.Background(), model.CreateVolumeRequest{}, nil)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != obs.MetricAuditEvents || !ok {
				continue
			}
			for _, p := range sum.DataPoints {
				label := func(k attribute.Key) string {
					v, _ := p.Attributes.Value(k)
					return v.AsString()
				}
				if label(obs.LabelService) != Service ||
					label(obs.LabelAction) != "create_volume" ||
					label(obs.LabelOutcome) != "ok" {
					continue
				}
				if p.Value != 1 {
					t.Fatalf("%s = %d, want 1", obs.MetricAuditEvents, p.Value)
				}
				return
			}
		}
	}
	t.Fatalf("no %s{service=%s,action=create_volume,outcome=ok}", obs.MetricAuditEvents, Service)
}
