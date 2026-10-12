package storage_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

// forbiddenLabels can grow without bound or identify a caller. A metric label
// must never be one of them.
var forbiddenLabels = []string{"bucket", "key", "object", "resource_id", "request_id", "operation_id", "volume", "snapshot", "project", "trace_id"}

// setupMetrics runs obs.Setup with an in-memory reader. obs is process-wide.
func setupMetrics(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	shutdown, err := obs.Setup(context.Background(), obs.ForService("vws-storage"),
		obs.WithOverride(func(c *obs.Config) { c.MetricReaders = append(c.MetricReaders, reader) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	return reader
}

type point struct {
	name  string
	attrs attribute.Set
}

func collect(t *testing.T, r *sdkmetric.ManualReader) []point {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var out []point
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					out = append(out, point{m.Name, p.Attributes})
				}
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					out = append(out, point{m.Name, p.Attributes})
				}
			case metricdata.Gauge[float64]:
				for _, p := range d.DataPoints {
					out = append(out, point{m.Name, p.Attributes})
				}
			case metricdata.Histogram[float64]:
				for _, p := range d.DataPoints {
					out = append(out, point{m.Name, p.Attributes})
				}
			}
		}
	}
	return out
}

// assertNoIdentifiers fails when a label key is forbidden or a label value
// carries one of the ids the test created.
func assertNoIdentifiers(t *testing.T, points []point, ids ...string) {
	t.Helper()
	for _, p := range points {
		for _, kv := range p.attrs.ToSlice() {
			for _, bad := range forbiddenLabels {
				if string(kv.Key) == bad {
					t.Errorf("%s has label %q", p.name, bad)
				}
			}
			for _, id := range ids {
				if strings.Contains(kv.Value.String(), id) {
					t.Errorf("%s label %s=%q carries %q", p.name, kv.Key, kv.Value.String(), id)
				}
			}
		}
	}
}

func has(points []point, name string, key attribute.Key, value string) bool {
	for _, p := range points {
		if v, ok := p.attrs.Value(key); p.name == name && ok && v.AsString() == value {
			return true
		}
	}
	return false
}

func TestEngineMetricsCarryNoIdentifiers(t *testing.T) {
	reader := setupMetrics(t)
	obj := fake.NewObjects(fake.ObjectOptions{Now: time.Now})
	svc, cancel := openRunning(t, storage.Memory(), fake.NewBlock(), obj)
	defer cancel()
	obs.M().RegisterQueue(storage.QueueName, svc.QueueDepth, svc.QueueOldest)

	volOp, err := svc.Submit(context.Background(), token(t, "vol"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	vol := waitDone(t, svc, volOp.ID).Target().(model.VolumeID)
	snapOp, err := svc.Submit(context.Background(), token(t, "snap"), model.CreateSnapshotRequest{Source: vol})
	if err != nil {
		t.Fatal(err)
	}
	snap := waitDone(t, svc, snapOp.ID)
	bucket, err := model.ParseBucketName("metrics-secret-bucket")
	if err != nil {
		t.Fatal(err)
	}
	bop, err := svc.Submit(context.Background(), token(t, "bkt"), model.CreateBucketRequest{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, svc, bop.ID)

	points := collect(t, reader)
	if !has(points, obs.MetricQueueDepth, obs.LabelQueue, storage.QueueName) {
		t.Errorf("no %s{queue=%s}", obs.MetricQueueDepth, storage.QueueName)
	}
	if !has(points, obs.MetricQueueOldestAge, obs.LabelQueue, storage.QueueName) {
		t.Errorf("no %s{queue=%s}", obs.MetricQueueOldestAge, storage.QueueName)
	}
	if !has(points, obs.MetricReconcileDuration, obs.LabelController, storage.SnapshotReadyController) {
		t.Errorf("no %s{controller=%s}", obs.MetricReconcileDuration, storage.SnapshotReadyController)
	}
	assertNoIdentifiers(t, points, "metrics-secret-bucket", vol.String(), snap.Target().String(),
		volOp.ID.String(), snapOp.ID.String())
}
