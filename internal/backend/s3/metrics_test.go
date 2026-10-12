package s3_test

import (
	"context"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/popododo0720/vws-lib/obs"
)

// A failed presign is counted in vws.aws.errors by action and S3 code only:
// the bucket, key and real bucket name never become labels.
func TestPresignErrorsAreCountedWithoutIdentifiers(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	shutdown, err := obs.Setup(context.Background(), obs.ForService("vws-storage"),
		obs.WithOverride(func(c *obs.Config) { c.MetricReaders = append(c.MetricReaders, reader) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	d := openOn(t, "http://127.0.0.1:1", "lab")
	r := ref(t, "secret-bucket", "secret-key.txt")
	past := time.Now().Add(-time.Minute)
	if _, err := d.PresignPut(context.Background(), r, 1, [32]byte{}, past); err == nil {
		t.Fatal("presign put with a past expiry succeeded")
	}
	if _, err := d.PresignGet(context.Background(), r, past); err == nil {
		t.Fatal("presign get with a past expiry succeeded")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || m.Name != obs.MetricAWSErrors {
				continue
			}
			for _, p := range sum.DataPoints {
				action, _ := p.Attributes.Value(obs.LabelAction)
				awsCode, _ := p.Attributes.Value(obs.LabelAWSCode)
				got[action.AsString()+"/"+awsCode.AsString()] += p.Value
				for _, kv := range p.Attributes.ToSlice() {
					switch v := kv.Value.String(); {
					case kv.Key != obs.LabelService && kv.Key != obs.LabelAction && kv.Key != obs.LabelAWSCode:
						t.Errorf("unexpected label %s", kv.Key)
					case strings.Contains(v, "secret") || strings.Contains(v, "lab-vws-"):
						t.Errorf("label %s=%q carries an identifier", kv.Key, v)
					}
				}
			}
		}
	}
	if got["presign_put/InvalidExpiry"] != 1 || got["presign_get/InvalidExpiry"] != 1 {
		t.Fatalf("vws.aws.errors = %v", got)
	}
}
