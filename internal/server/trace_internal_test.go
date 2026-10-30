package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/popododo0720/vws-storage/internal/model"
)

// The bytes go straight to the object endpoint, so the service's span and
// trace context must continue there. The presigned URL is the bucket and the
// key, so neither may appear in the span.
func TestObjectTransferContinuesTheTraceWithoutTheURL(t *testing.T) {
	cases := map[string]struct {
		method string
		call   func(context.Context, model.Locator) error
	}{
		"write": {http.MethodPut, func(ctx context.Context, loc model.Locator) error {
			return putBytes(ctx, loc, []byte("payload"))
		}},
		"read": {http.MethodGet, func(ctx context.Context, loc model.Locator) error {
			_, err := getBytes(ctx, loc)
			return err
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var traceparent string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				traceparent = r.Header.Get("traceparent")
				if r.Method == http.MethodGet {
					_, _ = io.WriteString(w, "payload")
				}
			}))
			defer srv.Close()

			recorder := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			oldTP, oldProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
			otel.SetTracerProvider(tp)
			otel.SetTextMapPropagator(propagation.TraceContext{})
			t.Cleanup(func() {
				otel.SetTracerProvider(oldTP)
				otel.SetTextMapPropagator(oldProp)
			})

			ctx, parent := tp.Tracer("test").Start(context.Background(), "rpc")
			loc := model.Locator{Method: tc.method, URL: srv.URL + "/vws-bucket/secret-key"}
			if err := tc.call(ctx, loc); err != nil {
				t.Fatal(err)
			}
			parent.End()

			if !strings.Contains(traceparent, parent.SpanContext().TraceID().String()) {
				t.Fatalf("traceparent = %q, want trace %s", traceparent, parent.SpanContext().TraceID())
			}
			var child sdktrace.ReadOnlySpan
			for _, s := range recorder.Ended() {
				if s.Parent().SpanID() == parent.SpanContext().SpanID() {
					child = s
				}
			}
			if child == nil {
				t.Fatal("no child span for the object transfer")
			}
			if child.Name() != "object transfer" {
				t.Errorf("span name = %q", child.Name())
			}
			for _, kv := range child.Attributes() {
				got := kv.Value.String()
				if strings.Contains(got, "vws-bucket") || strings.Contains(got, "secret-key") {
					t.Errorf("span attribute %s carries %q", kv.Key, got)
				}
			}
		})
	}
}
