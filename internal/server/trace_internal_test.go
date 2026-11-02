package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

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
			traceparent := make(chan string, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				traceparent <- r.Header.Get("traceparent")
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

			if got := <-traceparent; !strings.Contains(got, parent.SpanContext().TraceID().String()) {
				t.Fatalf("traceparent = %q, want trace %s", got, parent.SpanContext().TraceID())
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

// installRecorder points the global tracer at a span recorder for one test.
func installRecorder(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	oldTP, oldProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTP)
		otel.SetTextMapPropagator(oldProp)
	})
	return tp, recorder
}

func transferSpans(recorder *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var spans []sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.Name() == "object transfer" {
			spans = append(spans, s)
		}
	}
	return spans
}

// The transfer wraps an outgoing HTTP request, so its span is a client span,
// and it covers the body transfer: it stays open until the body is closed.
func TestObjectTransferSpanIsClientAndCoversTheBody(t *testing.T) {
	_, recorder := installRecorder(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "payload")
	}))
	defer srv.Close()

	resp, err := doLocator(context.Background(), http.MethodGet, model.Locator{URL: srv.URL + "/b/k"}, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	if got := transferSpans(recorder); len(got) != 0 {
		t.Fatal("span ended before the body was read")
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
	if got := transferSpans(recorder); len(got) != 0 {
		t.Fatal("span ended before the body was closed")
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close() // a second close must not end the span twice
	ended := transferSpans(recorder)
	if len(ended) != 1 {
		t.Fatalf("ended transfer spans = %d, want 1", len(ended))
	}
	if ended[0].SpanKind() != trace.SpanKindClient {
		t.Errorf("span kind = %v, want client", ended[0].SpanKind())
	}
	if ended[0].Status().Code != otelcodes.Unset {
		t.Errorf("status = %v, want unset for 200", ended[0].Status())
	}
}

// A failed request has no body to close, so the span ends at once with Error.
func TestObjectTransferSpanEndsOnRequestError(t *testing.T) {
	_, recorder := installRecorder(t)
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	//nolint:bodyclose // the request fails, so there is no response body to close
	if _, err := doLocator(context.Background(), http.MethodGet, model.Locator{URL: url + "/b/k"}, http.NoBody); err == nil {
		t.Fatal("want a request error")
	}
	ended := transferSpans(recorder)
	if len(ended) != 1 || ended[0].Status().Code != otelcodes.Error {
		t.Fatalf("ended spans = %v, want one with Error status", ended)
	}
}

// OTel HTTP client semantic conventions treat 4xx as an error on client spans,
// for example 403 on an expired presign or 404 on a missing object.
func TestObjectTransferSpanMarks4xxAsError(t *testing.T) {
	for _, code := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			_, recorder := installRecorder(t)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(code)
			}))
			defer srv.Close()

			if _, err := getBytes(context.Background(), model.Locator{URL: srv.URL + "/b/k"}); err == nil {
				t.Fatal("want an error for a non-2xx response")
			}
			ended := transferSpans(recorder)
			if len(ended) != 1 {
				t.Fatalf("ended transfer spans = %d, want 1", len(ended))
			}
			if ended[0].Status().Code != otelcodes.Error {
				t.Errorf("status = %v, want Error", ended[0].Status())
			}
		})
	}
}
