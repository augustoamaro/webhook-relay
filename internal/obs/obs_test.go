package obs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSetupNoEndpointIsNoop(t *testing.T) {
	// Without OTEL_EXPORTER_OTLP_ENDPOINT, Setup must succeed with a no-op pipeline.
	shutdown, metrics, err := Setup(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	metrics.DeliveryAttempt("success", time.Millisecond) // must not panic
	metrics.EndToEnd(time.Second)
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPMiddlewarePassesThrough(t *testing.T) {
	h := HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 204 {
		t.Fatalf("code: %d", rec.Code)
	}
}
