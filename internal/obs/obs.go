// Package obs wires OpenTelemetry: OTLP traces + metrics when an endpoint is
// configured (standard OTEL_EXPORTER_OTLP_ENDPOINT), no-op otherwise.
package obs

import (
	"context"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Metrics implements dispatch.Metrics plus ingest counters.
type Metrics struct {
	attempts   metric.Int64Counter
	attemptDur metric.Float64Histogram
	endToEnd   metric.Float64Histogram
}

// DeliveryAttempt records a delivery attempt counter and its duration, tagged by outcome.
func (m *Metrics) DeliveryAttempt(outcome string, d time.Duration) {
	ctx := context.Background()
	m.attempts.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	m.attemptDur.Record(ctx, d.Seconds(), metric.WithAttributes(attribute.String("outcome", outcome)))
}

// EndToEnd records the elapsed time from message creation to successful delivery.
func (m *Metrics) EndToEnd(d time.Duration) {
	m.endToEnd.Record(context.Background(), d.Seconds())
}

// Setup configures global tracer/meter providers. Returns a shutdown func.
func Setup(ctx context.Context, service string) (func(context.Context) error, *Metrics, error) {
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(service)))
	if err != nil {
		return nil, nil, err
	}
	otel.SetTextMapPropagator(propagation.TraceContext{})

	shutdowns := []func(context.Context) error{}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		texp, err := otlptracegrpc.New(ctx)
		if err != nil {
			return nil, nil, err
		}
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(texp), sdktrace.WithResource(res))
		otel.SetTracerProvider(tp)
		shutdowns = append(shutdowns, tp.Shutdown)

		mexp, err := otlpmetricgrpc.New(ctx)
		if err != nil {
			return nil, nil, err
		}
		mp := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(mexp, sdkmetric.WithInterval(10*time.Second))),
			sdkmetric.WithResource(res))
		otel.SetMeterProvider(mp)
		shutdowns = append(shutdowns, mp.Shutdown)
	}

	meter := otel.Meter("webhook-relay")
	attempts, err := meter.Int64Counter("relay.delivery.attempts",
		metric.WithDescription("delivery attempts by outcome"))
	if err != nil {
		return nil, nil, err
	}
	attemptDur, err := meter.Float64Histogram("relay.delivery.attempt.duration",
		metric.WithUnit("s"))
	if err != nil {
		return nil, nil, err
	}
	endToEnd, err := meter.Float64Histogram("relay.delivery.end_to_end",
		metric.WithUnit("s"), metric.WithDescription("message created to delivery succeeded"))
	if err != nil {
		return nil, nil, err
	}

	shutdown := func(ctx context.Context) error {
		var firstErr error
		for _, fn := range shutdowns {
			if err := fn(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
	return shutdown, &Metrics{attempts: attempts, attemptDur: attemptDur, endToEnd: endToEnd}, nil
}

// HTTPMiddleware instruments the API with server spans + http metrics.
func HTTPMiddleware(next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "relay-api")
}
