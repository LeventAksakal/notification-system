// Package telemetry wires logs, traces and metrics. Logs go to stdout as JSON with
// the trace id attached, and to OTLP through the slog bridge, so a log line in
// Grafana links to its trace. With no endpoint configured nothing is exported:
// traces still get real ids (an SDK tracer provider without an exporter) so
// the W3C context reaches the provider; metrics and logs stay local.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// Setup installs the global logger and OTel providers. The returned func
// flushes and stops them; call it on shutdown with a bounded context.
//
// The tracer provider is built without WithSampler on purpose: the SDK then
// takes its sampler from OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG
// (validated in config), and WithSampler would override them. Unset, it is
// parentbased_always_on: 100%, the local default (decision 0019).
func Setup(ctx context.Context, service, endpoint string, level slog.Level) (func(context.Context) error, error) {
	stdout := contextHandler{slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})}
	// The W3C propagator is installed even without an exporter: an incoming
	// traceparent then still reaches the request context and the stored row.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if endpoint == "" {
		// A real tracer provider without an exporter: spans get valid ids, so
		// a worker still sends a valid traceparent to the provider and log
		// lines still carry trace_id; nothing leaves the process.
		tp := sdktrace.NewTracerProvider()
		otel.SetTracerProvider(tp)
		slog.SetDefault(slog.New(stdout))
		return tp.Shutdown, nil
	}

	// Every process is its own instance: without the id, the metric series of
	// several workers (same service name) overwrite each other in the backend.
	host, _ := os.Hostname() // a container's id under compose; "" is still told apart by the pid
	instance := fmt.Sprintf("%s-%d", host, os.Getpid())
	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(semconv.ServiceName(service), semconv.ServiceInstanceID(instance)))
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}
	traceExp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"))
	if err != nil {
		return nil, fmt.Errorf("trace exporter: %w", err)
	}
	metricExp, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(endpoint+"/v1/metrics"))
	if err != nil {
		return nil, fmt.Errorf("metric exporter: %w", err)
	}
	logExp, err := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(endpoint+"/v1/logs"))
	if err != nil {
		return nil, fmt.Errorf("log exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)), sdkmetric.WithResource(res))
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)), sdklog.WithResource(res))
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetLoggerProvider(lp)

	bridge := otelslog.NewHandler(service, otelslog.WithLoggerProvider(lp))
	slog.SetDefault(slog.New(slog.NewMultiHandler(stdout, levelFilter{plainValues{contextHandler{bridge}}, level})))

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx), lp.Shutdown(ctx))
	}, nil
}

// levelFilter applies the configured level to the OTLP bridge, which has none.
type levelFilter struct {
	slog.Handler
	min slog.Level
}

func (h levelFilter) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.min && h.Handler.Enabled(ctx, l)
}
func (h levelFilter) WithAttrs(as []slog.Attr) slog.Handler {
	return levelFilter{h.Handler.WithAttrs(as), h.min}
}
func (h levelFilter) WithGroup(n string) slog.Handler {
	return levelFilter{h.Handler.WithGroup(n), h.min}
}
