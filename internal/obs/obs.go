// Package obs wires logs, traces and metrics. Logs go to stdout as JSON with
// the trace id attached, and to OTLP through the slog bridge, so a log line in
// Grafana links to its trace. With no endpoint configured nothing is exported
// and the global providers stay no-ops (tests, local runs without the stack).
package obs

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
	"go.opentelemetry.io/otel/trace"
)

// Setup installs the global logger and OTel providers. The returned func
// flushes and stops them; call it on shutdown with a bounded context.
func Setup(ctx context.Context, service, endpoint string, level slog.Level) (func(context.Context) error, error) {
	stdout := traceIDHandler{slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})}
	if endpoint == "" {
		slog.SetDefault(slog.New(stdout))
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(semconv.ServiceName(service)))
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
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	bridge := otelslog.NewHandler(service, otelslog.WithLoggerProvider(lp))
	slog.SetDefault(slog.New(slog.NewMultiHandler(stdout, levelFilter{bridge, level})))

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx), lp.Shutdown(ctx))
	}, nil
}

// traceIDHandler adds trace_id and span_id to every record logged with a
// context that carries a span.
type traceIDHandler struct{ slog.Handler }

func (h traceIDHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceIDHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return traceIDHandler{h.Handler.WithAttrs(as)}
}
func (h traceIDHandler) WithGroup(n string) slog.Handler {
	return traceIDHandler{h.Handler.WithGroup(n)}
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
