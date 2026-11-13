package obs

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type ctxKey int

const (
	correlationKey ctxKey = iota
	notificationKey
)

// WithCorrelationID returns ctx carrying the request's correlation id; every
// log line written with that context gets `correlation_id`.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey, id)
}

// CorrelationID returns the correlation id in ctx, or "".
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey).(string)
	return id
}

// WithNotificationID returns ctx carrying the notification a log line is
// about; every log line written with that context gets `notification_id`.
func WithNotificationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, notificationKey, id)
}

// WithNotification returns ctx for log lines about one notification: they
// carry its id and its stored correlation id (the one it was created with),
// which replaces the current request's. An empty correlationID keeps it.
func WithNotification(ctx context.Context, notificationID, correlationID string) context.Context {
	ctx = WithNotificationID(ctx, notificationID)
	if correlationID != "" {
		ctx = WithCorrelationID(ctx, correlationID)
	}
	return ctx
}

// NotificationID returns the notification id in ctx, or "".
func NotificationID(ctx context.Context) string {
	id, _ := ctx.Value(notificationKey).(string)
	return id
}

// Traceparent returns the W3C traceparent of the span in ctx, or "" when ctx
// carries no valid span context.
func Traceparent(ctx context.Context) string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// contextHandler adds the ids carried by the context to every record:
// trace_id and span_id of the active span, correlation_id and notification_id.
type contextHandler struct{ slog.Handler }

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	if id := CorrelationID(ctx); id != "" {
		r.AddAttrs(slog.String("correlation_id", id))
	}
	if id := NotificationID(ctx); id != "" {
		r.AddAttrs(slog.String("notification_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(as)}
}
func (h contextHandler) WithGroup(n string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(n)}
}

// NewHandler wraps h so records carry the context's ids (see contextHandler).
func NewHandler(h slog.Handler) slog.Handler { return contextHandler{h} }
