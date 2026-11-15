package delivery

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
)

// storedTraceID is the trace id of the traceparents the API "stored".
const storedTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"

// The sampling decision is taken once, at the API, and the worker honours it
// through the stored traceparent (decision 0019): with the production sampler
// (parentbased_traceidratio) an unsampled stored trace gets no recorded
// delivery span even at ratio 1, and a sampled one is recorded even at ratio
// 0; with no stored trace the ratio decides. The tracer provider is the one
// telemetry.Setup builds, in both of its branches (no endpoint, and an OTLP
// endpoint: a local stand-in that accepts everything), the sampler comes from
// the environment, and the tracer is the one cmd/app gives the worker.
func TestTheWorkerHonoursTheStoredSamplingDecision(t *testing.T) {
	otlp := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(otlp.Close)
	for name, tc := range map[string]sampling{
		"unsampled at the API, ratio 1: nothing recorded": {ratio: "1", stored: "00-" + storedTraceID + "-00f067aa0ba902b7-00"},
		"sampled at the API, ratio 0: recorded":           {ratio: "0", stored: "00-" + storedTraceID + "-00f067aa0ba902b7-01", recorded: true},
		// Without a stored trace the ratio decides: 0 proves the variable
		// reaches the provider (the SDK's default would record it).
		"no stored trace, ratio 0: nothing recorded": {ratio: "0"},
	} {
		for branch, endpoint := range map[string]string{"no endpoint": "", "OTLP endpoint": otlp.URL} {
			t.Run(name+"/"+branch, func(t *testing.T) { tc.check(t, endpoint) })
		}
	}
}

// sampling is one case: the ratio set, the traceparent stored on the job
// ("" for none), and whether the delivery span must be recorded.
type sampling struct {
	ratio, stored string
	recorded      bool
}

func (tc sampling) check(t *testing.T, endpoint string) {
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", tc.ratio)
	t.Cleanup(keepGlobals())
	shutdown, err := telemetry.Setup(t.Context(), "test", endpoint, slog.LevelError)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	q := newMemQueue()
	q.add(1, notification.ChannelSMS, notification.PriorityNormal)
	q.rows[q.order[0]].job.Traceparent = tc.stored

	var (
		mu        sync.Mutex
		seen      []trace.SpanContext
		recording []bool   // asked during the call: an ended span no longer records
		sent      []string // traceparent the provider received
	)
	snd := scriptSender{func(ctx context.Context, m provider.Message) (provider.Response, error) {
		carrier := propagation.MapCarrier{}
		propagation.TraceContext{}.Inject(ctx, carrier)
		span := trace.SpanFromContext(ctx)
		mu.Lock()
		seen = append(seen, span.SpanContext())
		recording = append(recording, span.IsRecording())
		sent = append(sent, carrier.Get("traceparent"))
		mu.Unlock()
		return accept(ctx, m)
	}}
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: snd,
		Tracer: otel.Tracer("notifier/worker")}))
	waitFor(t, "the notification sent", func() bool { return q.count(notification.StatusSent) == 1 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	if len(seen) != 1 {
		t.Fatalf("provider calls: want 1, got %d", len(seen))
	}
	if tc.stored != "" && seen[0].TraceID().String() != storedTraceID {
		t.Errorf("delivery span trace id: want the stored %s, got %s", storedTraceID, seen[0].TraceID())
	}
	if recording[0] != tc.recorded {
		t.Errorf("delivery span recording: want %v, got %v", tc.recorded, recording[0])
	}
	if seen[0].IsSampled() != tc.recorded {
		t.Errorf("delivery span sampled flag: want %v, got %v", tc.recorded, seen[0].IsSampled())
	}
	wantFlags := "-00"
	if tc.recorded {
		wantFlags = "-01"
	}
	if !strings.HasSuffix(sent[0], wantFlags) || !seen[0].IsValid() {
		t.Errorf("traceparent to the provider: want a valid one ending %s, got %q", wantFlags, sent[0])
	}
}

// keepGlobals saves what telemetry.Setup replaces and returns its restore.
func keepGlobals() func() {
	tp, mp, lp := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetLoggerProvider()
	prop, log := otel.GetTextMapPropagator(), slog.Default()
	return func() {
		otel.SetTracerProvider(tp)
		otel.SetMeterProvider(mp)
		otel.SetLoggerProvider(lp)
		otel.SetTextMapPropagator(prop)
		slog.SetDefault(log)
	}
}
