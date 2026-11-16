package httpapi

import (
	"context"
	"errors"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
)

// MetricsReader computes the metrics summary from shared state (decision 0014).
type MetricsReader interface {
	Read(ctx context.Context, window time.Duration) (store.Metrics, error)
}

// errNoMetrics is a server built without a MetricsReader.
var errNoMetrics = errors.New("metrics: no reader configured")

type metricsInput struct {
	WindowSeconds windowSeconds `query:"windowSeconds"`
}

func (*metricsInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type metricsOutput struct{ Body Metrics }

// GetMetrics reads every number from MySQL on each call, so any API instance
// answers the same.
func (s *Server) GetMetrics(ctx context.Context, in *metricsInput) (*metricsOutput, error) {
	if s.metrics == nil {
		return nil, errNoMetrics
	}
	window := time.Duration(in.WindowSeconds) * time.Second
	if window == 0 {
		window = notification.DefaultMetricsWindow
	}
	m, err := s.metrics.Read(ctx, window)
	if err != nil {
		return nil, err
	}
	out := Metrics{
		GeneratedAt:       time.Now().UTC(),
		WindowSeconds:     int(window / time.Second),
		QueueDepth:        m.QueueDepth(),
		Totals:            toCounts(m.Totals),
		Queue:             make([]QueueDepth, len(m.Queue)),
		Delivery:          toDeliveryTotals(m.Delivery),
		DeliveryByChannel: make([]DeliveryStats, len(m.ByChannel)),
	}
	for i, q := range m.Queue {
		out.Queue[i] = QueueDepth{Channel: Channel(q.Channel), Pending: q.Pending, Scheduled: q.Scheduled,
			Processing: q.Processing, OldestPendingAgeMs: q.OldestPendingAgeMs}
	}
	for i, d := range m.ByChannel {
		t := toDeliveryTotals(d.Delivery)
		out.DeliveryByChannel[i] = DeliveryStats{Channel: Channel(d.Channel), Sent: t.Sent, Failed: t.Failed,
			Attempts: t.Attempts, FailedAttempts: t.FailedAttempts, LatencyMs: t.LatencyMs,
			SuccessRate: t.SuccessRate, FailureRate: t.FailureRate}
	}
	return &metricsOutput{out}, nil
}

func toDeliveryTotals(d store.Delivery) DeliveryTotals {
	return DeliveryTotals{
		Sent: d.Sent, Failed: d.Failed, Attempts: d.Attempts, FailedAttempts: d.FailedAttempts,
		LatencyMs:   LatencyMs{P50: d.Latency.P50, P95: d.Latency.P95, P99: d.Latency.P99},
		SuccessRate: d.SuccessRate(), FailureRate: d.FailureRate(),
	}
}
