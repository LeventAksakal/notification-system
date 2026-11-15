package httpapi

import (
	"context"
	"errors"
	"time"

	"github.com/LeventAksakal/notification-system/internal/store"
)

// defaultWindowSeconds is GetMetrics' window when the query does not set one
// (api/openapi.yaml; the spec validator enforces 1..3600).
const defaultWindowSeconds = 60

// MetricsReader computes the metrics summary from shared state (decision 0014).
type MetricsReader interface {
	Read(ctx context.Context, window time.Duration) (store.Metrics, error)
}

// errNoMetrics is a server built without a MetricsReader.
var errNoMetrics = errors.New("metrics: no reader configured")

// GetMetrics reads every number from MySQL on each call, so any API instance
// answers the same.
func (s *Server) GetMetrics(ctx context.Context, req GetMetricsRequestObject) (GetMetricsResponseObject, error) {
	if s.metrics == nil {
		return nil, errNoMetrics
	}
	window := defaultWindowSeconds
	if req.Params.WindowSeconds != nil {
		window = *req.Params.WindowSeconds
	}
	m, err := s.metrics.Read(ctx, time.Duration(window)*time.Second)
	if err != nil {
		return nil, err
	}
	out := GetMetrics200JSONResponse{
		GeneratedAt:   time.Now().UTC(),
		WindowSeconds: window,
		QueueDepth:    m.QueueDepth(),
		Totals: StatusCounts{
			Scheduled:  m.Totals[string(Scheduled)],
			Pending:    m.Totals[string(Pending)],
			Processing: m.Totals[string(Processing)],
			Sent:       m.Totals[string(Sent)],
			Failed:     m.Totals[string(Failed)],
			Cancelled:  m.Totals[string(Cancelled)],
		},
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
	return out, nil
}

func toDeliveryTotals(d store.Delivery) DeliveryTotals {
	return DeliveryTotals{
		Sent: d.Sent, Failed: d.Failed, Attempts: d.Attempts, FailedAttempts: d.FailedAttempts,
		LatencyMs:   LatencyMs{P50: d.Latency.P50, P95: d.Latency.P95, P99: d.Latency.P99},
		SuccessRate: d.SuccessRate(), FailureRate: d.FailureRate(),
	}
}
