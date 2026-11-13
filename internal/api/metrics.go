package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
	out := metricsResponse{
		Metrics: Metrics{
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
			Queue: make([]QueueDepth, len(m.Queue)),
		},
		Delivery:          toDeliveryTotals(m.Delivery),
		DeliveryByChannel: make([]deliveryStats, len(m.ByChannel)),
	}
	for i, q := range m.Queue {
		out.Queue[i] = QueueDepth{Channel: Channel(q.Channel), Pending: q.Pending, Scheduled: q.Scheduled,
			Processing: q.Processing, OldestPendingAgeMs: q.OldestPendingAgeMs}
	}
	for i, d := range m.ByChannel {
		t := toDeliveryTotals(d.Delivery)
		out.DeliveryByChannel[i] = deliveryStats{
			DeliveryStats: DeliveryStats{Channel: Channel(d.Channel), Sent: t.Sent, Failed: t.Failed,
				Attempts: t.Attempts, FailedAttempts: t.FailedAttempts, LatencyMs: t.LatencyMs},
			SuccessRate: t.SuccessRate, FailureRate: t.FailureRate,
		}
	}
	return out, nil
}

// The generated models type `number` as float32, which rounds a rate such as
// 15/18 at the seventh digit. These wrappers keep the generated shape and
// encode the rates as float64 (owner item 0009 asks for `format: double`).
type (
	deliveryTotals struct {
		DeliveryTotals
		SuccessRate float64 `json:"successRate"`
		FailureRate float64 `json:"failureRate"`
	}
	deliveryStats struct {
		DeliveryStats
		SuccessRate float64 `json:"successRate"`
		FailureRate float64 `json:"failureRate"`
	}
	metricsResponse struct {
		Metrics
		Delivery          deliveryTotals  `json:"delivery"`
		DeliveryByChannel []deliveryStats `json:"deliveryByChannel"`
	}
)

var _ GetMetricsResponseObject = metricsResponse{}

// VisitGetMetricsResponse writes 200 with the Metrics body, as the generated
// GetMetrics200JSONResponse does.
func (m metricsResponse) VisitGetMetricsResponse(w http.ResponseWriter) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(m); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err := buf.WriteTo(w)
	return err
}

func toDeliveryTotals(d store.Delivery) deliveryTotals {
	return deliveryTotals{
		DeliveryTotals: DeliveryTotals{
			Sent: d.Sent, Failed: d.Failed, Attempts: d.Attempts, FailedAttempts: d.FailedAttempts,
			LatencyMs: LatencyMs{P50: d.Latency.P50, P95: d.Latency.P95, P99: d.Latency.P99},
		},
		SuccessRate: d.SuccessRate(), FailureRate: d.FailureRate(),
	}
}
