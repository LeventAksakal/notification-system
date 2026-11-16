package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// QueueDepth is one channel's unfinished work.
type QueueDepth struct {
	Channel            notification.Channel
	Pending            int // stored pending and due now
	Scheduled          int // stored pending and due later (scheduled or backing off)
	Processing         int
	OldestPendingAgeMs int // age of the oldest due pending row; 0 when none
}

// Latency holds percentiles in milliseconds, zeros when empty: the nearest
// rank located in the per-minute latency buckets and interpolated inside its
// bucket, so each is within its bucket's width of the exact nearest-rank value
// (exact to 16 ms, under 12.5% above; latency_buckets.go).
type Latency struct{ P50, P95, P99 int }

// Delivery is what happened in the window, for one channel or for all.
type Delivery struct {
	Sent, Failed int
	// Attempts counts calls with an outcome (not in flight); FailedAttempts
	// the transient and permanent ones; UnknownAttempts those whose result was
	// never recorded (decision 0005), which are neither.
	Attempts, FailedAttempts, UnknownAttempts int
	Latency                                   Latency
}

// SuccessRate is sent / (sent + failed); 0 when both are 0.
func (d Delivery) SuccessRate() float64 {
	if d.Sent+d.Failed == 0 {
		return 0
	}
	return float64(d.Sent) / float64(d.Sent+d.Failed)
}

// FailureRate is failed / (sent + failed); 0 when both are 0.
func (d Delivery) FailureRate() float64 {
	if d.Sent+d.Failed == 0 {
		return 0
	}
	return float64(d.Failed) / float64(d.Sent+d.Failed)
}

// Metrics is the summary GET /v1/metrics shows (decision 0014). Every number
// is read from MySQL on the database clock, so every instance agrees.
type Metrics struct {
	Queue     []QueueDepth // one per channel, in notification.Channels order
	Totals    map[string]int
	Delivery  Delivery
	ByChannel []ChannelDelivery // one per channel, in notification.Channels order
}

// ChannelDelivery is one channel's Delivery.
type ChannelDelivery struct {
	Channel notification.Channel
	Delivery
}

// QueueDepth is every unfinished notification: pending (due or not) plus processing.
func (m Metrics) QueueDepth() int {
	n := 0
	for _, q := range m.Queue {
		n += q.Pending + q.Scheduled + q.Processing
	}
	return n
}

// MetricsReader computes Metrics from the database.
type MetricsReader struct {
	db *sql.DB
	q  *sqlc.Queries
}

// NewMetricsReader reads from db.
func NewMetricsReader(db *sql.DB) *MetricsReader {
	return &MetricsReader{db: db, q: sqlc.New(db)}
}

// Read computes the metrics, with delivery over the last window. Every query
// runs in one read-only REPEATABLE READ transaction against one reading of
// the database clock, so the numbers describe one moment and agree. Delivery
// comes from the per-minute rollups plus at most half a minute of raw rows,
// so its cost does not grow with the traffic in the window; queue depth and
// the status totals are live index reads (the totals scan every kept row:
// owner item 0017).
func (r *MetricsReader) Read(ctx context.Context, window time.Duration) (Metrics, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return Metrics{}, fmt.Errorf("metrics: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // read only: nothing to commit
	q := r.q.WithTx(tx)
	now, err := q.MetricsNow(ctx)
	if err != nil {
		return Metrics{}, fmt.Errorf("metrics clock: %w", err)
	}
	since := now.Add(-window)

	m := Metrics{Totals: map[string]int{}}
	queue := map[notification.Channel]QueueDepth{}
	rows, err := q.MetricsQueue(ctx, sqlc.MetricsQueueParams{Now: now})
	if err != nil {
		return Metrics{}, fmt.Errorf("metrics queue: %w", err)
	}
	for _, row := range rows {
		ch := notification.Channel(row.Channel)
		queue[ch] = QueueDepth{Channel: ch, Pending: int(row.PendingDue), Scheduled: int(row.PendingLater),
			Processing: int(row.Processing), OldestPendingAgeMs: max(int(row.OldestDueAgeMs), 0)}
	}
	for _, ch := range notification.Channels {
		qd := queue[ch]
		qd.Channel = ch
		m.Queue = append(m.Queue, qd)
	}

	counts, err := q.MetricsStatusCounts(ctx)
	if err != nil {
		return Metrics{}, fmt.Errorf("metrics status counts: %w", err)
	}
	for _, c := range counts {
		m.Totals[string(c.Status)] = int(c.N)
	}
	scheduled, err := q.MetricsScheduled(ctx, now)
	if err != nil {
		return Metrics{}, fmt.Errorf("metrics scheduled: %w", err)
	}
	// The API view splits scheduled out of pending (notification.Status docs). On one
	// snapshot scheduled rows are a subset of pending ones; the clamp is a guard.
	m.Totals["scheduled"] = int(scheduled)
	m.Totals[string(notification.StatusPending)] = max(m.Totals[string(notification.StatusPending)]-int(scheduled), 0)

	by, err := delivery(ctx, q, since)
	if err != nil {
		return Metrics{}, err
	}
	all := latencyHistogram{}
	for _, ch := range notification.Channels {
		all.add(by[ch].latency)
	}
	m.Delivery.Latency = all.percentiles()
	for _, ch := range notification.Channels {
		d := by[ch].Delivery
		d.Latency = by[ch].latency.percentiles()
		m.ByChannel = append(m.ByChannel, ChannelDelivery{Channel: ch, Delivery: d})
		m.Delivery.Sent += d.Sent
		m.Delivery.Failed += d.Failed
		m.Delivery.Attempts += d.Attempts
		m.Delivery.FailedAttempts += d.FailedAttempts
		m.Delivery.UnknownAttempts += d.UnknownAttempts
	}
	return m, nil
}

// channelDelivery is one channel's counts and latency buckets while they are summed.
type channelDelivery struct {
	Delivery
	latency latencyHistogram
}

// delivery counts what happened in [since, now] per channel: whole minutes
// from the rollups, and the partial first minute from the raw rows on the
// same timestamps the rollup triggers truncate (started_at for attempts,
// finished_at for notifications), so no outcome is counted twice or missed.
// Of the partial minute m0 it reads the shorter side of since: the raw rows
// of [since, m0+1min) added to the rollups from m0+1min, or those of
// [m0, since) taken from the rollups from m0 (exact: a rollup row equals a
// recount of its rows). At most halfMinute of raw rows, whatever the window.
func delivery(ctx context.Context, q *sqlc.Queries, since time.Time) (map[notification.Channel]*channelDelivery, error) {
	by := map[notification.Channel]*channelDelivery{}
	for _, ch := range notification.Channels {
		by[ch] = &channelDelivery{latency: latencyHistogram{}}
	}
	attempt := func(ch, outcome string, n int64) {
		d := by[notification.Channel(ch)]
		if d == nil {
			return
		}
		// Attempts are calls with a result; in_flight counts nowhere and
		// unknown only in its own count (owner item 0015).
		switch notification.AttemptOutcome(outcome) {
		case notification.OutcomeAccepted:
			d.Attempts += int(n)
		case notification.OutcomeTransient, notification.OutcomePermanent:
			d.Attempts += int(n)
			d.FailedAttempts += int(n)
		case notification.OutcomeUnknown:
			d.UnknownAttempts += int(n)
		}
	}
	finish := func(ch, status string, le, n int64) {
		d := by[notification.Channel(ch)]
		if d == nil {
			return
		}
		switch notification.Status(status) {
		case notification.StatusSent:
			d.Sent += int(n)
			d.latency[le] += n
		case notification.StatusFailed:
			d.Failed += int(n)
		}
	}

	m0 := since.Truncate(time.Minute)
	from, rawFrom, rawUntil, sign := m0, since, since, int64(1) // since on a minute: no raw rows
	switch {
	case since.Equal(m0):
	case since.Sub(m0) < halfMinute:
		rawFrom, rawUntil, sign = m0, since, -1
	default:
		from, rawFrom, rawUntil = m0.Add(time.Minute), since, m0.Add(time.Minute)
	}

	attempts, err := q.MetricsAttemptRollups(ctx, from)
	if err != nil {
		return nil, fmt.Errorf("metrics attempt rollups: %w", err)
	}
	for _, a := range attempts {
		attempt(string(a.Channel), string(a.Outcome), a.N)
	}
	finishes, err := q.MetricsFinishRollups(ctx, from)
	if err != nil {
		return nil, fmt.Errorf("metrics finish rollups: %w", err)
	}
	for _, f := range finishes {
		finish(string(f.Channel), string(f.Status), int64(f.LatencyLeMs), f.N) //nolint:gosec // bucket bounds stay far below 2^63
	}
	if !rawUntil.After(rawFrom) {
		return by, nil
	}
	rawAttempts, err := q.MetricsAttemptsBetween(ctx, sqlc.MetricsAttemptsBetweenParams{FromTime: rawFrom, UntilTime: rawUntil})
	if err != nil {
		return nil, fmt.Errorf("metrics attempts in [%s, %s): %w", rawFrom, rawUntil, err)
	}
	for _, a := range rawAttempts {
		attempt(string(a.Channel), string(a.Outcome), sign)
	}
	rawFinishes, err := q.MetricsFinishesBetween(ctx, sqlc.MetricsFinishesBetweenParams{
		FromTime: sql.NullTime{Time: rawFrom, Valid: true}, UntilTime: sql.NullTime{Time: rawUntil, Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("metrics finishes in [%s, %s): %w", rawFrom, rawUntil, err)
	}
	for _, f := range rawFinishes {
		finish(string(f.Channel), string(f.Status), latencyBucket(f.LatencyMs), sign)
	}
	return by, nil
}

// halfMinute splits the partial first minute of a window: below it the raw
// rows before since are subtracted, from it those after are added, so the
// raw read never covers more than half a minute.
const halfMinute = 30 * time.Second
