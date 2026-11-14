package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// QueueDepth is one channel's unfinished work.
type QueueDepth struct {
	Channel            core.Channel
	Pending            int // stored pending and due now
	Scheduled          int // stored pending and due later (scheduled or backing off)
	Processing         int
	OldestPendingAgeMs int // age of the oldest due pending row; 0 when none
}

// Latency holds nearest-rank percentiles in milliseconds; zeros when empty.
type Latency struct{ P50, P95, P99 int }

// Delivery is what happened in the window, for one channel or for all.
type Delivery struct {
	Sent, Failed             int
	Attempts, FailedAttempts int
	Latency                  Latency
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
	Queue     []QueueDepth // one per channel, in core.Channels order
	Totals    map[string]int
	Delivery  Delivery
	ByChannel []ChannelDelivery // one per channel, in core.Channels order
}

// ChannelDelivery is one channel's Delivery.
type ChannelDelivery struct {
	Channel core.Channel
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

// LatencyQuery returns the percentile statement and its arguments for the
// window that starts at since. Latency runs from the first due time (the
// creation, or the schedule when later; a retry or release moves due_at but
// not this) to provider acceptance. Percentiles are computed in MySQL with
// window functions (nearest rank: the value at row ceil(p*n) in latency
// order), so only the rows at the ranks travel, whatever the rate times the
// window is. The scan is a finished_idx range over the window's finished rows
// (status = 'sent' is checked in the index; the times are read from the row).
func LatencyQuery(since time.Time) (string, []any) {
	return `WITH sent AS (
  SELECT channel, GREATEST(CEIL(TIMESTAMPDIFF(MICROSECOND,
    GREATEST(COALESCE(scheduled_at, created_at), created_at), finished_at) / 1000), 0) AS ms
  FROM notifications FORCE INDEX (finished_idx)
  WHERE finished_at >= ? AND status = 'sent'
), ranked AS (
  SELECT channel, ms,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ms) AS rn,
    COUNT(*) OVER (PARTITION BY channel) AS n,
    ROW_NUMBER() OVER (ORDER BY ms) AS rn_all,
    COUNT(*) OVER () AS n_all
  FROM sent
)
SELECT CAST(channel AS CHAR), CAST(ms AS SIGNED), rn, n, rn_all, n_all FROM ranked
WHERE rn IN (CEIL(0.5 * n), CEIL(0.95 * n), CEIL(0.99 * n))
   OR rn_all IN (CEIL(0.5 * n_all), CEIL(0.95 * n_all), CEIL(0.99 * n_all))`, []any{since}
}

// Read computes the metrics, with delivery over the last window. Every query
// runs in one read-only REPEATABLE READ transaction against one reading of
// the database clock, so the numbers describe one moment and agree.
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
	queue := map[core.Channel]QueueDepth{}
	rows, err := q.MetricsQueue(ctx, sqlc.MetricsQueueParams{Now: now})
	if err != nil {
		return Metrics{}, fmt.Errorf("metrics queue: %w", err)
	}
	for _, row := range rows {
		ch := core.Channel(row.Channel)
		queue[ch] = QueueDepth{Channel: ch, Pending: int(row.PendingDue), Scheduled: int(row.PendingLater),
			Processing: int(row.Processing), OldestPendingAgeMs: max(int(row.OldestDueAgeMs), 0)}
	}
	for _, ch := range core.Channels {
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
	// The API view splits scheduled out of pending (core.Status docs). On one
	// snapshot scheduled rows are a subset of pending ones; the clamp is a guard.
	m.Totals["scheduled"] = int(scheduled)
	m.Totals[string(core.StatusPending)] = max(m.Totals[string(core.StatusPending)]-int(scheduled), 0)

	by := map[core.Channel]*Delivery{}
	for _, ch := range core.Channels {
		by[ch] = &Delivery{}
	}
	finished, err := q.MetricsFinished(ctx, sql.NullTime{Time: since, Valid: true})
	if err != nil {
		return Metrics{}, fmt.Errorf("metrics finished: %w", err)
	}
	for _, f := range finished {
		if d := by[core.Channel(f.Channel)]; d != nil {
			d.Sent, d.Failed = int(f.Sent), int(f.Failed)
		}
	}
	attempts, err := q.MetricsAttempts(ctx, since)
	if err != nil {
		return Metrics{}, fmt.Errorf("metrics attempts: %w", err)
	}
	for _, a := range attempts {
		if d := by[core.Channel(a.Channel)]; d != nil {
			d.Attempts, d.FailedAttempts = int(a.Attempts), int(a.FailedAttempts)
		}
	}
	if err := latency(ctx, tx, since, by, &m.Delivery.Latency); err != nil {
		return Metrics{}, err
	}
	for _, ch := range core.Channels {
		d := *by[ch]
		m.ByChannel = append(m.ByChannel, ChannelDelivery{Channel: ch, Delivery: d})
		m.Delivery.Sent += d.Sent
		m.Delivery.Failed += d.Failed
		m.Delivery.Attempts += d.Attempts
		m.Delivery.FailedAttempts += d.FailedAttempts
	}
	return m, nil
}

func latency(ctx context.Context, tx *sql.Tx, since time.Time, by map[core.Channel]*Delivery, all *Latency) error {
	query, args := LatencyQuery(since)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("metrics latency: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var ch string
		var ms, rn, n, rnAll, nAll int64
		if err := rows.Scan(&ch, &ms, &rn, &n, &rnAll, &nAll); err != nil {
			return fmt.Errorf("metrics latency: %w", err)
		}
		if d := by[core.Channel(ch)]; d != nil {
			setRank(&d.Latency, rn, n, ms)
		}
		setRank(all, rnAll, nAll, ms)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("metrics latency: %w", err)
	}
	return nil
}

// setRank stores ms in each percentile whose nearest rank is rn of n.
func setRank(l *Latency, rn, n, ms int64) {
	v := int(ms)
	if rn == nearestRank(50, n) {
		l.P50 = v
	}
	if rn == nearestRank(95, n) {
		l.P95 = v
	}
	if rn == nearestRank(99, n) {
		l.P99 = v
	}
}

// nearestRank is ceil(pct/100 * n) in integers: the SQL's exact decimal
// CEIL(0.95 * n), without float rounding.
func nearestRank(pct, n int64) int64 {
	return (pct*n + 99) / 100
}
