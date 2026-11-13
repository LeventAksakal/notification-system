//go:build integration

package store_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

// metricsQueries reads queries/metrics.sql into name -> statement, with each
// sqlc.arg(...) turned into a plain placeholder.
func metricsQueries(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile("queries/metrics.sql")
	if err != nil {
		t.Fatal(err)
	}
	arg := regexp.MustCompile(`sqlc\.arg\([a-z_]+\)`)
	out := map[string]string{}
	for _, part := range strings.Split(string(b), "-- name: ")[1:] {
		name, body, _ := strings.Cut(part, "\n")
		name, _, _ = strings.Cut(name, " ")
		var lines []string
		for _, l := range strings.Split(body, "\n") {
			if !strings.HasPrefix(strings.TrimSpace(l), "--") {
				lines = append(lines, l)
			}
		}
		out[name] = strings.TrimSuffix(strings.TrimSpace(arg.ReplaceAllString(strings.Join(lines, "\n"), "?")), ";")
	}
	return out
}

// Every metrics statement reads through an index; the windowed ones never
// scan notifications or delivery_attempts in full.
func TestMetricsQueriesUseTheirIndexes(t *testing.T) {
	db, _ := support.MySQL(t)
	for _, ch := range core.Channels {
		insertListed(t, db, 3, ch, core.StatusPending)
		insertListed(t, db, 2, ch, core.StatusSent)
	}
	if _, err := db.ExecContext(t.Context(), `UPDATE notifications SET finished_at = NOW(3) WHERE status = 'sent'`); err != nil {
		t.Fatal(err)
	}
	// Attempts spread over two days, so the window is a small part of the index.
	for i := range 200 {
		if _, err := db.ExecContext(t.Context(), `INSERT INTO delivery_attempts
			(notification_id, attempt_no, channel, started_at, duration_ms, outcome)
			SELECT id, 1, channel, NOW(3) - INTERVAL ? MINUTE, 5, 'accepted' FROM notifications LIMIT 1`, i*15); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(t.Context(), `ANALYZE TABLE notifications, delivery_attempts`); err != nil {
		t.Fatal(err)
	}
	window := int64(60_000_000)
	qs := metricsQueries(t)
	latency, latencyArgs := store.LatencyQuery(time.Minute)
	cases := []struct {
		name     string
		query    string
		args     []any
		index    string
		windowed bool
	}{
		{"queue", qs["MetricsQueue"], nil, "claim_idx", false},
		{"status counts", qs["MetricsStatusCounts"], nil, "list_status_idx", false},
		{"scheduled", qs["MetricsScheduled"], nil, "claim_idx", false},
		{"finished in window", qs["MetricsFinished"], []any{window}, "finished_idx", true},
		{"attempts in window", qs["MetricsAttempts"], []any{window}, "attempts_window_idx", true},
		{"latency in window", latency, latencyArgs, "finished_idx", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.query == "" {
				t.Fatal("statement missing from queries/metrics.sql")
			}
			used := false
			for _, row := range explain(t, db, tc.query, tc.args) {
				table := row["table"]
				if table != "notifications" && table != "delivery_attempts" {
					continue
				}
				t.Logf("%s: table=%s type=%s key=%s extra=%s", tc.name, table, row["type"], row["key"], row["Extra"])
				if (tc.windowed && row["type"] == "ALL") || row["type"] == "" {
					t.Errorf("%s scans %s in full: %v", tc.name, table, row)
				}
				if tc.windowed && row["type"] != "range" {
					t.Errorf("%s: want a range scan of %s, got type %q", tc.name, table, row["type"])
				}
				if row["key"] == tc.index {
					used = true
				}
			}
			if !used {
				t.Errorf("%s does not read through %s", tc.name, tc.index)
			}
		})
	}
}

// The reader's numbers match rows written with known states and times.
func TestMetricsReadCountsWhatIsStored(t *testing.T) {
	db, _ := support.MySQL(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(t.Context(), q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	insertListed(t, db, 2, core.ChannelSMS, core.StatusPending) // due now
	later := insertListed(t, db, 1, core.ChannelSMS, core.StatusPending)
	exec(`UPDATE notifications SET due_at = NOW(3) + INTERVAL 1 HOUR WHERE id = ?`, later[0])
	retry := insertListed(t, db, 1, core.ChannelSMS, core.StatusPending) // backing off: scheduled in the queue, pending in totals
	exec(`UPDATE notifications SET due_at = NOW(3) + INTERVAL 1 HOUR, attempts = 1 WHERE id = ?`, retry[0])
	insertListed(t, db, 1, core.ChannelEmail, core.StatusProcessing)
	sent := insertListed(t, db, 4, core.ChannelPush, core.StatusSent)
	for i, id := range sent {
		exec(`UPDATE notifications SET due_at = NOW(3) - INTERVAL ? MICROSECOND, finished_at = NOW(3) WHERE id = ?`, (i+1)*100_000, id)
		exec(`INSERT INTO delivery_attempts (notification_id, attempt_no, channel, started_at, duration_ms, outcome)
			VALUES (?, 1, 'push', NOW(3), 5, 'accepted')`, id)
	}
	failed := insertListed(t, db, 1, core.ChannelEmail, core.StatusFailed)
	exec(`UPDATE notifications SET finished_at = NOW(3) WHERE id = ?`, failed[0])
	exec(`INSERT INTO delivery_attempts (notification_id, attempt_no, channel, started_at, duration_ms, outcome)
		VALUES (?, 1, 'email', NOW(3), 5, 'permanent')`, failed[0])
	old := insertListed(t, db, 1, core.ChannelEmail, core.StatusSent) // outside the window
	exec(`UPDATE notifications SET finished_at = NOW(3) - INTERVAL 2 HOUR WHERE id = ?`, old[0])

	m, err := store.NewMetricsReader(db).Read(t.Context(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	want := []store.QueueDepth{
		{Channel: core.ChannelSMS, Pending: 2, Scheduled: 2},
		{Channel: core.ChannelEmail, Processing: 1},
		{Channel: core.ChannelPush},
	}
	for i, q := range m.Queue {
		age := q.OldestPendingAgeMs
		q.OldestPendingAgeMs = 0
		if q != want[i] {
			t.Errorf("queue[%d]: want %+v, got %+v", i, want[i], q)
		}
		if age < 0 || age > 60_000 {
			t.Errorf("queue[%d] oldest age: %d", i, age)
		}
	}
	if m.QueueDepth() != 5 {
		t.Errorf("queue depth: want 5, got %d", m.QueueDepth())
	}
	wantTotals := map[string]int{"pending": 3, "scheduled": 1, "processing": 1, "sent": 5, "failed": 1}
	for k, v := range wantTotals {
		if m.Totals[k] != v {
			t.Errorf("totals[%s]: want %d, got %d (%v)", k, v, m.Totals[k], m.Totals)
		}
	}
	d := m.Delivery
	if d.Sent != 4 || d.Failed != 1 || d.Attempts != 5 || d.FailedAttempts != 1 {
		t.Errorf("delivery: %+v", d)
	}
	if d.SuccessRate() != 0.8 || d.FailureRate() != 0.2 {
		t.Errorf("rates: %v %v", d.SuccessRate(), d.FailureRate())
	}
	// Latencies 100, 200, 300, 400 ms (each within a ms of it): nearest rank
	// p50 = 2nd, p95 = p99 = 4th.
	if l := d.Latency; l.P50 < 200 || l.P50 > 201 || l.P95 < 400 || l.P95 > 401 || l.P99 != l.P95 {
		t.Errorf("latency: %+v", l)
	}
	if m.ByChannel[2].Latency != d.Latency || m.ByChannel[0].Latency != (store.Latency{}) {
		t.Errorf("latency by channel: %+v", m.ByChannel)
	}
}
