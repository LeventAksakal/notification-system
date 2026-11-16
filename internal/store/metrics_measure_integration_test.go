//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/migrations"
	"github.com/LeventAksakal/notification-system/test/support"
)

// The endpoint's delivery numbers as computed before the rollups (main at
// ebedafb, with the attempt outcomes of decision 0005): every row of the
// window, exact nearest-rank percentiles. The baseline and the oracle.
const (
	rawFinishedSQL = `SELECT channel, CAST(COALESCE(SUM(status = 'sent'), 0) AS SIGNED), CAST(COALESCE(SUM(status = 'failed'), 0) AS SIGNED)
FROM notifications FORCE INDEX (finished_idx) WHERE finished_at >= ? AND status IN ('sent', 'failed') GROUP BY channel`
	rawAttemptsSQL = `SELECT channel, CAST(COALESCE(SUM(outcome IN ('accepted', 'transient', 'permanent')), 0) AS SIGNED),
  CAST(COALESCE(SUM(outcome IN ('transient', 'permanent')), 0) AS SIGNED), CAST(COALESCE(SUM(outcome = 'unknown'), 0) AS SIGNED)
FROM delivery_attempts FORCE INDEX (attempts_window_idx) WHERE started_at >= ? GROUP BY channel`
	rawLatencySQL = `WITH sent AS (
  SELECT channel, GREATEST(CEIL(TIMESTAMPDIFF(MICROSECOND,
    GREATEST(COALESCE(scheduled_at, created_at), created_at), finished_at) / 1000), 0) AS ms
  FROM notifications FORCE INDEX (finished_idx)
  WHERE finished_at >= ? AND status = 'sent'
), ranked AS (
  SELECT channel, ms,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ms) AS rn,
    COUNT(*) OVER (PARTITION BY channel) AS n
  FROM sent
)
SELECT CAST(channel AS CHAR), CAST(ms AS SIGNED), rn, n FROM ranked
WHERE rn IN (CEIL(0.5 * n), CEIL(0.95 * n), CEIL(0.99 * n))`
)

// rawDelivery computes the per-channel delivery numbers the old way.
func rawDelivery(t testing.TB, db *sql.DB, since time.Time) map[notification.Channel]store.Delivery {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	out := map[notification.Channel]store.Delivery{}
	each := func(query string, scan func(*sql.Rows) error) {
		rows, err := tx.QueryContext(t.Context(), query, since)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			if err := scan(rows); err != nil {
				t.Fatal(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	each(rawFinishedSQL, func(r *sql.Rows) error {
		var ch string
		var sent, failed int
		err := r.Scan(&ch, &sent, &failed)
		d := out[notification.Channel(ch)]
		d.Sent, d.Failed = sent, failed
		out[notification.Channel(ch)] = d
		return err
	})
	each(rawAttemptsSQL, func(r *sql.Rows) error {
		var ch string
		var a, f, u int
		err := r.Scan(&ch, &a, &f, &u)
		d := out[notification.Channel(ch)]
		d.Attempts, d.FailedAttempts, d.UnknownAttempts = a, f, u
		out[notification.Channel(ch)] = d
		return err
	})
	each(rawLatencySQL, func(r *sql.Rows) error {
		var ch string
		var ms, rn, n int64
		err := r.Scan(&ch, &ms, &rn, &n)
		d := out[notification.Channel(ch)]
		rank := func(p int64) int64 { return (p*n + 99) / 100 }
		if rn == rank(50) {
			d.Latency.P50 = int(ms)
		}
		if rn == rank(95) {
			d.Latency.P95 = int(ms)
		}
		if rn == rank(99) {
			d.Latency.P99 = int(ms)
		}
		out[notification.Channel(ch)] = d
		return err
	})
	return out
}

// seedHour loads n notifications finished evenly over the hour before now,
// each with one attempt: 95% accepted and sent, 3% refused and failed, 2%
// transient and back to pending. The rules of decision 0005 (status_from
// triggers and CHECKs) are switched off first, as in TestMeasureRulesLatency,
// so rows go in in their final state in bulk; the rollup triggers count them
// on insert. Latency: 97% aligned sends of 205 to 510 ms (the 200 ms
// alignment, a 0 to 250 ms poll, a 5 to 60 ms call), 3% a tail of up to 5
// minutes (retry delays up to APP_RETRY_CAP); drawn from CRC32 of the row
// number, so every run loads the same values.
func seedHour(t testing.TB, db *sql.DB, n int) {
	t.Helper()
	ctx := t.Context()
	_, drop := ruleStatements(t)
	execEach(t, db, drop)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	exec := func(query string, args ...any) int64 {
		t.Helper()
		res, err := conn.ExecContext(ctx, query, args...)
		if err != nil {
			t.Fatalf("%.80s: %v", query, err)
		}
		k, _ := res.RowsAffected()
		return k
	}
	exec(`SET SESSION cte_max_recursion_depth = 1000000`)
	exec(`SET @t0 = NOW(3) - INTERVAL 1 HOUR, @step_us = 3600000000 / ?`, n)
	const chunk = 50_000
	st := time.Now()
	for off := 0; off < n; off += chunk {
		exec(fmt.Sprintf(`INSERT INTO notifications (id, recipient, channel, priority, content, status, failure_kind, attempts,
			  due_at, provider_message_id, correlation_id, created_at, finished_at)
			WITH RECURSIVE seq (i) AS (SELECT %d UNION ALL SELECT i + 1 FROM seq WHERE i < %d),
			drawn AS (SELECT i, CRC32(CONCAT(i, 'o')) %% 100 AS o,
			  CASE WHEN CRC32(i) %% 100 < 97 THEN 205 + CRC32(CONCAT(i, 'p')) %% 250 + CRC32(CONCAT(i, 'c')) %% 56
			    ELSE CRC32(CONCAT(i, 't')) %% 300000 END AS ms,
			  @t0 + INTERVAL i * @step_us MICROSECOND AS done FROM seq)
			SELECT UUID(), ELT(i %% 3 + 1, '+905551234567', 'a@example.com', 'device-1'), ELT(i %% 3 + 1, 'sms', 'email', 'push'), 'normal', 'hello',
			  CASE WHEN o < 95 THEN 'sent' WHEN o < 98 THEN 'failed' ELSE 'pending' END,
			  CASE WHEN o BETWEEN 95 AND 97 THEN 'permanent' END, 1,
			  CASE WHEN o < 98 THEN done ELSE NOW(3) + INTERVAL 1 HOUR END,
			  CASE WHEN o < 95 THEN 'm' END, 'seed', done - INTERVAL ms * 1000 MICROSECOND,
			  CASE WHEN o < 98 THEN done END
			FROM drawn`, off, min(off+chunk, n)-1))
	}
	t.Logf("seed: %d notifications in %s", n, time.Since(st).Round(time.Second))
	st = time.Now()
	last := ""
	for {
		var next sql.NullString
		if err := conn.QueryRowContext(ctx, `SELECT MAX(id) FROM (SELECT id FROM notifications WHERE id > ? AND correlation_id = 'seed' ORDER BY id LIMIT ?) AS c`, last, chunk).Scan(&next); err != nil {
			t.Fatal(err)
		}
		if !next.Valid {
			break
		}
		exec(`INSERT INTO delivery_attempts (notification_id, attempt_no, channel, started_at, duration_ms, outcome, claim_version, http_status, error, provider_message_id)
			SELECT id, 1, channel, COALESCE(finished_at, created_at + INTERVAL 300 * 1000 MICROSECOND) - INTERVAL 20 * 1000 MICROSECOND, 20,
			  CASE status WHEN 'sent' THEN 'accepted' WHEN 'failed' THEN 'permanent' ELSE 'transient' END, 1,
			  CASE status WHEN 'sent' THEN 202 WHEN 'failed' THEN 400 ELSE 503 END,
			  CASE WHEN status <> 'sent' THEN 'seeded failure' END, provider_message_id
			FROM notifications WHERE id > ? AND id <= ? AND correlation_id = 'seed'`, last, next.String)
		last = next.String
	}
	t.Logf("seed: attempts in %s", time.Since(st).Round(time.Second))
}

func median(ds []time.Duration) time.Duration {
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[len(s)/2]
}

// metricsDSN points TestMeasureMetricsAtScale at a database that outlives the
// test (seeded once, measured many times) instead of a fresh container.
var metricsDSN = flag.String("metrics-dsn", "", "MySQL DSN (root) for TestMeasureMetricsAtScale; seeded when empty of seed rows")

// TestMeasureMetricsAtScale: the metrics read over 1,000,000 attempts in one
// hour, before (raw rows) and after (rollups), on the same data; and the
// bucket percentiles against the exact ones. Windows end at the last seeded
// finish ("virtual now"), so a seed of a quarter hour does not age the data:
// both reads count everything after the window's start, and nothing is after
// that end.
//
//	go test -tags integration ./internal/store -run TestMeasureMetricsAtScale -measure -v -timeout 60m [-metrics-dsn root:pw@tcp(127.0.0.1:port)/app?parseTime=true]
func TestMeasureMetricsAtScale(t *testing.T) {
	if !*measure {
		t.Skip("pass -measure")
	}
	dsn := *metricsDSN
	if dsn == "" {
		_, dsn = support.MySQL(t)
	}
	root := rootDB(t, dsn)
	db, err := store.Open(t.Context(), dsn, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := store.Migrate(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	const n = 1_000_000
	var seeded int
	if err := root.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM notifications WHERE correlation_id = 'seed'`).Scan(&seeded); err != nil {
		t.Fatal(err)
	}
	if seeded == 0 {
		st := time.Now()
		seedHour(t, root, n)
		t.Logf("seeded %d notifications and attempts in %s", n, time.Since(st).Round(time.Second))
		if _, err := root.ExecContext(t.Context(), `ANALYZE TABLE notifications, delivery_attempts, attempts_per_minute, finishes_per_minute`); err != nil {
			t.Fatal(err)
		}
	}
	var rollupRows int
	var end time.Time
	if err := root.QueryRowContext(t.Context(), `SELECT (SELECT COUNT(*) FROM attempts_per_minute) + (SELECT COUNT(*) FROM finishes_per_minute),
		(SELECT MAX(started_at) FROM delivery_attempts)`).Scan(&rollupRows, &end); err != nil {
		t.Fatal(err)
	}
	t.Logf("rollup rows: %d; virtual now (last attempt start): %s", rollupRows, end.Format(time.RFC3339Nano))
	// The retention pass folds each minute's shards into shard 0 two minutes
	// after it starts; fold the seeded hour as it would have (all of it: the
	// seed ended before the measurement starts, so the last two minutes are
	// folded too, which spares a read at most two minutes of shards).
	st := time.Now()
	if err := store.FoldRollups(t.Context(), db, end.Add(-2*time.Hour), time.Now().UTC().Add(-2*time.Minute).Truncate(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := root.QueryRowContext(t.Context(), `SELECT (SELECT COUNT(*) FROM attempts_per_minute) + (SELECT COUNT(*) FROM finishes_per_minute)`).Scan(&rollupRows); err != nil {
		t.Fatal(err)
	}
	t.Logf("rollup rows after folding the shards: %d (fold took %s)", rollupRows, time.Since(st).Round(time.Millisecond))

	qs := metricsQueries(t)
	for _, window := range []time.Duration{time.Hour, time.Minute} {
		since := end.Add(-window)
		edge := since.Truncate(time.Minute).Add(time.Minute)
		const runs = 5
		var rawT, newT []time.Duration
		var raw, rolled map[notification.Channel]store.Delivery
		for range runs {
			s := time.Now()
			raw = rawDelivery(t, root, since) // root: the raw read outlives the pool's read timeout
			rawT = append(rawT, time.Since(s))
			s = time.Now()
			rolled, err = store.DeliverySince(t.Context(), db, since)
			newT = append(newT, time.Since(s))
			if err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("window %s (since %s): delivery before (raw rows) median %s %v; after (rollups) median %s %v",
			window, since.Format("15:04:05.000"), median(rawT), rawT, median(newT), newT)
		for _, part := range []struct {
			name string
			args []any
		}{
			{"MetricsAttemptRollups", []any{edge}},
			{"MetricsFinishRollups", []any{edge}},
			{"MetricsAttemptsBetween", []any{since, edge}},
			{"MetricsFinishesBetween", []any{since, edge}},
		} {
			var ts []time.Duration
			for range runs {
				ts = append(ts, timeQuery(t, db, qs[part.name], part.args...))
			}
			var plan string
			if err := root.QueryRowContext(t.Context(), "EXPLAIN ANALYZE "+qs[part.name], part.args...).Scan(&plan); err != nil {
				t.Fatal(err)
			}
			t.Logf("  %s: median %s %v\n%s", part.name, median(ts), ts, plan)
		}
		for _, ch := range notification.Channels {
			r, g := raw[ch], rolled[ch]
			t.Logf("  %s: raw %+v | rollups %+v", ch, r, g)
			if r.Sent != g.Sent || r.Failed != g.Failed || r.Attempts != g.Attempts || r.FailedAttempts != g.FailedAttempts || r.UnknownAttempts != g.UnknownAttempts {
				t.Errorf("%s counts differ: raw %+v, rollups %+v", ch, r, g)
			}
			for _, p := range [][2]int{{r.Latency.P50, g.Latency.P50}, {r.Latency.P95, g.Latency.P95}, {r.Latency.P99, g.Latency.P99}} {
				if store.LatencyBucket(int64(p[1])) != store.LatencyBucket(int64(p[0])) {
					t.Errorf("%s: estimate %d not in the bucket of the exact %d", ch, p[1], p[0])
				}
			}
		}
	}
	// The hour window by where its start falls in the minute: the raw read is
	// longest just under and at half a minute (30 s of rows). It starts in the
	// minute after the one an hour back (59 whole minutes and the partial one),
	// because the seed thins out in its first minutes and would understate the
	// raw half.
	hourStart := end.Add(-time.Hour).Truncate(time.Minute).Add(time.Minute)
	for _, off := range []time.Duration{0, 15 * time.Second, 29_999 * time.Millisecond, 30 * time.Second, 45 * time.Second} {
		var ts []time.Duration
		for range 5 {
			s := time.Now()
			if _, err := store.DeliverySince(t.Context(), db, hourStart.Add(off)); err != nil {
				t.Fatal(err)
			}
			ts = append(ts, time.Since(s))
		}
		t.Logf("hour window starting %s into its minute: after (rollups) median %s %v", off, median(ts), ts)
	}
	// The rest of Read, statement by statement, in one read-only snapshot as Read runs them.
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UTC()
	for _, part := range []struct {
		name string
		args []any
	}{
		{"MetricsQueue", []any{now, now, now, now}},
		{"MetricsStatusCounts", nil},
		{"MetricsScheduled", []any{now}},
	} {
		var ts []time.Duration
		for range 3 {
			ts = append(ts, timeQuery(t, tx, qs[part.name], part.args...))
		}
		var plan string
		if err := root.QueryRowContext(t.Context(), "EXPLAIN ANALYZE "+qs[part.name], part.args...).Scan(&plan); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: median %s %v\n%s", part.name, median(ts), ts, plan)
	}
	var whole []time.Duration
	for range 3 {
		s := time.Now()
		if _, err := store.NewMetricsReader(db).Read(t.Context(), time.Hour); err != nil {
			t.Fatal(err)
		}
		whole = append(whole, time.Since(s))
	}
	t.Logf("whole Read (1 h window ending at the real now): median %s %v", median(whole), whole)
}

// rollupTriggerStatements reads the rollup trigger migration and returns the
// statements that create the triggers and those that drop them.
func rollupTriggerStatements(t testing.TB) (add, drop []string) {
	t.Helper()
	names, err := fs.Glob(migrations.FS, "*_delivery_rollups.sql")
	if err != nil || len(names) != 1 {
		t.Fatalf("find the rollup trigger migration: %v %v", names, err)
	}
	b, err := fs.ReadFile(migrations.FS, names[0])
	if err != nil {
		t.Fatal(err)
	}
	up, _, _ := strings.Cut(string(b), "-- +goose Down")
	var trig strings.Builder
	in := false
	for line := range strings.Lines(up) {
		switch {
		case strings.HasPrefix(line, "-- +goose StatementBegin"):
			in = true
			trig.Reset()
		case strings.HasPrefix(line, "-- +goose StatementEnd"):
			in = false
			add = append(add, trig.String())
			name := regexp.MustCompile("(?i)CREATE TRIGGER `?([A-Za-z0-9_]+)`?").FindStringSubmatch(trig.String())
			drop = append(drop, "DROP TRIGGER "+name[1])
		case in:
			trig.WriteString(line)
		}
	}
	return add, drop
}

// seedChannels inserts per pending notifications due now on every channel.
func seedChannels(t testing.TB, db *sql.DB, per int) {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(t.Context(), `SET SESSION cte_max_recursion_depth = 1000000`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(t.Context(), fmt.Sprintf(`INSERT INTO notifications (id, recipient, channel, priority, content, due_at, correlation_id)
		WITH RECURSIVE seq (i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM seq WHERE i < %d)
		SELECT UUID(), ELT(i %% 3 + 1, '+905551234567', 'a@example.com', 'device-1'), ELT(i %% 3 + 1, 'sms', 'email', 'push'),
		  'normal', 'hello', NOW(3) - INTERVAL 1 SECOND, 'load' FROM seq`, 3*per-1)); err != nil {
		t.Fatal(err)
	}
}

type lockStats struct{ waits, timeMs, deadlocks int64 }

func readLockStats(t testing.TB, root *sql.DB) lockStats {
	t.Helper()
	var s lockStats
	var name string
	if err := root.QueryRowContext(t.Context(), `SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits'`).Scan(&name, &s.waits); err != nil {
		t.Fatal(err)
	}
	if err := root.QueryRowContext(t.Context(), `SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_time'`).Scan(&name, &s.timeMs); err != nil {
		t.Fatal(err)
	}
	if err := root.QueryRowContext(t.Context(), `SELECT COUNT FROM information_schema.INNODB_METRICS WHERE NAME = 'lock_deadlocks'`).Scan(&s.deadlocks); err != nil {
		t.Fatal(err)
	}
	return s
}

// pacedLoad runs workers processes, each with one loop per channel that
// claims, begins and marks sent one notification every workers/rate seconds,
// so each channel finishes rate per second from all processes together (the
// deployed 100 per second less 5% headroom = 95). It returns the MarkSent
// latencies and the finishes done.
func pacedLoad(t testing.TB, q *store.Queue, workers, rate int, d time.Duration) ([]time.Duration, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	var mu sync.Mutex
	var finishes []time.Duration
	var wg sync.WaitGroup
	for w := range workers {
		for _, ch := range notification.Channels {
			wg.Go(func() {
				every := time.Duration(workers) * time.Second / time.Duration(rate)
				// Spread the processes over one interval, as independent clocks would be.
				tick := time.NewTicker(every)
				defer tick.Stop()
				time.Sleep(every * time.Duration(w) / time.Duration(workers))
				var f []time.Duration
				for {
					select {
					case <-ctx.Done():
						mu.Lock()
						finishes = append(finishes, f...)
						mu.Unlock()
						return
					case <-tick.C:
					}
					jobs, err := q.Claim(ctx, ch, notification.PriorityNormal, 1, 30*time.Second)
					if err != nil || len(jobs) == 0 {
						continue
					}
					wctx := context.WithoutCancel(ctx)
					st := time.Now()
					a := notification.Attempt{StartedAt: st, Duration: time.Millisecond, Outcome: notification.OutcomeAccepted, HTTPStatus: 202, ProviderMessageID: "m"}
					fin := time.Now()
					if err := q.MarkSent(wctx, jobs[0], a); err != nil {
						t.Errorf("mark sent: %v", err)
						continue
					}
					f = append(f, time.Since(fin))
				}
			})
		}
	}
	wg.Wait()
	return finishes, len(finishes)
}

// TestMeasureRollupWriteCost: what the rollup triggers cost the result write,
// with them on and off in turn on one database: paced at the deployed rate
// (three worker processes, 95 per second per channel), the row lock waits
// that a hot rollup row would show; and flat out (eight workers on one
// channel), the throughput.
//
//	go test -tags integration ./internal/store -run TestMeasureRollupWriteCost -measure -v -timeout 30m
func TestMeasureRollupWriteCost(t *testing.T) {
	if !*measure {
		t.Skip("pass -measure")
	}
	_, dsn := support.MySQL(t)
	root := rootDB(t, dsn)
	db, err := store.Open(t.Context(), dsn, 32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	q := store.NewQueue(db)
	add, drop := rollupTriggerStatements(t)
	const workers, rate, window = 3, 95, 20 * time.Second
	seedChannels(t, root, 6*rate*int(window/time.Second))
	seedPending(t, root, 200_000)
	pacedLoad(t, q, workers, rate, 3*time.Second) // warm up
	on := true
	for round := range 4 {
		state := map[bool]string{true: "triggers on ", false: "triggers off"}[on]
		before := readLockStats(t, root)
		finishes, done := pacedLoad(t, q, workers, rate, window)
		after := readLockStats(t, root)
		t.Logf("paced round %d %s: %.1f finishes/s per channel; MarkSent %s; row lock waits %d (%d ms), deadlocks %d",
			round+1, state, float64(done)/3/window.Seconds(), pct(finishes),
			after.waits-before.waits, after.timeMs-before.timeMs, after.deadlocks-before.deadlocks)
		before = readLockStats(t, root)
		_, flat := deliveryLoad(t, q, 8, 10*time.Second)
		after = readLockStats(t, root)
		t.Logf("flat-out round %d %s: %.0f finishes/s on one channel; MarkSent %s; row lock waits %d (%d ms), deadlocks %d",
			round+1, state, float64(len(flat))/10, pct(flat), after.waits-before.waits, after.timeMs-before.timeMs, after.deadlocks-before.deadlocks)
		if on {
			execEach(t, root, drop)
		} else {
			execEach(t, root, add)
		}
		on = !on
	}
}

// timeQuery runs query and reads every row it returns, failing on any error,
// and reports how long that took.
func timeQuery(t *testing.T, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, args ...any) time.Duration {
	t.Helper()
	s := time.Now()
	rows, err := q.QueryContext(t.Context(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() { //nolint:revive // the rows are only read, to time the query
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return time.Since(s)
}
