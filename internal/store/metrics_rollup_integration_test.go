//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
	"github.com/LeventAksakal/notification-system/test/support"
)

// bucketExpr is the latency bucket expression of the rollup triggers, over a
// column ms, and how many times schema/notifications.sql holds it.
func bucketExpr(t *testing.T) (string, int) {
	t.Helper()
	schema, err := os.ReadFile("../../schema/notifications.sql")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?s)CASE WHEN NEW\.status <> 'sent' OR ms <= 0 THEN 0\s+ELSE (.*?) END`).FindStringSubmatch(string(schema))
	if m == nil {
		t.Fatal("bucket expression not found in the rollup triggers")
	}
	return m[1], strings.Count(string(schema), m[1])
}

// The bucket expression of the rollup triggers is the Go bucket function that
// buckets the raw partial minute, for every millisecond up to 70 s and at every
// power-of-two edge up to 2^31 ms.
func TestLatencyBucketExpressionAgreesWithGo(t *testing.T) {
	expr, held := bucketExpr(t)
	if held != 4 {
		t.Errorf("the rollup triggers hold the bucket expression %d times, want 4 (insert; update: old, old again after a fold, new)", held)
	}
	db, _ := support.MySQL(t)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(t.Context(), `SET SESSION cte_max_recursion_depth = 100000`); err != nil {
		t.Fatal(err)
	}
	var edges []string
	for k := range 32 {
		edges = append(edges, fmt.Sprintf("SELECT %d - 1 UNION ALL SELECT %d UNION ALL SELECT %d + 1", int64(1)<<k, int64(1)<<k, int64(1)<<k))
	}
	// G202: the bucket expression read from schema/notifications.sql and fixed edge literals, no input.
	//nolint:gosec
	rows, err := conn.QueryContext(t.Context(),
		`WITH RECURSIVE seq (ms) AS (SELECT -2 UNION ALL SELECT ms + 1 FROM seq WHERE ms < 70000)
		SELECT ms, CAST(CASE WHEN ms <= 0 THEN 0 ELSE `+expr+` END AS SIGNED) FROM (SELECT ms FROM seq UNION ALL `+strings.Join(edges, " UNION ALL ")+`) AS v (ms)`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		var ms, le int64
		if err := rows.Scan(&ms, &le); err != nil {
			t.Fatal(err)
		}
		if want := store.LatencyBucket(ms); le != want {
			t.Errorf("latency %d ms: SQL bucket %d, Go bucket %d", ms, le, want)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n < 70_000 {
		t.Fatalf("compared %d values", n)
	}
}

// rollupCounts reads both rollup tables, shards summed, zero sums dropped.
func rollupCounts(t *testing.T, db *sql.DB) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	scan := func(query string) {
		rows, err := db.QueryContext(t.Context(), query)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var key string
			var n int64
			if err := rows.Scan(&key, &n); err != nil {
				t.Fatal(err)
			}
			out[key] = n
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	scan(`SELECT CONCAT_WS('|', 'attempts', DATE_FORMAT(minute, '%Y-%m-%d %H:%i'), channel, outcome), SUM(n) FROM attempts_per_minute GROUP BY minute, channel, outcome HAVING SUM(n) <> 0`)
	scan(`SELECT CONCAT_WS('|', 'finishes', DATE_FORMAT(minute, '%Y-%m-%d %H:%i'), channel, status, latency_le_ms), SUM(n) FROM finishes_per_minute GROUP BY minute, channel, status, latency_le_ms HAVING SUM(n) <> 0`)
	return out
}

// recount counts the raw rows minute by minute with the endpoint's own
// partial-minute statements: what the rollups must equal.
func recount(t *testing.T, db *sql.DB) map[string]int64 {
	t.Helper()
	qs := metricsQueries(t)
	var minutes []time.Time
	func() {
		rows, err := db.QueryContext(t.Context(), `
		SELECT DISTINCT CAST(DATE_FORMAT(started_at, '%Y-%m-%d %H:%i:00') AS DATETIME) FROM delivery_attempts
		UNION SELECT DISTINCT CAST(DATE_FORMAT(finished_at, '%Y-%m-%d %H:%i:00') AS DATETIME) FROM notifications WHERE finished_at IS NOT NULL
		UNION SELECT minute FROM attempts_per_minute UNION SELECT minute FROM finishes_per_minute`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var m time.Time
			if err := rows.Scan(&m); err != nil {
				t.Fatal(err)
			}
			minutes = append(minutes, m)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}()
	out := map[string]int64{}
	for _, m := range minutes {
		label := m.Format("2006-01-02 15:04")
		count := func(query, kind string, skip func([]string) bool, bucket bool) {
			rows, err := db.QueryContext(t.Context(), query, m, m.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			cols, _ := rows.Columns()
			for rows.Next() {
				vals := make([]sql.NullString, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					t.Fatal(err)
				}
				parts := make([]string, len(cols))
				for i := range parts {
					parts[i] = vals[i].String
				}
				if skip(parts) {
					continue
				}
				if bucket { // channel, status, latency in ms: the rollup holds its bucket
					var ms int64
					_, _ = fmt.Sscan(parts[2], &ms)
					parts[2] = fmt.Sprint(store.LatencyBucket(ms))
				}
				out[strings.Join(append([]string{kind, label}, parts...), "|")]++ // one row per attempt or finish
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
		}
		count(qs["MetricsAttemptsBetween"], "attempts", func(p []string) bool { return p[1] == string(notification.OutcomeInFlight) }, false)
		count(qs["MetricsFinishesBetween"], "finishes", func([]string) bool { return false }, true)
	}
	return out
}

// The rollups equal a recount of the rows they describe after every way an
// outcome is written: sent, retried, failed, replayed, given up as unknown by
// the reaper or a release, a result arriving after the lease was lost, rows
// whose times move, concurrent workers, and retention.
func TestRollupsEqualARecountOfTheirRows(t *testing.T) {
	db, _ := support.MySQL(t)
	ctx := t.Context()
	q := store.NewQueue(db)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	check := func(step string) {
		t.Helper()
		got, want := rollupCounts(t, db), recount(t, db)
		if !maps.Equal(got, want) {
			var diff []string
			for k, n := range got {
				if want[k] != n {
					diff = append(diff, fmt.Sprintf("%s rollups %d recount %d", k, n, want[k]))
				}
			}
			for k, n := range want {
				if _, ok := got[k]; !ok {
					diff = append(diff, fmt.Sprintf("%s rollups 0 recount %d", k, n))
				}
			}
			t.Fatalf("%s: rollups differ from a recount:\n%s", step, strings.Join(diff, "\n"))
		}
		if len(got) == 0 {
			t.Fatalf("%s: nothing counted", step)
		}
	}
	claim := func() notification.Job {
		t.Helper()
		jobs, err := q.Claim(ctx, notification.ChannelPush, notification.PriorityNormal, 1, time.Minute)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("claim: %v %d", err, len(jobs))
		}
		return jobs[0] // the claim wrote its attempt in flight
	}
	accepted := notification.Attempt{StartedAt: time.Now(), Duration: 3 * time.Millisecond, Outcome: notification.OutcomeAccepted, HTTPStatus: 202, ProviderMessageID: "m"}
	transient := notification.Attempt{StartedAt: time.Now(), Duration: 3 * time.Millisecond, Outcome: notification.OutcomeTransient, HTTPStatus: 503, Error: "busy"}
	permanent := notification.Attempt{StartedAt: time.Now(), Duration: 3 * time.Millisecond, Outcome: notification.OutcomePermanent, HTTPStatus: 400, Error: "refused"}

	// Latencies over many buckets: notification i was created i^3 ms ago.
	ids := insertListed(t, db, 40, notification.ChannelPush, notification.StatusPending)
	for i, id := range ids {
		exec(`UPDATE notifications SET created_at = NOW(3) - INTERVAL ? MICROSECOND, due_at = NOW(3) - INTERVAL 1 SECOND WHERE id = ?`, i*i*i*1000, id)
	}
	for range 30 {
		if err := q.MarkSent(ctx, claim(), accepted); err != nil {
			t.Fatal(err)
		}
	}
	check("sent")

	j := claim()
	if err := q.Reschedule(ctx, j, transient, time.Now()); err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE notifications SET due_at = NOW(3) - INTERVAL 1 SECOND WHERE id = ?`, j.ID)
	check("retried")
	var failedID string
	for {
		j := claim()
		if j.Attempts > 0 {
			failedID = j.ID
			if err := q.MarkFailed(ctx, j, permanent, notification.FailurePermanent); err != nil {
				t.Fatal(err)
			}
			break
		}
		if err := q.MarkSent(ctx, j, accepted); err != nil {
			t.Fatal(err)
		}
	}
	check("failed")
	res, err := sqlc.New(db).ReplayNotification(ctx, failedID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("replay: %v %d rows", err, n)
	}
	check("replayed")

	// Pending rows left; the reaper gives an expired lease's attempt up as unknown.
	lost := claim()
	exec(`UPDATE notifications SET lease_until = NOW(3) - INTERVAL 1 SECOND WHERE id = ?`, lost.ID)
	if _, err := q.ReapExpired(ctx); err != nil {
		t.Fatal(err)
	}
	check("reaped as unknown")
	// The result arrives after the lease was lost: the attempt moves from unknown to accepted.
	if err := q.MarkSent(ctx, lost, accepted); !errors.Is(err, notification.ErrLeaseLost) {
		t.Fatalf("late result: want ErrLeaseLost, got %v", err)
	}
	check("late result")
	released := claim()
	if err := q.Release(ctx, released, time.Now()); err != nil {
		t.Fatal(err)
	}
	check("released after the call began")

	// Rows whose times move carry their counts along.
	exec(`UPDATE notifications SET created_at = created_at - INTERVAL 7 SECOND WHERE status = 'sent' ORDER BY id LIMIT 3`)
	exec(`UPDATE delivery_attempts SET started_at = started_at - INTERVAL 3 MINUTE ORDER BY id LIMIT 4`)
	// Two finished two hours ago, their calls started then too (retention deletes them).
	exec(`UPDATE notifications SET finished_at = NOW(3) - INTERVAL 2 HOUR WHERE status = 'sent' ORDER BY id LIMIT 2`)
	exec(`UPDATE delivery_attempts a JOIN notifications n ON n.id = a.notification_id
		SET a.started_at = n.finished_at - INTERVAL 1 SECOND WHERE n.finished_at < NOW(3) - INTERVAL 1 HOUR`)
	check("times moved")

	// Concurrent workers.
	seedPending(t, db, 3000)
	deliveryLoad(t, q, 8, 3*time.Second)
	check("eight concurrent workers")
	mixedLoad(t, db, q, 3, 4*time.Second)
	check("three workers with every outcome, a reaper and late results")

	// Folding the shards into shard 0 keeps every sum, and a change after the
	// fold takes its count out of whichever shard now holds it.
	shards := func() (n int) {
		t.Helper()
		if err := db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM attempts_per_minute WHERE shard > 0) + (SELECT COUNT(*) FROM finishes_per_minute WHERE shard > 0)`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if shards() == 0 {
		t.Fatal("the load wrote no shard but 0")
	}
	now := time.Now().UTC()
	if err := store.FoldRollups(ctx, db, now.Add(-3*time.Hour), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if n := shards(); n != 0 {
		t.Fatalf("%d rollup rows left in shards 1 to 3 after the fold", n)
	}
	check("shards folded")
	// Two folders sweep the minutes being written while three workers write
	// them: a count added between a fold's sum and its delete would be lost.
	foldCtx, stopFolding := context.WithCancel(ctx)
	var folders sync.WaitGroup
	folds := make([]int, 2)
	for i := range folds {
		folders.Go(func() {
			for foldCtx.Err() == nil {
				now := time.Now().UTC()
				err := store.FoldRollups(foldCtx, db, now.Add(-3*time.Hour), now.Add(time.Minute))
				switch {
				case err == nil:
					folds[i]++
				case foldCtx.Err() == nil && !store.IsLockConflict(err):
					t.Errorf("fold during load: %v", err)
				}
			}
		})
	}
	mixedLoad(t, db, q, 3, 3*time.Second)
	stopFolding()
	folders.Wait()
	if folds[0]+folds[1] < 2 {
		t.Fatalf("folds completed during the load: %v", folds)
	}
	check("folded while three workers write, two folders at once")
	exec(`UPDATE delivery_attempts SET started_at = started_at - INTERVAL 2 MINUTE WHERE id % 4 <> 0 AND started_at > NOW(3) - INTERVAL 1 HOUR ORDER BY id DESC LIMIT 8`)
	exec(`UPDATE notifications SET created_at = created_at - INTERVAL 3 SECOND WHERE status = 'sent' AND finished_at > NOW(3) - INTERVAL 1 HOUR ORDER BY id DESC LIMIT 8`)
	check("changed after the fold")

	// Retention deletes rows and rollup minutes at the same minute bound: the
	// minute the retention reaches into keeps its rows on both sides of
	// now - keep, and its rollup.
	exec(`UPDATE notifications SET finished_at = NOW(3) - INTERVAL 1 HOUR - INTERVAL (SECOND(NOW(3) - INTERVAL 1 HOUR) * 1000000 + MICROSECOND(NOW(3) - INTERVAL 1 HOUR)) MICROSECOND + INTERVAL 1 MICROSECOND
		WHERE status = 'sent' AND finished_at > NOW(3) - INTERVAL 1 HOUR ORDER BY id LIMIT 2`)
	exec(`UPDATE delivery_attempts a JOIN notifications n ON n.id = a.notification_id
		SET a.started_at = n.finished_at WHERE n.finished_at < NOW(3) - INTERVAL 59 MINUTE AND n.finished_at > NOW(3) - INTERVAL 61 MINUTE`)
	if _, err := store.SweepFinished(ctx, db, time.Hour); err != nil {
		t.Fatal(err)
	}
	check("after retention")
	var old, edge int
	if err := db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM finishes_per_minute WHERE minute < NOW() - INTERVAL 61 MINUTE),
		(SELECT COUNT(*) FROM notifications WHERE finished_at < NOW(3) - INTERVAL 1 HOUR)`).Scan(&old, &edge); err != nil || old != 0 {
		t.Fatalf("rollups past the retention's minute: %d %v", old, err)
	}
	if edge == 0 {
		t.Fatal("no row of the retention's own minute was kept past now - keep")
	}
}

// A window that does not start on a whole minute counts its partial first
// minute from the raw rows (the shorter side of its start: added after it,
// or subtracted before it) and the rest from the rollups, each outcome once.
func TestMetricsWindowSplitsAtTheMinute(t *testing.T) {
	db, _ := support.MySQL(t)
	ctx := t.Context()
	q := store.NewQueue(db)
	var now time.Time
	if err := db.QueryRowContext(ctx, `SELECT NOW(3)`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	base := now.Truncate(time.Minute).Add(-10 * time.Minute)
	at := []time.Duration{29_999 * time.Millisecond, 30 * time.Second, 59_999 * time.Millisecond, time.Minute, 5 * time.Minute}
	seedPending(t, db, len(at))
	deliveryLoad(t, q, 1, time.Second)
	var ids []string
	func() {
		rows, err := db.QueryContext(ctx, `SELECT id FROM notifications WHERE status = 'sent' ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}()
	if len(ids) != len(at) {
		t.Fatalf("sent %d, want %d", len(ids), len(at))
	}
	for i, id := range ids {
		ts := base.Add(at[i])
		// Latency 100 ms each; the attempt started at the same moment.
		if _, err := db.ExecContext(ctx, `UPDATE notifications SET created_at = ?, scheduled_at = NULL, finished_at = ? WHERE id = ?`,
			ts.Add(-100*time.Millisecond), ts, id); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE delivery_attempts SET started_at = ? WHERE notification_id = ?`, ts, id); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name  string
		since time.Duration
		want  int
	}{
		{"on a whole minute", 0, 5},
		{"a millisecond in: the rows before since are taken from the minute", time.Millisecond, 5},
		{"just under half a minute, at a row's time", 29_999 * time.Millisecond, 5},
		{"just under half a minute, a millisecond after a row", 29_999*time.Millisecond + 500*time.Microsecond, 4},
		{"half a minute in, at a row's time", 30 * time.Second, 4},
		{"a millisecond after a row", 30*time.Second + time.Millisecond, 3},
		{"the next whole minute", time.Minute, 2},
		{"a millisecond before the minute", 59_999 * time.Millisecond, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.DeliverySince(ctx, db, base.Add(tc.since))
			if err != nil {
				t.Fatal(err)
			}
			d := got[notification.ChannelPush]
			if d.Sent != tc.want || d.Attempts != tc.want {
				t.Errorf("since base+%s: sent %d, attempts %d, want %d", tc.since, d.Sent, d.Attempts, tc.want)
			}
			// Every latency is 100 ms: the estimate lies in 100's bucket.
			if store.LatencyBucket(int64(d.Latency.P50)) != store.LatencyBucket(100) || store.LatencyBucket(int64(d.Latency.P99)) != store.LatencyBucket(100) {
				t.Errorf("latency %+v, want within the bucket of 100 ms", d.Latency)
			}
		})
	}
}

// mixedLoad runs workers that claim push notifications and end them every
// way in turn (sent, retried, failed, released, or left to expire and then
// answered late), while a reaper gives expired leases up as unknown, so the
// same per-minute rows take increments and decrements concurrently.
func mixedLoad(t *testing.T, db *sql.DB, q *store.Queue, workers int, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	accepted := notification.Attempt{Duration: 3 * time.Millisecond, Outcome: notification.OutcomeAccepted, HTTPStatus: 202, ProviderMessageID: "m"}
	transient := notification.Attempt{Duration: 3 * time.Millisecond, Outcome: notification.OutcomeTransient, HTTPStatus: 503, Error: "busy"}
	permanent := notification.Attempt{Duration: 3 * time.Millisecond, Outcome: notification.OutcomePermanent, HTTPStatus: 400, Error: "refused"}
	var wg sync.WaitGroup
	wg.Go(func() {
		for ctx.Err() == nil {
			if _, err := q.ReapExpired(context.WithoutCancel(ctx)); err != nil {
				t.Errorf("reap: %v", err)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
	var mu sync.Mutex
	ends := map[string]int{}
	for w := range workers {
		wg.Go(func() {
			i := w
			for ctx.Err() == nil {
				jobs, err := q.Claim(ctx, notification.ChannelPush, notification.PriorityNormal, 5, time.Minute)
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				for _, j := range jobs {
					wctx := context.WithoutCancel(ctx)
					now := time.Now()
					var err error
					var how string
					switch i % 5 {
					case 0:
						how, err = "sent", q.MarkSent(wctx, j, withStart(accepted, now))
					case 1:
						how, err = "retried", q.Reschedule(wctx, j, withStart(transient, now), now)
					case 2:
						how, err = "failed", q.MarkFailed(wctx, j, withStart(permanent, now), notification.FailurePermanent)
					case 3:
						how, err = "released", q.Release(wctx, j, now)
					case 4:
						// The lease runs out; the reaper may turn the call unknown before the result lands.
						if _, err := db.ExecContext(wctx, `UPDATE notifications SET lease_until = NOW(3) - INTERVAL 1 SECOND WHERE id = ? AND claim_version = ?`, j.ID, j.ClaimVersion); err != nil {
							t.Errorf("expire: %v", err)
							return
						}
						time.Sleep(25 * time.Millisecond)
						how, err = "late", q.MarkSent(wctx, j, withStart(accepted, now))
					}
					if err != nil && !errors.Is(err, notification.ErrLeaseLost) {
						t.Errorf("%s: %v", how, err)
						return
					}
					mu.Lock()
					ends[how]++
					mu.Unlock()
					i++
				}
			}
		})
	}
	wg.Wait()
	t.Logf("mixed load: %v", ends)
}

func withStart(a notification.Attempt, at time.Time) notification.Attempt {
	a.StartedAt = at
	return a
}
