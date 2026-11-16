//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/migrations"
	"github.com/LeventAksakal/notification-system/test/support"
)

// measure turns on the measurements below; they take minutes and only log
// numbers (decision 0005 "As built", decision 0025):
//
//	go test -tags integration ./internal/store -run TestMeasure -measure -v -timeout 30m
var measure = flag.Bool("measure", false, "run the latency measurements (minutes)")

// pct formats p50, p99 and the count of ds.
func pct(ds []time.Duration) string {
	if len(ds) == 0 {
		return "n=0"
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	at := func(p float64) time.Duration { return s[min(len(s)-1, int(float64(len(s))*p))] }
	return fmt.Sprintf("n=%d p50=%s p99=%s", len(s), at(0.50).Round(10*time.Microsecond), at(0.99).Round(10*time.Microsecond))
}

func p99(ds []time.Duration) time.Duration {
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[min(len(s)-1, int(float64(len(s))*0.99))]
}

// deliveryLoad runs workers that claim push notifications and mark them sent,
// as the worker does, for d; it returns every Claim and MarkSent latency.
func deliveryLoad(t testing.TB, q *store.Queue, workers int, d time.Duration) (claims, finishes []time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	return runLoad(t, ctx, q, workers)
}

// deliveryLoadUntil is deliveryLoad until ctx ends; it returns the Claim latencies.
func deliveryLoadUntil(t testing.TB, ctx context.Context, q *store.Queue, workers int) []time.Duration {
	claims, _ := runLoad(t, ctx, q, workers)
	return claims
}

func runLoad(t testing.TB, ctx context.Context, q *store.Queue, workers int) (claims, finishes []time.Duration) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			var c, f []time.Duration
			for ctx.Err() == nil {
				st := time.Now()
				jobs, err := q.Claim(ctx, notification.ChannelPush, notification.PriorityNormal, 10, 30*time.Second)
				if ctx.Err() != nil {
					break
				}
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				c = append(c, time.Since(st))
				for _, j := range jobs {
					st := time.Now()
					a := notification.Attempt{StartedAt: st, Duration: time.Millisecond, Outcome: notification.OutcomeAccepted, HTTPStatus: 202, ProviderMessageID: "m"}
					// The write outlives the load window, as the worker's does.
					if err := q.MarkSent(context.WithoutCancel(ctx), j, a); err != nil {
						t.Errorf("mark sent: %v", err)
						return
					}
					f = append(f, time.Since(st))
				}
			}
			mu.Lock()
			claims, finishes = append(claims, c...), append(finishes, f...)
			mu.Unlock()
		})
	}
	wg.Wait()
	return claims, finishes
}

// seedPending inserts n pending push notifications due now, in one statement.
func seedPending(t testing.TB, db *sql.DB, n int) {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(t.Context(), `SET SESSION cte_max_recursion_depth = 1000000`); err != nil {
		t.Fatalf("seed pending: %v", err)
	}
	for left := n; left > 0; left -= 20_000 {
		q := fmt.Sprintf(`INSERT INTO notifications (id, recipient, channel, priority, content, due_at, correlation_id)
		 WITH RECURSIVE seq (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < %d)
		 SELECT UUID(), 'device-1', 'push', 'normal', 'hello', NOW(3) - INTERVAL 1 SECOND, 'load' FROM seq`, min(left, 20_000))
		if _, err := conn.ExecContext(t.Context(), q); err != nil {
			t.Fatalf("seed pending: %v", err)
		}
	}
}

// ruleStatements reads this branch's migration and returns the statements
// that add its triggers and CHECKs and those that drop them.
func ruleStatements(t testing.TB) (add, drop []string) {
	t.Helper()
	names, err := fs.Glob(migrations.FS, "*_db_rules_retention.sql")
	if err != nil || len(names) != 1 {
		t.Fatalf("find the rules migration: %v %v", names, err)
	}
	b, err := fs.ReadFile(migrations.FS, names[0])
	if err != nil {
		t.Fatalf("read %s: %v", names[0], err)
	}
	up, down, _ := strings.Cut(string(b), "-- +goose Down")
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
		case in:
			trig.WriteString(line)
		case strings.Contains(line, "ADD CONSTRAINT"):
			add = append(add, strings.TrimSpace(line))
		}
	}
	for line := range strings.Lines(down) {
		if strings.HasPrefix(line, "DROP TRIGGER") || strings.Contains(line, "DROP CONSTRAINT") {
			drop = append(drop, strings.TrimSpace(line))
		}
	}
	return add, drop
}

func execEach(t testing.TB, db *sql.DB, stmts []string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.ExecContext(t.Context(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// rootDB opens the test container's database as root (performance_schema,
// DDL, bulk seeding), with reads allowed to take minutes.
func rootDB(t testing.TB, dsn string) *sql.DB {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.User = "root"
	cfg.ReadTimeout, cfg.WriteTimeout = 10*time.Minute, 10*time.Minute
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMeasureRulesLatency(t *testing.T) {
	if !*measure {
		t.Skip("pass -measure")
	}
	_, dsn := support.MySQL(t)
	db, err := store.Open(t.Context(), dsn, 32)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	root := rootDB(t, dsn)
	q := store.NewQueue(db)
	add, drop := ruleStatements(t)
	t.Logf("rules toggled: %d statements on, %d off", len(add), len(drop))

	const workers, window = 8, 10 * time.Second
	seedPending(t, root, 150_000)
	deliveryLoad(t, q, workers, 3*time.Second) // warm the buffer pool
	on := true
	for round := range 4 {
		claims, finishes := deliveryLoad(t, q, workers, window)
		state := "rules on "
		if !on {
			state = "rules off"
		}
		t.Logf("round %d %s: claim %s | finish %s", round+1, state, pct(claims), pct(finishes))
		if on {
			execEach(t, root, drop)
		} else {
			// Without the triggers status_from went stale; NULL is "origin
			// unknown", which the CHECK accepts, as on rows older than the rules.
			execEach(t, root, append([]string{`UPDATE notifications SET status_from = NULL`}, add...))
		}
		on = !on
	}
}

func TestMeasureRetentionUnderLoad(t *testing.T) {
	if !*measure {
		t.Skip("pass -measure")
	}
	_, dsn := support.MySQL(t)
	db, err := store.Open(t.Context(), dsn, 32)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	root := rootDB(t, dsn)
	q := store.NewQueue(db)

	const workers, window, expired = 8, 10 * time.Second, 100_000
	seedPending(t, root, 150_000)
	for range expired / 20_000 {
		seedExpired(t, root, 20_000, 2*time.Hour)
	}
	deliveryLoad(t, q, workers, 3*time.Second)

	base1, _ := deliveryLoad(t, q, workers, window)
	t.Logf("baseline 1: claim %s", pct(base1))

	if _, err := root.ExecContext(t.Context(), `TRUNCATE TABLE performance_schema.events_statements_summary_by_digest`); err != nil {
		t.Fatalf("truncate digests: %v", err)
	}
	// The load runs for the whole sweep: from a second before it starts to its end.
	var swept store.Swept
	var sweepTook time.Duration
	var during []time.Duration
	loadCtx, stopLoad := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() { during = deliveryLoadUntil(t, loadCtx, q, workers) })
	time.Sleep(time.Second)
	st := time.Now()
	swept, err = store.SweepFinished(t.Context(), db, time.Hour)
	sweepTook = time.Since(st)
	stopLoad()
	wg.Wait()
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	t.Logf("during sweep: claim %s; swept %+v in %s", pct(during), swept, sweepTook.Round(time.Millisecond))

	rows, err := root.QueryContext(t.Context(), `SELECT DIGEST_TEXT, COUNT_STAR, AVG_TIMER_WAIT, QUANTILE_99, MAX_TIMER_WAIT
		FROM performance_schema.events_statements_summary_by_digest
		WHERE DIGEST_TEXT LIKE 'DELETE FROM %' OR DIGEST_TEXT LIKE 'SELECT %FROM %notifications% WHERE %finished_at% <%'`)
	if err != nil {
		t.Fatalf("digests: %v", err)
	}
	defer func() { _ = rows.Close() }()
	ms := func(ps int64) string { return fmt.Sprintf("%.1fms", float64(ps)/1e9) }
	for rows.Next() {
		var text string
		var n, avg, q99, maxw int64
		if err := rows.Scan(&text, &n, &avg, &q99, &maxw); err != nil {
			t.Fatalf("scan digest: %v", err)
		}
		t.Logf("statement %.60s: n=%d avg=%s p99=%s max=%s", text, n, ms(avg), ms(q99), ms(maxw))
		if maxw > int64(100*time.Millisecond)*1000 {
			t.Errorf("a retention statement took %s, target under 100ms", ms(maxw))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("digest rows: %v", err)
	}

	base2, _ := deliveryLoad(t, q, workers, window)
	t.Logf("baseline 2: claim %s", pct(base2))
	t.Logf("claim p99: baseline %s / %s, during sweep %s", p99(base1), p99(base2), p99(during))
}
