//go:build integration

package store_test

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

// finishedAgo walks a new row to status s and back-dates its finished_at (and
// created_at) by ago. Back-dating changes no status, so every rule allows it.
func finishedAgo(t *testing.T, db *sql.DB, s notification.Status, ago time.Duration) string {
	t.Helper()
	id := rowIn(t, db, s)
	if _, err := db.ExecContext(t.Context(), `UPDATE notifications
		SET finished_at = NOW(3) - INTERVAL ? MICROSECOND, created_at = NOW(3) - INTERVAL ? MICROSECOND WHERE id = ?`,
		ago.Microseconds(), ago.Microseconds()+1000, id); err != nil {
		t.Fatalf("back-date %s: %v", id, err)
	}
	return id
}

// addAttempt records attempt no of id, transient, the way the worker does:
// in flight before the call, the result after it.
func addAttempt(t *testing.T, db *sql.DB, id string, no int) {
	t.Helper()
	attemptIn(t, db, id, no, notification.OutcomeTransient)
}

func count(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// explainArgs returns the rows of EXPLAIN query, by column name.
func explainArgs(t *testing.T, db *sql.DB, query string, args ...any) []map[string]string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "EXPLAIN "+query, args...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer func() { _ = rows.Close() }()
	cols, _ := rows.Columns()
	var out []map[string]string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan explain: %v", err)
		}
		row := map[string]string{}
		for i, c := range cols {
			row[c] = vals[i].String
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("explain rows: %v", err)
	}
	return out
}

func TestRetention(t *testing.T) {
	db, _ := support.MySQL(t)
	const keep = time.Hour

	t.Run("expired finished rows and their attempts go; in-flight, scheduled and recent rows stay", func(t *testing.T) {
		gone := map[string]notification.Status{}
		for _, s := range []notification.Status{notification.StatusSent, notification.StatusFailed, notification.StatusCancelled} {
			id := finishedAgo(t, db, s, keep+time.Minute)
			addAttempt(t, db, id, 1)
			gone[id] = s
		}
		recent := finishedAgo(t, db, notification.StatusFailed, keep-time.Minute)
		addAttempt(t, db, recent, 1)
		pending := rowIn(t, db, notification.StatusPending)
		processing := rowIn(t, db, notification.StatusProcessing)
		addAttempt(t, db, processing, 1)
		scheduled := insertPending(t, db)
		if _, err := db.ExecContext(t.Context(), `UPDATE notifications SET due_at = NOW(3) + INTERVAL 1 DAY, scheduled_at = due_at,
			created_at = NOW(3) - INTERVAL 2 DAY WHERE id = ?`, scheduled); err != nil {
			t.Fatalf("schedule: %v", err)
		}
		if _, err := db.ExecContext(t.Context(), `INSERT INTO templates (name, body) VALUES ('kept', 'hi')`); err != nil {
			t.Fatalf("template: %v", err)
		}

		swept, err := store.SweepFinished(t.Context(), db, keep)
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if swept.Notifications != 3 || swept.Attempts != 3 {
			t.Fatalf("swept %+v, want 3 notifications and 3 attempts", swept)
		}
		for id, s := range gone {
			if n := count(t, db, `SELECT COUNT(*) FROM notifications WHERE id = ?`, id); n != 0 {
				t.Errorf("expired %s notification kept", s)
			}
			if n := count(t, db, `SELECT COUNT(*) FROM delivery_attempts WHERE notification_id = ?`, id); n != 0 {
				t.Errorf("attempts of the expired %s notification kept", s)
			}
		}
		for name, id := range map[string]string{"recent": recent, "pending": pending, "processing": processing, "scheduled": scheduled} {
			if n := count(t, db, `SELECT COUNT(*) FROM notifications WHERE id = ?`, id); n != 1 {
				t.Errorf("%s notification deleted", name)
			}
		}
		if n := count(t, db, `SELECT COUNT(*) FROM delivery_attempts WHERE notification_id IN (?, ?)`, recent, processing); n != 2 {
			t.Errorf("attempts of kept notifications: %d, want 2", n)
		}
		if n := count(t, db, `SELECT COUNT(*) FROM templates WHERE name = 'kept'`); n != 1 {
			t.Errorf("template deleted")
		}
	})

	t.Run("a batch goes with its last notification, not before", func(t *testing.T) {
		newBatch := func(items ...string) string {
			b := uuid.Must(uuid.NewV7()).String()
			if _, err := db.ExecContext(t.Context(), `INSERT INTO batches (id, total, correlation_id, created_at)
				VALUES (?, ?, 'corr', NOW(3) - INTERVAL 2 HOUR)`, b, len(items)); err != nil {
				t.Fatalf("batch: %v", err)
			}
			for i, id := range items {
				if _, err := db.ExecContext(t.Context(), `UPDATE notifications SET batch_id = ?, batch_index = ? WHERE id = ?`, b, i, id); err != nil {
					t.Fatalf("batch item: %v", err)
				}
			}
			return b
		}
		allExpired := newBatch(finishedAgo(t, db, notification.StatusSent, keep+time.Minute), finishedAgo(t, db, notification.StatusCancelled, keep+time.Minute))
		oneLeft := newBatch(finishedAgo(t, db, notification.StatusSent, keep+time.Minute), rowIn(t, db, notification.StatusPending))

		if _, err := store.SweepFinished(t.Context(), db, keep); err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if n := count(t, db, `SELECT COUNT(*) FROM batches WHERE id = ?`, allExpired); n != 0 {
			t.Errorf("batch with no notification left was kept")
		}
		if n := count(t, db, `SELECT COUNT(*) FROM batches WHERE id = ?`, oneLeft); n != 1 {
			t.Errorf("batch with a pending notification was deleted")
		}
	})

	t.Run("two sweepers at once delete every expired row exactly once", func(t *testing.T) {
		seedExpired(t, db, 3000, keep+time.Minute)
		var wg sync.WaitGroup
		results := make([]store.Swept, 2)
		errs := make([]error, 2)
		for i := range 2 {
			wg.Go(func() { results[i], errs[i] = store.SweepFinished(t.Context(), db, keep) })
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("sweeper %d: %v", i, err)
			}
		}
		if got := results[0].Notifications + results[1].Notifications; got != 3000 {
			t.Fatalf("sweepers deleted %d+%d notifications, want 3000", results[0].Notifications, results[1].Notifications)
		}
		if n := count(t, db, `SELECT COUNT(*) FROM notifications WHERE finished_at < NOW(3) - INTERVAL 1 HOUR`); n != 0 {
			t.Fatalf("%d expired notifications left", n)
		}
	})
}

// seedExpired inserts n sent sms notifications finished ago, one attempt each,
// in bulk statements that go through both state machines (pending, processing,
// sent; in_flight, accepted) like any writer.
func seedExpired(t testing.TB, db *sql.DB, n int, ago time.Duration) {
	t.Helper()
	tag := uuid.Must(uuid.NewV7()).String()
	stmts := []string{
		`SET SESSION cte_max_recursion_depth = 1000000`,
		`INSERT INTO notifications (id, recipient, channel, content, due_at, correlation_id)
		 WITH RECURSIVE seq (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < ?)
		 SELECT UUID(), '+905551234567', 'sms', 'hello', NOW(3), ? FROM seq`,
		`UPDATE notifications SET status = 'processing', lease_until = NOW(3) + INTERVAL 1 MINUTE WHERE correlation_id = ?`,
		`UPDATE notifications SET status = 'sent', lease_until = NULL, attempts = 1, finished_at = NOW(3) - INTERVAL ? MICROSECOND,
		 created_at = NOW(3) - INTERVAL ? MICROSECOND WHERE correlation_id = ?`,
		`INSERT INTO delivery_attempts (notification_id, attempt_no, channel, started_at, outcome, claim_version)
		 SELECT id, 1, 'sms', created_at, 'in_flight', 1 FROM notifications WHERE correlation_id = ?`,
		`UPDATE delivery_attempts a JOIN notifications n ON n.id = a.notification_id
		 SET a.outcome = 'accepted', a.duration_ms = 5, a.http_status = 202 WHERE n.correlation_id = ?`,
	}
	args := [][]any{nil, {n, tag}, {tag}, {ago.Microseconds(), ago.Microseconds() + 1000, tag}, {tag}, {tag}}
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for i, q := range stmts {
		if _, err := conn.ExecContext(t.Context(), q, args[i]...); err != nil {
			t.Fatalf("seed step %d: %v", i, err)
		}
	}
}
