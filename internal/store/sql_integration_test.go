//go:build integration

package store_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/support"
)

// Database-level tests (decisions 0005, 0025): the statements are the real
// text of internal/store/queries/*.sql, sent to MySQL with database/sql and no
// store code in between, so they test what sqlc compiles, not a copy.

var (
	sqlcArgRE   = regexp.MustCompile(`sqlc\.n?arg\(\w+\)`)
	sqlcSliceRE = regexp.MustCompile(`sqlc\.slice\(\w+\)`)
)

// query returns the statement named name in internal/store/queries, with
// sqlc's named arguments as ? and each sqlc.slice as n placeholders.
func query(t testing.TB, name string, n int) string {
	t.Helper()
	files, err := filepath.Glob("queries/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("list queries: %v", err)
	}
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // G304: the package's own query files
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		_, rest, ok := strings.Cut(string(b), "-- name: "+name+" ")
		if !ok {
			continue
		}
		_, body, _ := strings.Cut(rest, "\n") // drop the ":many" etc.
		body, _, _ = strings.Cut(body, "-- name: ")
		var lines []string
		for line := range strings.Lines(body) {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				lines = append(lines, line)
			}
		}
		q := strings.TrimSuffix(strings.TrimSpace(strings.Join(lines, "")), ";")
		q = sqlcArgRE.ReplaceAllString(q, "?")
		return sqlcSliceRE.ReplaceAllString(q, strings.TrimSuffix(strings.Repeat("?,", max(n, 1)), ","))
	}
	t.Fatalf("no query named %s in queries/", name)
	return ""
}

func TestRetentionStatementsInSQL(t *testing.T) {
	db, _ := support.MySQL(t)
	keepUs := time.Hour.Microseconds()

	t.Run("each retention statement reads through its index", func(t *testing.T) {
		for name, tc := range map[string]struct {
			sql  string
			args []any
			key  string // the index the statement must read through
		}{
			"RetentionCandidates": {query(t, "RetentionCandidates", 0), []any{keepUs, 500}, "finished_idx"},
			"DeleteAttemptsOf":    {query(t, "DeleteAttemptsOf", 2), []any{"a", "b"}, "attempts_notification_idx"},
			"DeleteNotifications": {query(t, "DeleteNotifications", 2), []any{"a", "b"}, "PRIMARY"},
			"SweepBatches":        {query(t, "SweepBatches", 0), []any{keepUs, 1000}, "batches_created_idx"},
		} {
			rows := explainArgs(t, db, tc.sql, tc.args...)
			t.Logf("EXPLAIN %s: %v", name, rows)
			key := rows[0]["key"]
			if name == "DeleteAttemptsOf" && key == "attempts_claim_idx" {
				key = tc.key // also leads with notification_id; either serves the delete
			}
			if key != tc.key || rows[0]["type"] == "ALL" || strings.Contains(rows[0]["Extra"], "filesort") {
				t.Errorf("%s: want a range on %s without a sort, got %v", name, tc.key, rows[0])
			}
		}
	})

	t.Run("the statements delete expired finished rows and their attempts, nothing else", func(t *testing.T) {
		expired := []string{
			finishedAgo(t, db, notification.StatusSent, 2*time.Hour),
			finishedAgo(t, db, notification.StatusFailed, 2*time.Hour),
			finishedAgo(t, db, notification.StatusCancelled, 2*time.Hour),
		}
		for _, id := range expired {
			addAttempt(t, db, id, 1)
		}
		kept := []string{
			finishedAgo(t, db, notification.StatusSent, 30*time.Minute),
			rowIn(t, db, notification.StatusPending),
			rowIn(t, db, notification.StatusProcessing),
		}

		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback() }()
		rows, err := tx.QueryContext(t.Context(), query(t, "RetentionCandidates", 0), keepUs, 500)
		if err != nil {
			t.Fatalf("candidates: %v", err)
		}
		defer func() { _ = rows.Close() }()
		var ids []any
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan: %v", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("candidates: %v", err)
		}
		// Closed before the next statement on the transaction.
		if err := rows.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		// Earlier subtests' rows may be expired too; ours must all be there,
		// and nothing kept.
		for _, id := range expired {
			if !contains(ids, id) {
				t.Errorf("expired %s is not a candidate", id)
			}
		}
		for _, id := range kept {
			if contains(ids, id) {
				t.Errorf("kept %s is a candidate", id)
			}
		}
		if _, err := tx.ExecContext(t.Context(), query(t, "DeleteAttemptsOf", len(ids)), ids...); err != nil {
			t.Fatalf("delete attempts: %v", err)
		}
		if _, err := tx.ExecContext(t.Context(), query(t, "DeleteNotifications", len(ids)), ids...); err != nil {
			t.Fatalf("delete notifications: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		for _, id := range expired {
			if n := count(t, db, `SELECT COUNT(*) FROM notifications WHERE id = ?`, id) +
				count(t, db, `SELECT COUNT(*) FROM delivery_attempts WHERE notification_id = ?`, id); n != 0 {
				t.Errorf("expired %s or its attempts left", id)
			}
		}
		for _, id := range kept {
			if n := count(t, db, `SELECT COUNT(*) FROM notifications WHERE id = ?`, id); n != 1 {
				t.Errorf("kept %s deleted", id)
			}
		}
	})

	t.Run("deleting a notification before its attempts is refused", func(t *testing.T) {
		id := finishedAgo(t, db, notification.StatusSent, 2*time.Hour)
		addAttempt(t, db, id, 1)
		_, err := db.ExecContext(t.Context(), query(t, "DeleteNotifications", 1), id)
		if err == nil || !strings.Contains(err.Error(), "1451") {
			t.Fatalf("want a foreign key refusal (1451), got %v", err)
		}
	})

	t.Run("a row locked by a replay is skipped, not waited for", func(t *testing.T) {
		id := finishedAgo(t, db, notification.StatusFailed, 2*time.Hour)
		replay, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("begin replay: %v", err)
		}
		defer func() { _ = replay.Rollback() }()
		if _, err := replay.ExecContext(t.Context(), query(t, "ReplayNotification", 0), id); err != nil {
			t.Fatalf("replay: %v", err)
		}
		sweep, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("begin sweep: %v", err)
		}
		defer func() { _ = sweep.Rollback() }()
		st := time.Now()
		rows, err := sweep.QueryContext(t.Context(), query(t, "RetentionCandidates", 0), keepUs, 500)
		if err != nil {
			t.Fatalf("candidates: %v", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var got string
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("scan: %v", err)
			}
			if got == id {
				t.Errorf("the row a replay holds was returned")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("candidates: %v", err)
		}
		if took := time.Since(st); took > time.Second {
			t.Errorf("candidates waited %s for the replay's lock", took)
		}
	})
}

func contains(ids []any, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}
