//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

// insertDue inserts n pending notifications due now.
func insertDue(t *testing.T, db *sql.DB, n int, ch notification.Channel, p notification.Priority) []string {
	t.Helper()
	to := map[notification.Channel]string{notification.ChannelSMS: "+905551234567", notification.ChannelEmail: "a@example.com", notification.ChannelPush: "device-1"}[ch]
	ids := make([]string, n)
	for i := range n {
		ids[i] = uuid.Must(uuid.NewV7()).String()
		_, err := db.ExecContext(t.Context(), `INSERT INTO notifications (id, recipient, channel, priority, content, due_at, correlation_id)
			VALUES (?, ?, ?, ?, 'hello', NOW(3) - INTERVAL 1 SECOND, 'corr')`, ids[i], to, ch, p)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return ids
}

func TestQueue(t *testing.T) {
	db, _ := support.MySQL(t)
	q := store.NewQueue(db)

	t.Run("the claim query uses claim_idx without a sort or a full scan", func(t *testing.T) {
		insertDue(t, db, 50, notification.ChannelPush, notification.PriorityLow)
		insertDue(t, db, 50, notification.ChannelPush, notification.PriorityHigh)
		rows, err := db.QueryContext(t.Context(), `EXPLAIN SELECT id, batch_id, recipient, content, attempts, claim_version, due_at, correlation_id, traceparent
			FROM notifications
			WHERE status = 'pending' AND channel = 'push' AND priority = 'high' AND due_at <= NOW(3)
			ORDER BY due_at LIMIT 1 FOR UPDATE SKIP LOCKED`)
		if err != nil {
			t.Fatalf("explain: %v", err)
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
				t.Fatalf("scan explain: %v", err)
			}
			got := map[string]string{}
			for i, c := range cols {
				got[c] = vals[i].String
			}
			t.Logf("EXPLAIN: type=%s key=%s extra=%s", got["type"], got["key"], got["Extra"])
			if strings.Contains(got["Extra"], "Using filesort") {
				t.Errorf("claim query sorts: %v", got)
			}
			if got["type"] == "ALL" || got["type"] == "index" {
				t.Errorf("claim query scans: %v", got)
			}
			if got["key"] != "claim_idx" {
				t.Errorf("claim query key: want claim_idx, got %q", got["key"])
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read explain: %v", err)
		}
	})

	t.Run("concurrent claimers never get the same row", func(t *testing.T) {
		const total, claimers = 300, 8
		want := insertDue(t, db, total, notification.ChannelSMS, notification.PriorityNormal)
		var mu sync.Mutex
		seen := map[string]int{}
		var wg sync.WaitGroup
		errs := make(chan error, claimers)
		for range claimers {
			wg.Go(func() {
				for {
					jobs, err := q.Claim(t.Context(), notification.ChannelSMS, notification.PriorityNormal, 3, time.Minute)
					if err != nil {
						errs <- err
						return
					}
					if len(jobs) == 0 {
						return
					}
					mu.Lock()
					for _, j := range jobs {
						seen[j.ID]++
					}
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("claim: %v", err)
		}
		for id, n := range seen {
			if n > 1 {
				t.Errorf("%s claimed %d times", id, n)
			}
		}
		for _, id := range want {
			if seen[id] != 1 {
				t.Errorf("%s claimed %d times, want 1", id, seen[id])
			}
		}
	})

	t.Run("a call whose worker died reads attempt 1 unknown, attempt 2 accepted", func(t *testing.T) {
		ids := insertDue(t, db, 1, notification.ChannelEmail, notification.PriorityHigh)
		jobs, err := q.Claim(t.Context(), notification.ChannelEmail, notification.PriorityHigh, 5, 100*time.Millisecond)
		if err != nil || len(jobs) != 1 || jobs[0].ID != ids[0] {
			t.Fatalf("claim: %v %+v", err, jobs)
		}
		// The claim wrote the attempt in flight; the worker dies here, its
		// request sent, and the lease runs out.
		waitReaped(t, q, db, ids[0], 1)
		again, err := q.Claim(t.Context(), notification.ChannelEmail, notification.PriorityHigh, 5, time.Minute)
		// The unknown call counts, against the budget too (decision 0026: the
		// reaper cannot tell an outage from a message that kills its worker).
		if err != nil || len(again) != 1 || again[0].ClaimVersion != jobs[0].ClaimVersion+1 || again[0].Attempts != 1 || again[0].BudgetSpent != 1 {
			t.Fatalf("reclaim: %v %+v", err, again)
		}
		if err := q.MarkSent(t.Context(), again[0], accepted("m-2")); err != nil {
			t.Fatalf("mark sent: %v", err)
		}
		if got, want := history(t, db, ids[0]), []string{"1 unknown", "2 accepted"}; !slices.Equal(got, want) {
			t.Errorf("history %v, want %v", got, want)
		}
		if st, n := statusOf(t, db, ids[0]); st != "sent" || n != 2 {
			t.Errorf("want sent after 2 attempts, got %s after %d", st, n)
		}
	})

	t.Run("a worker whose lease ran out still records its own result", func(t *testing.T) {
		ids := insertDue(t, db, 1, notification.ChannelEmail, notification.PriorityHigh)
		jobs, err := q.Claim(t.Context(), notification.ChannelEmail, notification.PriorityHigh, 5, 100*time.Millisecond)
		if err != nil || len(jobs) != 1 || jobs[0].ID != ids[0] {
			t.Fatalf("claim: %v %+v", err, jobs)
		}
		stale := jobs[0]
		waitReaped(t, q, db, stale.ID, 1)
		if err := q.MarkSent(t.Context(), stale, accepted("m-1")); !errors.Is(err, notification.ErrLeaseLost) {
			t.Fatalf("late MarkSent: want ErrLeaseLost, got %v", err)
		}
		// The call's result replaces unknown; the notification is not the
		// stale worker's to finish.
		if got, want := history(t, db, stale.ID), []string{"1 accepted"}; !slices.Equal(got, want) {
			t.Errorf("history %v, want %v", got, want)
		}
		if st, n := statusOf(t, db, stale.ID); st != "pending" || n != 1 {
			t.Errorf("want pending after 1 attempt, got %s after %d", st, n)
		}
		again, err := q.Claim(t.Context(), notification.ChannelEmail, notification.PriorityHigh, 5, time.Minute)
		if err != nil || len(again) != 1 || again[0].ClaimVersion != stale.ClaimVersion+1 {
			t.Fatalf("reclaim: %v %+v", err, again)
		}
		if err := q.Reschedule(t.Context(), again[0], notification.Attempt{StartedAt: time.Now(), Outcome: notification.OutcomeTransient, HTTPStatus: 503, Error: "HTTP 503"}, time.Now()); err != nil {
			t.Fatalf("reschedule: %v", err)
		}
		third, err := q.Claim(t.Context(), notification.ChannelEmail, notification.PriorityHigh, 5, time.Minute)
		if err != nil || len(third) != 1 || third[0].Attempts != 2 {
			t.Fatalf("claim after retry: %v %+v", err, third)
		}
		if err := q.MarkSent(t.Context(), third[0], accepted("m-3")); err != nil {
			t.Fatalf("mark sent: %v", err)
		}
		if got, want := history(t, db, stale.ID), []string{"1 accepted", "2 transient", "3 accepted"}; !slices.Equal(got, want) {
			t.Errorf("history %v, want %v", got, want)
		}
		if st, n := statusOf(t, db, stale.ID); st != "sent" || n != 3 {
			t.Errorf("want sent after 3 attempts, got %s after %d", st, n)
		}
	})

	t.Run("a claim writes each job's call in flight, with its claim", func(t *testing.T) {
		insertDue(t, db, 2, notification.ChannelSMS, notification.PriorityNormal)
		jobs, err := q.Claim(t.Context(), notification.ChannelSMS, notification.PriorityNormal, 2, time.Minute)
		if err != nil || len(jobs) != 2 {
			t.Fatalf("claim: %v %+v", err, jobs)
		}
		for _, j := range jobs {
			var outcome string
			var claim int64
			if err := db.QueryRowContext(t.Context(), `SELECT outcome, claim_version FROM delivery_attempts WHERE notification_id = ? AND attempt_no = 1`, j.ID).Scan(&outcome, &claim); err != nil {
				t.Fatalf("attempt of %s: %v", j.ID, err)
			}
			if outcome != "in_flight" || claim != int64(j.ClaimVersion) { //nolint:gosec // small test values
				t.Errorf("%s: attempt %s on claim %d, want in_flight on claim %d", j.ID, outcome, claim, j.ClaimVersion)
			}
		}
	})

	t.Run("release returns the row and removes the call it never made", func(t *testing.T) {
		ids := insertDue(t, db, 2, notification.ChannelPush, notification.PriorityNormal)
		jobs, err := q.Claim(t.Context(), notification.ChannelPush, notification.PriorityNormal, 2, time.Minute)
		if err != nil || len(jobs) != 2 {
			t.Fatalf("claim: %v %+v", err, jobs)
		}
		for _, j := range jobs {
			if err := q.Release(t.Context(), j, time.Now()); err != nil {
				t.Fatalf("release: %v", err)
			}
		}
		for _, id := range ids {
			if st, n := statusOf(t, db, id); st != "pending" || n != 0 {
				t.Errorf("after release: want pending/0, got %s/%d", st, n)
			}
			if got := history(t, db, id); len(got) != 0 {
				t.Errorf("history %v, want none", got)
			}
		}
	})

	t.Run("a notification whose budget unknown calls spent fails without a call", func(t *testing.T) {
		ids := insertDue(t, db, 1, notification.ChannelEmail, notification.PriorityLow)
		if _, err := db.ExecContext(t.Context(), `UPDATE notifications SET attempts = 5, budget_spent = 3 WHERE id = ?`, ids[0]); err != nil {
			t.Fatal(err)
		}
		jobs, err := q.Claim(t.Context(), notification.ChannelEmail, notification.PriorityLow, 1, time.Minute)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("claim: %v %+v", err, jobs)
		}
		if err := q.MarkFailed(t.Context(), jobs[0], notification.Attempt{Error: "3 attempts spent"}, notification.FailureExhausted); err != nil {
			t.Fatalf("fail spent: %v", err)
		}
		var st, kind string
		var n, spent int
		if err := db.QueryRowContext(t.Context(), `SELECT status, failure_kind, attempts, budget_spent FROM notifications WHERE id = ?`, ids[0]).Scan(&st, &kind, &n, &spent); err != nil {
			t.Fatal(err)
		}
		// The claim's in-flight attempt went with it: no call was made, and
		// neither count moves.
		if st != "failed" || kind != "exhausted" || n != 5 || spent != 3 || len(history(t, db, ids[0])) != 0 {
			t.Errorf("want failed/exhausted after 5 attempts (3 counted) and no new row; got %s/%s after %d (%d), history %v", st, kind, n, spent, history(t, db, ids[0]))
		}
	})
}

func accepted(msgID string) notification.Attempt {
	return notification.Attempt{StartedAt: time.Now(), Duration: 12 * time.Millisecond, Outcome: notification.OutcomeAccepted, HTTPStatus: 202, ProviderMessageID: msgID}
}

// history lists the attempts of id as "<attempt_no> <outcome>", in order.
func history(t *testing.T, db *sql.DB, id string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT attempt_no, outcome FROM delivery_attempts WHERE notification_id = ? ORDER BY attempt_no`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var no int
		var outcome string
		if err := rows.Scan(&no, &outcome); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%d %s", no, outcome))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// statusOf returns the status and attempts of id.
func statusOf(t *testing.T, db *sql.DB, id string) (string, int) {
	t.Helper()
	var st string
	var n int
	if err := db.QueryRowContext(t.Context(), "SELECT status, attempts FROM notifications WHERE id = ?", id).Scan(&st, &n); err != nil {
		t.Fatal(err)
	}
	return st, n
}

// waitReaped reaps until id is pending again; the reaper must report the row
// as it is, with attempts counted (an attempt in flight counts as unknown).
func waitReaped(t *testing.T, q *store.Queue, db *sql.DB, id string, attempts int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		reaped, err := q.ReapExpired(ctx)
		if err != nil {
			t.Fatalf("reap: %v", err)
		}
		for _, j := range reaped {
			// What the worker publishes: the row's identity, not a guess.
			if j.ID == id && (j.Channel == "" || j.CorrelationID != "corr" || j.Attempts != attempts) {
				t.Fatalf("reaped job reported as %+v, want %d attempts", j, attempts)
			}
		}
		var st string
		if err := db.QueryRowContext(ctx, "SELECT status FROM notifications WHERE id = ?", id).Scan(&st); err != nil {
			t.Fatal(err)
		}
		if st == "pending" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(fmt.Errorf("lease of %s never reaped", id))
		case <-tick.C:
		}
	}
}

func TestQueueNextDue(t *testing.T) {
	db, _ := support.MySQL(t)
	q := store.NewQueue(db)
	ch := notification.ChannelEmail
	insert := func(p notification.Priority, status notification.Status, dueIn time.Duration) {
		t.Helper()
		id := uuid.Must(uuid.NewV7()).String()
		_, err := db.ExecContext(t.Context(), `INSERT INTO notifications (id, recipient, channel, priority, content, due_at, correlation_id)
			VALUES (?, 'a@example.com', ?, ?, 'hello', NOW(3) + INTERVAL ? MICROSECOND, 'corr')`,
			id, ch, p, dueIn.Microseconds())
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		// Rows enter pending; the database refuses any other way in (0005).
		for _, step := range pathTo[status] {
			if err := moveTo(t, db, id, step); err != nil {
				t.Fatalf("walking to %s, step %s: %v", status, step, err)
			}
		}
	}

	t.Run("nothing pending reports no priority", func(t *testing.T) {
		got, err := q.NextDue(t.Context(), ch)
		if err != nil || len(got) != 0 {
			t.Fatalf("want empty, got %v, %v", got, err)
		}
	})

	t.Run("each priority reports its earliest pending due time", func(t *testing.T) {
		insert(notification.PriorityHigh, notification.StatusPending, -time.Second)
		insert(notification.PriorityHigh, notification.StatusPending, time.Hour)
		insert(notification.PriorityNormal, notification.StatusPending, 3*time.Second)
		insert(notification.PriorityNormal, notification.StatusPending, 9*time.Second)
		insert(notification.PriorityLow, notification.StatusSent, -time.Second) // not pending: absent
		got, err := q.NextDue(t.Context(), ch)
		if err != nil {
			t.Fatal(err)
		}
		if w, ok := got[notification.PriorityHigh]; !ok || w > 0 {
			t.Errorf("high: want due now (<= 0), got %v (present %v)", w, ok)
		}
		if w := got[notification.PriorityNormal]; w < 2500*time.Millisecond || w > 3*time.Second {
			t.Errorf("normal: want about 3 s, got %v", w)
		}
		if _, ok := got[notification.PriorityLow]; ok {
			t.Errorf("low: want absent (nothing pending), got %v", got)
		}
	})

	t.Run("each priority costs one claim_idx dive, whatever the backlog", func(t *testing.T) {
		for range 200 {
			insert(notification.PriorityLow, notification.StatusPending, time.Minute)
		}
		// The arm of NextDue for one priority; the other two are the same shape.
		rows, err := db.QueryContext(t.Context(), `EXPLAIN SELECT MIN(due_at) IS NOT NULL, CAST(COALESCE(TIMESTAMPDIFF(MICROSECOND, NOW(3), MIN(due_at)), 0) AS SIGNED)
			FROM notifications WHERE status = 'pending' AND channel = 'email' AND priority = 'low'`)
		if err != nil {
			t.Fatalf("explain: %v", err)
		}
		defer func() { _ = rows.Close() }()
		cols, _ := rows.Columns()
		n := 0
		for rows.Next() {
			n++
			vals := make([]sql.NullString, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatalf("scan explain: %v", err)
			}
			got := map[string]string{}
			for i, c := range cols {
				got[c] = vals[i].String
			}
			t.Logf("EXPLAIN next due: type=%s key=%s rows=%s extra=%s", got["type"], got["key"], got["rows"], got["Extra"])
			// MIN over a constant index prefix is resolved from the index
			// while planning: no rows are read at execution.
			if got["Extra"] != "Select tables optimized away" {
				t.Errorf("want one index dive (Select tables optimized away), got %v", got)
			}
		}
		if err := rows.Err(); err != nil || n == 0 {
			t.Fatalf("read explain: %v (%d rows)", err, n)
		}
	})
}
