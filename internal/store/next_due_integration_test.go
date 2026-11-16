//go:build integration

package store_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

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
