//go:build integration

package store_test

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
	"github.com/LeventAksakal/notification-system/test/support"
)

// insertListed inserts n notifications on ch with the given status and
// created_at taken from the id, as intake does.
func insertListed(t *testing.T, db *sql.DB, n int, ch notification.Channel, status notification.Status) []string {
	t.Helper()
	to := map[notification.Channel]string{notification.ChannelSMS: "+905551234567", notification.ChannelEmail: "a@example.com", notification.ChannelPush: "device-1"}[ch]
	kind := sql.NullString{String: "permanent", Valid: status == notification.StatusFailed}
	ids := make([]string, n)
	for i := range n {
		id := uuid.Must(uuid.NewV7())
		ids[i] = id.String()
		if _, err := db.ExecContext(t.Context(), `INSERT INTO notifications
			(id, recipient, channel, content, status, failure_kind, due_at, correlation_id, created_at)
			VALUES (?, ?, ?, 'hello', ?, ?, NOW(3), 'corr', ?)`, ids[i], to, ch, status, kind, idCreatedAt(ids[i])); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return ids
}

func idCreatedAt(id string) time.Time {
	sec, nsec := uuid.MustParse(id).Time().UnixTime()
	return time.Unix(sec, nsec).UTC().Truncate(time.Millisecond)
}

// explain returns the EXPLAIN rows of query, one map per table.
func explain(t *testing.T, db *sql.DB, query string, args []any) []map[string]string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "EXPLAIN "+query, args...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("explain columns: %v", err)
	}
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
		got := map[string]string{}
		for i, c := range cols {
			got[c] = vals[i].String
		}
		out = append(out, got)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read explain: %v", err)
	}
	return out
}

func TestList(t *testing.T) {
	db, _ := support.MySQL(t)
	var all []string // creation order
	for _, ch := range notification.Channels {
		all = append(all, insertListed(t, db, 60, ch, notification.StatusPending)...)
		all = append(all, insertListed(t, db, 60, ch, notification.StatusSent)...)
		all = append(all, insertListed(t, db, 20, ch, notification.StatusFailed)...)
	}
	if _, err := db.ExecContext(t.Context(), "ANALYZE TABLE notifications"); err != nil {
		t.Fatalf("analyze: %v", err)
	}
	from, to := idCreatedAt(all[100]), idCreatedAt(all[400])
	scheduled := false

	for _, tc := range []struct {
		name string
		f    store.ListFilter
		key  string
	}{
		{"status filter", store.ListFilter{Status: "sent"}, "list_status_idx"},
		{"pending view filter", store.ListFilter{Status: "pending", Scheduled: &scheduled}, "list_status_idx"},
		{"channel filter", store.ListFilter{Channel: "email"}, "list_channel_idx"},
		{"status and channel", store.ListFilter{Status: "failed", Channel: "push"}, "list_status_idx"},
		{"date range", store.ListFilter{CreatedFrom: &from, CreatedTo: &to}, "PRIMARY"},
		{"status with a date range", store.ListFilter{Status: "sent", CreatedFrom: &from, CreatedTo: &to}, "list_status_idx"},
		{"no filter", store.ListFilter{}, "PRIMARY"},
		{"no filter with a cursor", store.ListFilter{BeforeID: all[300]}, "PRIMARY"},
	} {
		t.Run(tc.name+" reads "+tc.key+" in id order", func(t *testing.T) {
			tc.f.Limit = 51
			query, args := store.ListNotificationsQuery(tc.f)
			plan := explain(t, db, query, args)
			if len(plan) != 1 {
				t.Fatalf("want one table in the plan, got %v", plan)
			}
			p := plan[0]
			t.Logf("EXPLAIN %s: type=%s key=%s rows=%s extra=%s", tc.name, p["type"], p["key"], p["rows"], p["Extra"])
			if p["type"] == "ALL" {
				t.Errorf("full table scan: %v", p)
			}
			if strings.Contains(p["Extra"], "Using filesort") {
				t.Errorf("sorts: %v", p)
			}
			if p["key"] != tc.key {
				t.Errorf("key: want %s, got %q", tc.key, p["key"])
			}
		})
	}

	t.Run("a date range lists exactly the rows created inside it, newest first", func(t *testing.T) {
		for _, shift := range []time.Duration{0, -500 * time.Microsecond, 500 * time.Microsecond} {
			f, tt := from.Add(shift), to.Add(shift)
			var want []string
			for _, id := range all {
				if c := idCreatedAt(id); !c.Before(f) && c.Before(tt) {
					want = append(want, id)
				}
			}
			slices.Reverse(want)
			rows, err := store.ListNotifications(t.Context(), db, store.ListFilter{CreatedFrom: &f, CreatedTo: &tt, Limit: len(all)})
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, len(rows))
			for i, r := range rows {
				got[i] = r.ID
			}
			if !slices.Equal(got, want) {
				t.Errorf("shift %s: want %d rows, got %d", shift, len(want), len(got))
			}
		}
	})
}

func TestIdempotencySweep(t *testing.T) {
	db, _ := support.MySQL(t)
	q := sqlc.New(db)
	insert := func(key string, age time.Duration) {
		t.Helper()
		if _, err := db.ExecContext(t.Context(), `INSERT INTO idempotency_keys (idem_key, request_hash, resource_kind, resource_id, created_at)
			VALUES (?, UNHEX(SHA2(?, 256)), 'notification', ?, NOW(3) - INTERVAL ? MICROSECOND)`,
			key, key, uuid.NewString(), age.Microseconds()); err != nil {
			t.Fatalf("insert key: %v", err)
		}
	}
	for i := range 1200 {
		insert(fmt.Sprintf("old-%d", i), 2*time.Hour)
	}
	insert("fresh", time.Minute)
	if _, err := db.ExecContext(t.Context(), "ANALYZE TABLE idempotency_keys"); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	plan := explain(t, db, "DELETE FROM idempotency_keys WHERE created_at < NOW(3) - INTERVAL ? * 1000 MICROSECOND LIMIT 1000",
		[]any{time.Hour.Milliseconds()})
	t.Logf("EXPLAIN sweep: %v", plan)
	if len(plan) != 1 || plan[0]["key"] != "idempotency_created_idx" {
		t.Errorf("sweep plan: want idempotency_created_idx, got %v", plan)
	}

	n, err := q.SweepIdempotencyKeys(t.Context(), time.Hour.Milliseconds())
	if err != nil || n != 1000 {
		t.Fatalf("first sweep: want 1000 deleted, got %d, %v", n, err)
	}
	n, err = q.SweepIdempotencyKeys(t.Context(), time.Hour.Milliseconds())
	if err != nil || n != 200 {
		t.Fatalf("second sweep: want 200 deleted, got %d, %v", n, err)
	}
	if _, err := q.GetIdempotencyKey(t.Context(), "fresh"); err != nil {
		t.Fatalf("a key inside the retention was swept: %v", err)
	}
}
