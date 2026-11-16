//go:build integration

package store_test

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/test/support"
)

// Database-level tests of decision 0015 ("As built"): the template version
// and the notification's template origin are rules of the database, so they
// are driven here with hand-written SQL (and the real UpdateTemplate text)
// that bypasses the service: they must hold for any writer.

func TestTemplateVersionRulesInSQL(t *testing.T) {
	db, _ := support.MySQL(t)
	ctx := t.Context()

	version := func(t *testing.T, name string) uint32 {
		t.Helper()
		var v uint32
		if err := db.QueryRowContext(ctx, `SELECT version FROM templates WHERE name = ?`, name).Scan(&v); err != nil {
			t.Fatalf("read version of %s: %v", name, err)
		}
		return v
	}
	exec := func(t *testing.T, q string, args ...any) int64 {
		t.Helper()
		res, err := db.ExecContext(ctx, q, args...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("an insert always stores version 1, whatever the writer sends", func(t *testing.T) {
		exec(t, `INSERT INTO templates (name, body) VALUES ('plain', 'x')`)
		exec(t, `INSERT INTO templates (name, body, version) VALUES ('forged', 'x', 41)`)
		for _, name := range []string{"plain", "forged"} {
			if v := version(t, name); v != 1 {
				t.Errorf("%s: version %d, want 1", name, v)
			}
		}
	})

	t.Run("every update adds exactly 1, whatever the writer sets", func(t *testing.T) {
		exec(t, `INSERT INTO templates (name, body) VALUES ('steps', 'v1')`)
		for i, q := range []string{
			`UPDATE templates SET body = 'v2' WHERE name = 'steps'`,                // version not named
			`UPDATE templates SET body = 'v2' WHERE name = 'steps'`,                // same body: still a new version
			`UPDATE templates SET body = 'v3', version = 1 WHERE name = 'steps'`,   // reuse an old version
			`UPDATE templates SET body = 'v4', version = 100 WHERE name = 'steps'`, // skip ahead
			`UPDATE templates SET body = 'v5', version = 0 WHERE name = 'steps'`,   // reset below the CHECK
			`UPDATE templates SET version = version + 5 WHERE name = 'steps'`,      // jump by more than one
		} {
			if n := exec(t, q); n != 1 {
				t.Fatalf("%s: %d rows changed, want 1", q, n)
			}
			if got, want := version(t, "steps"), uint32(i+2); got != want {
				t.Fatalf("after %s: version %d, want %d", q, got, want)
			}
		}
	})

	t.Run("the service's conditional update changes the row only at the current version", func(t *testing.T) {
		exec(t, `INSERT INTO templates (name, body) VALUES ('cond', 'v1')`)
		update := query(t, "UpdateTemplate", 0)
		if n := exec(t, update, "v2", "cond", 1); n != 1 {
			t.Fatalf("at the current version: %d rows, want 1", n)
		}
		if n := exec(t, update, "stale", "cond", 1); n != 0 {
			t.Fatalf("at a stale version: %d rows, want 0", n)
		}
		if n := exec(t, update, "x", "absent", 1); n != 0 {
			t.Fatalf("on a missing template: %d rows, want 0", n)
		}
		var body string
		if err := db.QueryRowContext(ctx, `SELECT body FROM templates WHERE name = 'cond'`).Scan(&body); err != nil {
			t.Fatal(err)
		}
		if body != "v2" || version(t, "cond") != 2 {
			t.Fatalf("body %q version %d, want v2 at 2", body, version(t, "cond"))
		}
	})

	t.Run("a notification's template origin is both columns or neither, version from 1", func(t *testing.T) {
		insert := func(name sql.NullString, v sql.NullInt64) error {
			_, err := db.ExecContext(ctx, `INSERT INTO notifications (id, recipient, channel, content, due_at, correlation_id,
				template_name, template_version) VALUES (?, '+905551234567', 'sms', 'hello', NOW(3), 'corr', ?, ?)`,
				uuid.Must(uuid.NewV7()).String(), name, v)
			return err
		}
		name := sql.NullString{String: "welcome", Valid: true}
		none := sql.NullString{}
		for _, tc := range []struct {
			what  string
			name  sql.NullString
			v     sql.NullInt64
			check string // "" when accepted
		}{
			{"plain content: both NULL", none, sql.NullInt64{}, ""},
			{"from a template: both set", name, sql.NullInt64{Int64: 3, Valid: true}, ""},
			{"a name without a version", name, sql.NullInt64{}, "template_origin_complete"},
			{"a version without a name", none, sql.NullInt64{Int64: 3, Valid: true}, "template_origin_complete"},
			{"version 0", name, sql.NullInt64{Int64: 0, Valid: true}, "template_origin_version_positive"},
		} {
			t.Run(tc.what, func(t *testing.T) {
				err := insert(tc.name, tc.v)
				if tc.check == "" {
					if err != nil {
						t.Fatalf("refused: %v", err)
					}
					return
				}
				wantCheck(t, err, tc.check)
			})
		}
	})

	// Last: it drops the triggers of this test's own database.
	t.Run("without the triggers the CHECK still refuses version 0", func(t *testing.T) {
		exec(t, `INSERT INTO templates (name, body) VALUES ('bare', 'x')`)
		exec(t, `DROP TRIGGER templates_version_insert`)
		exec(t, `DROP TRIGGER templates_version_update`)
		_, err := db.ExecContext(ctx, `UPDATE templates SET version = 0 WHERE name = 'bare'`)
		wantCheck(t, err, "template_version_positive")
		_, err = db.ExecContext(ctx, `INSERT INTO templates (name, body, version) VALUES ('zero', 'x', 0)`)
		wantCheck(t, err, "template_version_positive")
	})
}
