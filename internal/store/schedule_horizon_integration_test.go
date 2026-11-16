//go:build integration

package store_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

// The scheduled_within_horizon CHECK holds notification.ScheduleHorizon on
// the database clock for every writer, the service or not: an insert or an
// update further ahead is refused, one at the horizon is stored. The offsets
// are computed in SQL from NOW(3), so the test runs on the clock the CHECK
// uses and asserts the CHECK's interval equals the Go constant.
func TestTheDatabaseRefusesASchedulePastTheHorizon(t *testing.T) {
	db, _ := support.MySQL(t)
	ctx := t.Context()
	horizonUS := notification.ScheduleHorizon.Microseconds()
	const insert = `INSERT INTO notifications (id, recipient, channel, content, due_at, scheduled_at, correlation_id)
		VALUES (?, 'device-1', 'push', 'hi', NOW(3), NOW(3) + INTERVAL ? MICROSECOND, 'test')`

	for _, tc := range []struct {
		name     string
		offsetUS int64 // scheduled_at minus NOW(3)
		refused  bool
	}{
		{"at the horizon: stored", horizonUS, false},
		{"one millisecond past it: refused", horizonUS + int64(time.Millisecond/time.Microsecond), true},
		{"in the past: stored", -int64(time.Hour / time.Microsecond), false},
	} {
		t.Run("insert "+tc.name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, insert, uuid.NewString(), tc.offsetUS)
			if got := store.IsBeyondScheduleHorizon(err); got != tc.refused || (!tc.refused && err != nil) {
				t.Fatalf("err %v, want refused=%v", err, tc.refused)
			}
		})
	}

	t.Run("an update past the horizon is refused and changes nothing", func(t *testing.T) {
		id := uuid.NewString()
		if _, err := db.ExecContext(ctx, insert, id, int64(time.Hour/time.Microsecond)); err != nil {
			t.Fatal(err)
		}
		_, err := db.ExecContext(ctx, `UPDATE notifications SET scheduled_at = NOW(3) + INTERVAL ? MICROSECOND WHERE id = ?`,
			horizonUS+int64(time.Second/time.Microsecond), id)
		if !store.IsBeyondScheduleHorizon(err) {
			t.Fatalf("err %v, want the horizon CHECK", err)
		}
	})
	t.Run("an update is measured from its own NOW(3), which ON UPDATE stamps", func(t *testing.T) {
		// Last written a day ago: the old updated_at would refuse a schedule
		// 30 days from now, the statement's NOW(3) accepts it.
		id := uuid.NewString()
		if _, err := db.ExecContext(ctx, `INSERT INTO notifications (id, recipient, channel, content, due_at, correlation_id, created_at, updated_at)
			VALUES (?, 'device-1', 'push', 'hi', NOW(3), 'test', NOW(3) - INTERVAL 1 DAY, NOW(3) - INTERVAL 1 DAY)`, id); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE notifications SET scheduled_at = NOW(3) + INTERVAL ? MICROSECOND WHERE id = ?`,
			horizonUS-int64(time.Minute/time.Microsecond), id); err != nil {
			t.Fatalf("want accepted against this statement's NOW(3): %v", err)
		}
	})
	t.Run("a later write to a row scheduled at the horizon still passes", func(t *testing.T) {
		id := uuid.NewString()
		if _, err := db.ExecContext(ctx, insert, id, horizonUS); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE notifications SET status = 'cancelled', finished_at = NOW(3) WHERE id = ?`, id); err != nil {
			t.Fatalf("updated_at only grows, so the row keeps passing: %v", err)
		}
	})
}
