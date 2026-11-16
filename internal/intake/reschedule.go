package intake

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
)

// ErrNotReschedulable refuses a reschedule of a notification a worker has
// already picked up: rescheduling a retry would override its backoff and any
// provider Retry-After (decision 0007).
var ErrNotReschedulable = errors.New("only a pending notification that no worker has picked up yet can be rescheduled")

// Reschedule gives a notification that no worker has claimed a new schedule:
// at, or now when at is nil or in the past (the create rule, decision 0016).
// It is one conditional UPDATE, never a read then a write, so a claim racing
// it on the same row has exactly one winner. The database decides the
// horizon in that same statement, on its own clock (the
// scheduled_within_horizon CHECK), so no clock is read first.
//
// No status event is published: the stored status stays pending and the
// event carries neither the schedule nor the API's `scheduled` view.
func (s *Service) Reschedule(ctx context.Context, id string, at *time.Time) (Notification, error) {
	changed, err := s.q.RescheduleNotification(ctx, sqlc.RescheduleNotificationParams{ID: id, ScheduledAt: nullTime(truncate(at))})
	switch {
	case store.IsBeyondScheduleHorizon(err):
		return Notification{}, invalid("scheduledAt", horizonReason())
	case err != nil:
		return Notification{}, fmt.Errorf("reschedule %s: %w", id, err)
	}
	n, err := s.get(ctx, id)
	if err != nil {
		return Notification{}, err // ErrNotFound when there is no such notification
	}
	// MySQL counts changed rows, not matched ones: an UPDATE that writes the
	// values the row already holds (the same future schedule again) changes
	// nothing. A row still unclaimed now was unclaimed
	// then too (claim_version never goes back to 0), so it matched.
	if changed == 0 && !(n.Notification.Status == sqlc.NotificationsStatusPending && n.ClaimVersion == 0) {
		return Notification{}, notReschedulable(n)
	}
	lctx := telemetry.WithNotification(ctx, n.ID, n.CorrelationID)
	slog.InfoContext(lctx, "notification rescheduled", "scheduled", n.Scheduled,
		"request_correlation_id", telemetry.CorrelationID(ctx))
	return n, nil
}

// notReschedulable says why n cannot be rescheduled, with its current status.
func notReschedulable(n Notification) error {
	if n.Notification.Status == sqlc.NotificationsStatusPending {
		return fmt.Errorf("%w; it was already picked up and waits for its next attempt (status %s)", ErrNotReschedulable, n.Status())
	}
	return fmt.Errorf("%w; its status is %s", ErrNotReschedulable, n.Status())
}

// truncate returns t in UTC to the millisecond, the precision of the
// DATETIME(3) columns; nil stays nil.
func truncate(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC().Truncate(time.Millisecond)
	return &u
}
