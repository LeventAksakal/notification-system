package intake

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
)

// Cancel moves a pending notification to cancelled (decision 0005).
func (s *Service) Cancel(ctx context.Context, id string) (Notification, error) {
	return s.transition(ctx, id, notification.StatusPending, notification.StatusCancelled, s.q.CancelNotification, ErrNotCancellable, "notification cancelled")
}

// Retry replays a failed notification: pending again, fresh attempt budget,
// due now; its attempt history is kept.
func (s *Service) Retry(ctx context.Context, id string) (Notification, error) {
	return s.transition(ctx, id, notification.StatusFailed, notification.StatusPending, s.q.ReplayNotification, ErrNotRetryable, "notification replayed")
}

func (s *Service) transition(ctx context.Context, id string, from, to notification.Status,
	update func(context.Context, string) (sql.Result, error), refused error, msg string,
) (Notification, error) {
	if err := notification.Transition(from, to); err != nil {
		return Notification{}, err
	}
	res, err := update(ctx, id)
	if err != nil {
		return Notification{}, fmt.Errorf("%s %s to %s: %w", id, from, to, err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return Notification{}, fmt.Errorf("%s %s to %s: rows affected: %w", id, from, to, err)
	}
	if changed == 0 {
		if _, err := s.get(ctx, id); err != nil {
			return Notification{}, err
		}
		return Notification{}, refused
	}
	n, err := s.get(ctx, id)
	if err != nil {
		return Notification{}, err
	}
	// Lines about a notification carry its stored correlation id; the
	// request's own id stays on the access line and is added here.
	lctx := telemetry.WithNotification(ctx, id, n.CorrelationID)
	slog.InfoContext(lctx, msg, "from", from, "to", to, "request_correlation_id", telemetry.CorrelationID(ctx))
	// The statement set status_seq = LAST_INSERT_ID(status_seq + 1): the
	// sequence of this change, whatever happens to the row after it. The
	// change is committed either way; without it the read's value is the
	// best left (exact unless a later change already landed).
	seq := n.StatusSeq
	if v, err := res.LastInsertId(); err == nil && v > 0 {
		seq = uint64(v) //nolint:gosec // v > 0
	} else {
		slog.WarnContext(lctx, "no status_seq from the update; using the read", "err", err, "status_seq", seq)
	}
	// The read may already show a later change (a worker claims a replayed
	// row at once); the event describes this change. A cancel is final and a
	// replay resets attempts, so neither depends on that read.
	e := notification.Event{
		NotificationID: n.ID, BatchID: n.BatchID.String, Status: to, Attempts: int(n.Attempts),
		Error: n.LastError.String, At: n.UpdatedAt, CorrelationID: n.CorrelationID, Seq: seq,
	}
	if to == notification.StatusPending { // a replay: fresh budget, claimable again
		e.Attempts = 0
	}
	s.publishEvent(lctx, e)
	if to == notification.StatusPending {
		s.wake(lctx, hintFor(n.Notification))
	}
	return n, nil
}
