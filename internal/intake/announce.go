package intake

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// BatchPublisher is a notification.Publisher that also sends many events as one call
// with one bounded wait (realtime.Publisher), so a batch's announcement never
// costs one wait per item.
type BatchPublisher interface {
	notification.Publisher
	PublishAll(ctx context.Context, es []notification.Event) error
}

// PublishBudget bounds the time a request spends announcing its changes when
// pub has no batch form: every event of the request shares this one deadline.
const PublishBudget = time.Second

// WithWaker makes s send a wake hint after every commit that makes a
// notification pending (create, batch, replay), so an idle worker claims it
// at once instead of on its safety poll. Call it before s is used.
func (s *Service) WithWaker(w notification.Waker) *Service {
	s.waker = w
	return s
}

// wake sends hints, best effort: an error is logged, never returned. It runs
// after the commit, so a worker it wakes finds the rows.
func (s *Service) wake(ctx context.Context, hints ...notification.Wake) {
	if s.waker == nil || len(hints) == 0 {
		return
	}
	if err := s.waker.Wake(ctx, hints...); err != nil {
		slog.WarnContext(ctx, "wake hint", "count", len(hints), "err", err)
	}
}

// hintFor is the wake hint for a stored pending notification.
func hintFor(n sqlc.Notification) notification.Wake {
	return notification.Wake{Channel: notification.Channel(n.Channel), Priority: notification.Priority(n.Priority), Due: n.DueAt}
}

// sequential gives a plain notification.Publisher a batch form: one event after the
// other under one shared deadline, stopping at the first error.
type sequential struct{ notification.Publisher }

func (p sequential) PublishAll(ctx context.Context, es []notification.Event) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), PublishBudget)
	defer cancel()
	for i, e := range es {
		if err := p.Publish(ctx, e); err != nil {
			return fmt.Errorf("publish event %d of %d: %w", i+1, len(es), err)
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("publish: %d of %d events left: %w", len(es)-i-1, len(es), err)
		}
	}
	return nil
}

// publishCreated announces a creation: pending, sequence 1. It is built from
// the stored row but not from its current state, which a worker may already
// have moved on (that change is announced by the worker).
func (s *Service) publishCreated(ctx context.Context, n sqlc.Notification) {
	s.publishEvent(ctx, notification.Event{
		NotificationID: n.ID, BatchID: n.BatchID.String, Status: notification.StatusPending,
		At: n.CreatedAt, CorrelationID: n.CorrelationID, Seq: 1,
	})
}

// publishEvent is best effort: an error is logged, never returned.
func (s *Service) publishEvent(ctx context.Context, e notification.Event) {
	if err := s.pub.Publish(ctx, e); err != nil {
		slog.WarnContext(ctx, "publish status change", "status", e.Status, "err", err)
	}
}
