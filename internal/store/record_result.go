package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// maxProviderIDLen is the width of provider_message_id.
const maxProviderIDLen = 255

// MarkSent records the accepted attempt and ends the notification as sent.
func (s *Queue) MarkSent(ctx context.Context, job notification.Job, a notification.Attempt) error {
	return s.finish(ctx, job, a, notification.StatusSent, func(q *sqlc.Queries) (int64, error) {
		return q.FinishSent(ctx, sqlc.FinishSentParams{
			ProviderMessageID: nullString(cut(a.ProviderMessageID, maxProviderIDLen)),
			ID:                job.ID,
			ClaimVersion:      job.ClaimVersion,
		})
	})
}

// Reschedule records the failed attempt and returns the notification to
// pending, due at dueAt. The delay is applied on the database clock. The
// failure counts against the budget only when a.Counted (decision 0026).
func (s *Queue) Reschedule(ctx context.Context, job notification.Job, a notification.Attempt, dueAt time.Time) error {
	return s.finish(ctx, job, a, notification.StatusPending, func(q *sqlc.Queries) (int64, error) {
		counted := uint32(0)
		if a.Counted {
			counted = 1
		}
		return q.FinishRetry(ctx, sqlc.FinishRetryParams{
			Counted:      counted,
			LastError:    nullString(cut(a.Error, notification.MaxErrorLen)),
			DelayUs:      delayUs(dueAt),
			ID:           job.ID,
			ClaimVersion: job.ClaimVersion,
		})
	})
}

// MarkFailed records the failed attempt and ends the notification as failed.
func (s *Queue) MarkFailed(ctx context.Context, job notification.Job, a notification.Attempt, kind notification.FailureKind) error {
	if a.Outcome == "" {
		return s.failSpent(ctx, job, a.Error)
	}
	return s.finish(ctx, job, a, notification.StatusFailed, func(q *sqlc.Queries) (int64, error) {
		return q.FinishFailed(ctx, sqlc.FinishFailedParams{
			LastError:    nullString(cut(a.Error, notification.MaxErrorLen)),
			FailureKind:  sqlc.NullNotificationsFailureKind{NotificationsFailureKind: sqlc.NotificationsFailureKind(kind), Valid: true},
			ID:           job.ID,
			ClaimVersion: job.ClaimVersion,
		})
	})
}

// finish updates the claimed row (fenced by claim_version) and inserts its
// delivery_attempts row in one transaction.
func (s *Queue) finish(ctx context.Context, job notification.Job, a notification.Attempt, to notification.Status, update func(*sqlc.Queries) (int64, error)) error {
	if err := notification.Transition(notification.StatusProcessing, to); err != nil {
		return err
	}
	return retryLockConflict(ctx, func() error { return s.finishOnce(ctx, job, a, to, update) })
}

func (s *Queue) finishOnce(ctx context.Context, job notification.Job, a notification.Attempt, to notification.Status, update func(*sqlc.Queries) (int64, error)) error {
	// READ COMMITTED like the claim: no gap locks, so a result write and a
	// claim on the same channel do not deadlock on claim_idx.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("finish %s: begin: %w", job.ID, err)
	}
	defer func() { _ = tx.Rollback() }()
	q := s.q.WithTx(tx)
	affected, err := update(q)
	if err != nil {
		return fmt.Errorf("finish %s as %s: %w", job.ID, to, RuleViolation(err))
	}
	// The attempt row (written in flight by the claim) gets its result
	// even when the lease was lost: the call happened either way.
	found, err := finishAttempt(ctx, q, job, a)
	if err != nil {
		return fmt.Errorf("finish %s: attempt: %w", job.ID, err)
	}
	switch {
	case !found && affected == 0:
		return fmt.Errorf("finish %s as %s: %w", job.ID, to, notification.ErrLeaseLost)
	case !found:
		return fmt.Errorf("finish %s: %w", job.ID, errNoAttempt)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finish %s: commit: %w", job.ID, err)
	}
	if affected == 0 {
		return fmt.Errorf("finish %s as %s (attempt recorded): %w", job.ID, to, notification.ErrLeaseLost)
	}
	return nil
}

// errNoAttempt: a result arrived for a claim whose attempt was never begun.
var errNoAttempt = errors.New("no attempt was begun for this claim")
