package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// finishAttempt writes the result of the claim's call to its attempt row
// (written in flight by the claim) and reports whether there was one.
func finishAttempt(ctx context.Context, q *sqlc.Queries, job notification.Job, a notification.Attempt) (bool, error) {
	if err := notification.AttemptTransition(notification.OutcomeInFlight, a.Outcome); err != nil {
		return false, err
	}
	n, err := q.FinishAttempt(ctx, sqlc.FinishAttemptParams{
		Outcome:           sqlc.DeliveryAttemptsOutcome(a.Outcome),
		DurationMs:        uint32(max(a.Duration.Milliseconds(), 0)), //nolint:gosec // non-negative, bounded by the timeout
		HttpStatus:        nullInt16(a.HTTPStatus),
		Error:             nullString(cut(a.Error, notification.MaxErrorLen)),
		ProviderMessageID: nullString(cut(a.ProviderMessageID, maxProviderIDLen)),
		NotificationID:    job.ID,
		ClaimVersion:      nullClaim(job.ClaimVersion),
	})
	if err != nil {
		return false, RuleViolation(err)
	}
	return n > 0, nil
}

// failSpent ends the claimed row as failed (exhausted) without a call, the
// budget spent by calls whose result is unknown, and removes the in-flight
// attempt its claim wrote, in one READ COMMITTED transaction.
func (s *Queue) failSpent(ctx context.Context, job notification.Job, reason string) error {
	if err := notification.Transition(notification.StatusProcessing, notification.StatusFailed); err != nil {
		return err
	}
	return retryLockConflict(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return fmt.Errorf("fail %s as spent: begin: %w", job.ID, err)
		}
		defer func() { _ = tx.Rollback() }()
		q := s.q.WithTx(tx)
		n, err := q.FailSpent(ctx, sqlc.FailSpentParams{LastError: nullString(cut(reason, notification.MaxErrorLen)), ID: job.ID, ClaimVersion: job.ClaimVersion})
		if err != nil {
			return fmt.Errorf("fail %s as spent: %w", job.ID, RuleViolation(err))
		}
		if n == 0 {
			return fmt.Errorf("fail %s as spent: %w", job.ID, notification.ErrLeaseLost)
		}
		if _, err := q.DeleteUnsentAttempt(ctx, sqlc.DeleteUnsentAttemptParams{NotificationID: job.ID, ClaimVersion: nullClaim(job.ClaimVersion)}); err != nil {
			return fmt.Errorf("fail %s as spent: attempt: %w", job.ID, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("fail %s as spent: commit: %w", job.ID, err)
		}
		return nil
	})
}
