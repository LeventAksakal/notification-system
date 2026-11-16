package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// Release returns a claimed notification to pending without an attempt: the
// call was never made, so an attempt the claim began is removed.
func (s *Queue) Release(ctx context.Context, job notification.Job, dueAt time.Time) error {
	if err := notification.Transition(notification.StatusProcessing, notification.StatusPending); err != nil {
		return err
	}
	return retryLockConflict(ctx, func() error { return s.releaseOnce(ctx, job, dueAt) })
}

// releaseOnce gives the lease back, then removes the claim's in-flight
// attempt, in one READ COMMITTED transaction.
func (s *Queue) releaseOnce(ctx context.Context, job notification.Job, dueAt time.Time) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("release %s: begin: %w", job.ID, err)
	}
	defer func() { _ = tx.Rollback() }()
	q := s.q.WithTx(tx)
	affected, err := q.ReleaseLease(ctx, sqlc.ReleaseLeaseParams{DelayUs: delayUs(dueAt), ID: job.ID, ClaimVersion: job.ClaimVersion})
	if err != nil {
		return fmt.Errorf("release %s: %w", job.ID, RuleViolation(err))
	}
	if affected == 0 {
		return fmt.Errorf("release %s: %w", job.ID, notification.ErrLeaseLost)
	}
	if _, err := q.DeleteUnsentAttempt(ctx, sqlc.DeleteUnsentAttemptParams{NotificationID: job.ID, ClaimVersion: nullClaim(job.ClaimVersion)}); err != nil {
		return fmt.Errorf("release %s: attempt: %w", job.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("release %s: commit: %w", job.ID, err)
	}
	return nil
}
