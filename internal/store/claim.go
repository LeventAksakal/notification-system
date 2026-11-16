package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// Claim leases up to n due pending notifications of one channel and priority.
// The SELECT ... FOR UPDATE SKIP LOCKED runs through claim_idx in a READ
// COMMITTED transaction, so concurrent claimers never wait on each other and
// never get the same row.
func (s *Queue) Claim(ctx context.Context, channel notification.Channel, priority notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	if n <= 0 {
		return nil, nil
	}
	if err := notification.Transition(notification.StatusPending, notification.StatusProcessing); err != nil {
		return nil, err
	}
	var jobs []notification.Job
	err := retryLockConflict(ctx, func() error {
		var err error
		jobs, err = s.claimOnce(ctx, channel, priority, n, lease)
		return err
	})
	return jobs, err
}

func (s *Queue) claimOnce(ctx context.Context, channel notification.Channel, priority notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	// Taken before the transaction, so LeaseUntil is never later than the
	// lease_until the database sets (the worker stops sending by it).
	started := time.Now()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("claim: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	q := s.q.WithTx(tx)
	rows, err := q.ClaimCandidates(ctx, sqlc.ClaimCandidatesParams{
		Channel:  sqlc.NotificationsChannel(channel),
		Priority: sqlc.NotificationsPriority(priority),
		Limit:    int32(min(n, 1000)), //nolint:gosec // bounded above
	})
	if err != nil {
		return nil, fmt.Errorf("claim: select: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	affected, err := q.LeaseNotifications(ctx, sqlc.LeaseNotificationsParams{LeaseUs: lease.Microseconds(), Ids: ids})
	if err != nil {
		return nil, fmt.Errorf("claim: lease: %w", RuleViolation(err))
	}
	if affected != int64(len(ids)) {
		// Cannot happen while the row locks are held; never report a lease
		// that was not taken.
		return nil, fmt.Errorf("claim: %d of %d locked rows leased", affected, len(ids))
	}
	// Each claimed call is in the history before any request can leave
	// (decision 0005): committed with the lease, in the same round trips.
	begun, err := q.BeginAttempts(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("claim: attempts: %w", RuleViolation(err))
	}
	if begun != int64(len(ids)) {
		return nil, fmt.Errorf("claim: %d of %d attempts begun", begun, len(ids))
	}
	jobs := make([]notification.Job, 0, len(rows))
	for _, r := range rows {
		jobs = append(jobs, notification.Job{
			ID:       r.ID,
			BatchID:  r.BatchID.String,
			Channel:  channel,
			Priority: priority,
			To:       r.Recipient,
			Content:  r.Content,
			Attempts: int(r.Attempts),
			// Failures counted against the budget so far (decision 0026).
			BudgetSpent: int(r.BudgetSpent),
			// Exact: the row lock is held from the SELECT to the commit.
			ClaimVersion: r.ClaimVersion + 1,
			StatusSeq:    r.StatusSeq + 1, // the same lock: LeaseNotifications added one
			// A lower bound on the local clock: the worker sends only while
			// it holds. The fenced value is claim_version.
			LeaseUntil:    started.Add(lease),
			DueAt:         r.DueAt,
			CorrelationID: r.CorrelationID,
			Traceparent:   r.Traceparent.String,
		})
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim: commit: %w", err)
	}
	return jobs, nil
}
