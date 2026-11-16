package store

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// ReapExpired returns notifications whose lease ran out to pending and
// reports them (ID, BatchID, Channel, Attempts, CorrelationID), so the caller
// can publish the change. Each batch of up to 1000 is locked, updated and
// committed in one READ COMMITTED transaction; concurrent reapers skip each
// other's rows.
func (s *Queue) ReapExpired(ctx context.Context) ([]notification.Job, error) {
	if err := notification.Transition(notification.StatusProcessing, notification.StatusPending); err != nil {
		return nil, err
	}
	var all []notification.Job
	for {
		var jobs []notification.Job
		err := retryLockConflict(ctx, func() error {
			var err error
			jobs, err = s.reapOnce(ctx)
			return err
		})
		all = append(all, jobs...)
		if err != nil {
			return all, err
		}
		if len(jobs) < reapBatch {
			return all, nil
		}
	}
}

// reapBatch is the LIMIT of ReapCandidates.
const reapBatch = 1000

func (s *Queue) reapOnce(ctx context.Context) ([]notification.Job, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("reap: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	q := s.q.WithTx(tx)
	rows, err := q.ReapCandidates(ctx)
	if err != nil {
		return nil, fmt.Errorf("reap: select: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	ids := make([]string, len(rows))
	jobs := make([]notification.Job, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
		jobs[i] = notification.Job{
			ID: r.ID, BatchID: r.BatchID.String, Channel: notification.Channel(r.Channel),
			Attempts: int(r.Attempts), CorrelationID: r.CorrelationID,
			StatusSeq: r.StatusSeq + 1, // ReapLeases adds one under the lock taken here
		}
	}
	// An attempt in flight on an expired lease becomes unknown and counts.
	inFlight, err := q.InFlightOf(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("reap: attempts in flight: %w", err)
	}
	if len(inFlight) > 0 {
		if _, err := q.UnknownOnReap(ctx, ids); err != nil {
			return nil, fmt.Errorf("reap: unknown attempts: %w", RuleViolation(err))
		}
		for i := range jobs {
			if slices.Contains(inFlight, jobs[i].ID) {
				jobs[i].Attempts++
			}
		}
	}
	n, err := q.ReapLeases(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("reap: update: %w", RuleViolation(err))
	}
	if n != int64(len(ids)) {
		// Cannot happen while the row locks are held; report nothing rather than a wrong list.
		return nil, fmt.Errorf("reap: %d of %d locked rows changed", n, len(ids))
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("reap: commit: %w", err)
	}
	return jobs, nil
}
