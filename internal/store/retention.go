package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// Bounds of one retention step (decision 0025). A step deletes at most
// RetentionBatch notifications and their attempts (at most APP_MAX_ATTEMPTS
// each per replay; a replayed notification keeps its earlier rows) in one
// short transaction. 500 rows took 68 to 89 ms per DELETE on average
// (max 476 ms) on the Docker Desktop harness, over the 100 ms target, and
// lifted the claim p99 from about 150 ms to 513 ms; hence 100 rows and a
// pause as long as the step after each one, so the sweeper keeps the database
// busy at most half the time.
const (
	RetentionBatch  = 100
	batchSweepLimit = 1000
)

// retentionDuty is the pause after a step, as a multiple of the step's own duration.
const retentionDuty = 1.0

// Swept counts what one retention pass deleted.
type Swept struct {
	Notifications, Attempts, Batches int64
}

// SweepFinished deletes finished notifications (sent, failed, cancelled) whose
// finished_at is older than keep on the database clock, their delivery
// attempts first, then batches left with no notification. It works in steps
// of RetentionBatch until a step finds fewer, so a backlog is cleared in one
// pass without holding locks for long. Pending, processing and scheduled rows
// (finished_at NULL) and templates are never touched. Several sweepers may run
// at once: each skips the rows another holds.
func SweepFinished(ctx context.Context, db *sql.DB, keep time.Duration) (Swept, error) {
	var total Swept
	q := sqlc.New(db)
	for ctx.Err() == nil {
		var n, a int64
		st := time.Now()
		err := retryLockConflict(ctx, func() error {
			var err error
			n, a, err = sweepStep(ctx, db, q, keep)
			return err
		})
		total.Notifications += n
		total.Attempts += a
		if err != nil {
			return total, err
		}
		if n < RetentionBatch {
			break
		}
		pause(ctx, time.Duration(float64(time.Since(st))*retentionDuty))
	}
	for ctx.Err() == nil {
		var b int64
		err := retryLockConflict(ctx, func() error {
			var err error
			b, err = q.SweepBatches(ctx, sqlc.SweepBatchesParams{KeepUs: keep.Microseconds(), Limit: batchSweepLimit})
			return err
		})
		total.Batches += b
		if err != nil {
			return total, fmt.Errorf("retention: batches: %w", err)
		}
		if b < batchSweepLimit {
			break
		}
	}
	return total, nil
}

// pause waits d or until ctx ends.
func pause(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// sweepStep locks one step of expired notifications and deletes their
// attempts, then them, in one READ COMMITTED transaction (no gap locks, like
// the claim). Nothing cascades: each DELETE names its rows.
func sweepStep(ctx context.Context, db *sql.DB, q *sqlc.Queries, keep time.Duration) (notifications, attempts int64, err error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, 0, fmt.Errorf("retention: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	qt := q.WithTx(tx)
	ids, err := qt.RetentionCandidates(ctx, sqlc.RetentionCandidatesParams{KeepUs: keep.Microseconds(), Limit: RetentionBatch})
	if err != nil {
		return 0, 0, fmt.Errorf("retention: select: %w", err)
	}
	if len(ids) == 0 {
		return 0, 0, nil
	}
	if attempts, err = qt.DeleteAttemptsOf(ctx, ids); err != nil {
		return 0, 0, fmt.Errorf("retention: delete attempts: %w", err)
	}
	if notifications, err = qt.DeleteNotifications(ctx, ids); err != nil {
		return 0, 0, fmt.Errorf("retention: delete notifications: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("retention: commit: %w", err)
	}
	return notifications, attempts, nil
}
