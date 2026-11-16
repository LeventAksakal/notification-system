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

// rollupSweepLimit bounds one DELETE of metrics rollup rows. A folded minute
// holds 3 channels x (4 attempt outcomes + failed + the sent latency buckets
// in use): 161 rows a minute on the 1M-row bench at 95 sends/s per channel
// with a 3% retry tail up to 300 s, 444 before its shards are folded
// (measured 16 Nov 2026). One statement thus clears about six folded minutes,
// and at the default one-minute sweep interval a pass deletes one minute's
// rows in one short statement on the primary key.
const rollupSweepLimit = 1000

// rollupFoldAfter and rollupFoldSpan set which metrics rollup minutes each
// retention pass folds into shard 0: those that started between
// rollupFoldSpan and rollupFoldAfter ago. A minute is written by the finishes
// of its own provider calls, which end within the 5 s provider timeout, so
// two minutes after it starts it is a minute past its end and its shards
// spread nothing; a rare late write (a reaped call, a late result) still
// counts into any shard and the next pass folds it. The span is eight passes
// at the default one-minute sweep interval: a stalled sweeper catches up, and
// a minute it never folds stays exact, only slower to read.
const (
	rollupFoldAfter = 2 * time.Minute
	rollupFoldSpan  = 10 * time.Minute
)

// retentionDuty is the pause after a step, as a multiple of the step's own duration.
const retentionDuty = 1.0

// Swept counts what one retention pass deleted.
type Swept struct {
	Notifications, Attempts, Batches int64
	// RollupRows counts per-minute metrics rollup rows past the retention.
	RollupRows int64
}

// SweepFinished deletes finished notifications (sent, failed, cancelled) that
// finished before the start of the minute keep reaches back into, on the
// database clock (so a row stays up to a minute past keep), their delivery
// attempts first, then batches left with no notification. It works in steps
// of RetentionBatch until a step finds fewer, so a backlog is cleared in one
// pass without holding locks for long. Pending, processing and scheduled rows
// (finished_at NULL) and templates are never touched. Several sweepers may run
// at once: each skips the rows another holds.
func SweepFinished(ctx context.Context, db *sql.DB, keep time.Duration) (Swept, error) {
	var total Swept
	q := sqlc.New(db)
	now, err := q.MetricsNow(ctx)
	if err != nil {
		return total, fmt.Errorf("retention: database clock: %w", err)
	}
	// Rows and metrics rollups go at the same minute bound (RetentionCandidates).
	before := now.Add(-keep).Truncate(time.Minute)
	for ctx.Err() == nil {
		var n, a int64
		st := time.Now()
		err := retryLockConflict(ctx, func() error {
			var err error
			n, a, err = sweepStep(ctx, db, q, before)
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
	for _, sweep := range []func(context.Context, sqlc.SweepAttemptRollupsParams) (int64, error){
		q.SweepAttemptRollups,
		func(ctx context.Context, p sqlc.SweepAttemptRollupsParams) (int64, error) {
			return q.SweepFinishRollups(ctx, sqlc.SweepFinishRollupsParams(p))
		},
	} {
		for ctx.Err() == nil {
			var r int64
			err := retryLockConflict(ctx, func() error {
				var err error
				r, err = sweep(ctx, sqlc.SweepAttemptRollupsParams{Before: before, Limit: rollupSweepLimit})
				return err
			})
			total.RollupRows += r
			if err != nil {
				return total, fmt.Errorf("retention: metrics rollups: %w", err)
			}
			if r < rollupSweepLimit {
				break
			}
		}
	}
	until := now.Add(-rollupFoldAfter).Truncate(time.Minute)
	if err := retryLockConflict(ctx, func() error { return FoldRollups(ctx, db, until.Add(rollupFoldAfter-rollupFoldSpan), until) }); err != nil {
		return total, err
	}
	return total, nil
}

// FoldRollups adds the shards 1 to 3 of every metrics rollup minute in
// [from, until) into shard 0 and deletes them, one transaction per table:
// the sums the endpoint reads do not change, and a folded minute is one row
// per key, as cheap to read as unsharded rollups (schema/delivery_rollups.sql
// says why minutes are sharded while they are written). REPEATABLE READ, so
// the locking read keeps the range still between the add and the delete.
func FoldRollups(ctx context.Context, db *sql.DB, from, until time.Time) error {
	for _, step := range []struct {
		table        string
		fold, delete func(*sqlc.Queries) (int64, error)
	}{
		{"attempts_per_minute",
			func(q *sqlc.Queries) (int64, error) {
				return q.FoldAttemptRollups(ctx, sqlc.FoldAttemptRollupsParams{FromMinute: from, UntilMinute: until})
			},
			func(q *sqlc.Queries) (int64, error) {
				return q.DeleteFoldedAttemptShards(ctx, sqlc.DeleteFoldedAttemptShardsParams{FromMinute: from, UntilMinute: until})
			}},
		{"finishes_per_minute",
			func(q *sqlc.Queries) (int64, error) {
				return q.FoldFinishRollups(ctx, sqlc.FoldFinishRollupsParams{FromMinute: from, UntilMinute: until})
			},
			func(q *sqlc.Queries) (int64, error) {
				return q.DeleteFoldedFinishShards(ctx, sqlc.DeleteFoldedFinishShardsParams{FromMinute: from, UntilMinute: until})
			}},
	} {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return fmt.Errorf("retention: fold %s: %w", step.table, err)
		}
		q := sqlc.New(tx)
		if _, err := step.fold(q); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("retention: fold %s: %w", step.table, err)
		}
		if _, err := step.delete(q); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("retention: fold %s: %w", step.table, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("retention: fold %s: %w", step.table, err)
		}
	}
	return nil
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
func sweepStep(ctx context.Context, db *sql.DB, q *sqlc.Queries, before time.Time) (notifications, attempts int64, err error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, 0, fmt.Errorf("retention: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	qt := q.WithTx(tx)
	ids, err := qt.RetentionCandidates(ctx, sqlc.RetentionCandidatesParams{Before: sql.NullTime{Time: before, Valid: true}, Limit: RetentionBatch})
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
