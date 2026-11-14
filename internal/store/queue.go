package store

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"time"
	"unicode/utf8"

	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// Queue is the MySQL core.Queue (decisions 0003, 0018).
type Queue struct {
	db *sql.DB
	q  *sqlc.Queries
}

var _ core.Queue = (*Queue)(nil)

// NewQueue returns the queue over db (opened with Open).
func NewQueue(db *sql.DB) *Queue { return &Queue{db: db, q: sqlc.New(db)} }

// maxProviderIDLen is the width of provider_message_id.
const maxProviderIDLen = 255

// Claim leases up to n due pending notifications of one channel and priority.
// The SELECT ... FOR UPDATE SKIP LOCKED runs through claim_idx in a READ
// COMMITTED transaction, so concurrent claimers never wait on each other and
// never get the same row.
func (s *Queue) Claim(ctx context.Context, channel core.Channel, priority core.Priority, n int, lease time.Duration) ([]core.Job, error) {
	if n <= 0 {
		return nil, nil
	}
	if err := core.Transition(core.StatusPending, core.StatusProcessing); err != nil {
		return nil, err
	}
	var jobs []core.Job
	err := retryLockConflict(ctx, func() error {
		var err error
		jobs, err = s.claimOnce(ctx, channel, priority, n, lease)
		return err
	})
	return jobs, err
}

func (s *Queue) claimOnce(ctx context.Context, channel core.Channel, priority core.Priority, n int, lease time.Duration) ([]core.Job, error) {
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
		return nil, fmt.Errorf("claim: lease: %w", err)
	}
	if affected != int64(len(ids)) {
		// Cannot happen while the row locks are held; never report a lease
		// that was not taken.
		return nil, fmt.Errorf("claim: %d of %d locked rows leased", affected, len(ids))
	}
	jobs := make([]core.Job, 0, len(rows))
	for _, r := range rows {
		jobs = append(jobs, core.Job{
			ID:       r.ID,
			BatchID:  r.BatchID.String,
			Channel:  channel,
			Priority: priority,
			To:       r.Recipient,
			Content:  r.Content,
			Attempts: int(r.Attempts),
			// Exact: the row lock is held from the SELECT to the commit.
			ClaimVersion: r.ClaimVersion + 1,
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

// MarkSent records the accepted attempt and ends the notification as sent.
func (s *Queue) MarkSent(ctx context.Context, job core.Job, a core.Attempt) error {
	return s.finish(ctx, job, a, core.StatusSent, func(q *sqlc.Queries) (int64, error) {
		return q.FinishSent(ctx, sqlc.FinishSentParams{
			ProviderMessageID: nullString(cut(a.ProviderMessageID, maxProviderIDLen)),
			ID:                job.ID,
			ClaimVersion:      job.ClaimVersion,
		})
	})
}

// Reschedule records the failed attempt and returns the notification to
// pending, due at dueAt. The delay is applied on the database clock.
func (s *Queue) Reschedule(ctx context.Context, job core.Job, a core.Attempt, dueAt time.Time) error {
	return s.finish(ctx, job, a, core.StatusPending, func(q *sqlc.Queries) (int64, error) {
		return q.FinishRetry(ctx, sqlc.FinishRetryParams{
			LastError:    nullString(cut(a.Error, core.MaxErrorLen)),
			DelayUs:      delayUs(dueAt),
			ID:           job.ID,
			ClaimVersion: job.ClaimVersion,
		})
	})
}

// MarkFailed records the failed attempt and ends the notification as failed.
func (s *Queue) MarkFailed(ctx context.Context, job core.Job, a core.Attempt, kind core.FailureKind) error {
	return s.finish(ctx, job, a, core.StatusFailed, func(q *sqlc.Queries) (int64, error) {
		return q.FinishFailed(ctx, sqlc.FinishFailedParams{
			LastError:    nullString(cut(a.Error, core.MaxErrorLen)),
			FailureKind:  sqlc.NullNotificationsFailureKind{NotificationsFailureKind: sqlc.NotificationsFailureKind(kind), Valid: true},
			ID:           job.ID,
			ClaimVersion: job.ClaimVersion,
		})
	})
}

// finish updates the claimed row (fenced by claim_version) and inserts its
// delivery_attempts row in one transaction.
func (s *Queue) finish(ctx context.Context, job core.Job, a core.Attempt, to core.Status, update func(*sqlc.Queries) (int64, error)) error {
	if err := core.Transition(core.StatusProcessing, to); err != nil {
		return err
	}
	return retryLockConflict(ctx, func() error { return s.finishOnce(ctx, job, a, to, update) })
}

func (s *Queue) finishOnce(ctx context.Context, job core.Job, a core.Attempt, to core.Status, update func(*sqlc.Queries) (int64, error)) error {
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
		return fmt.Errorf("finish %s as %s: %w", job.ID, to, err)
	}
	if affected == 0 {
		return fmt.Errorf("finish %s as %s: %w", job.ID, to, core.ErrLeaseLost)
	}
	var status sql.NullInt16
	if a.HTTPStatus > 0 {
		status = sql.NullInt16{Int16: int16(min(a.HTTPStatus, 999)), Valid: true} //nolint:gosec // bounded
	}
	err = q.InsertDeliveryAttempt(ctx, sqlc.InsertDeliveryAttemptParams{
		NotificationID:    job.ID,
		AttemptNo:         uint32(job.Attempts + 1), //nolint:gosec // attempts are small and non-negative
		Channel:           sqlc.DeliveryAttemptsChannel(job.Channel),
		StartedAt:         a.StartedAt.UTC(),
		DurationMs:        uint32(max(a.Duration.Milliseconds(), 0)), //nolint:gosec // non-negative, bounded by the timeout
		Outcome:           sqlc.DeliveryAttemptsOutcome(a.Outcome),
		HttpStatus:        status,
		Error:             nullString(cut(a.Error, core.MaxErrorLen)),
		ProviderMessageID: nullString(cut(a.ProviderMessageID, maxProviderIDLen)),
	})
	if err != nil {
		return fmt.Errorf("finish %s: insert attempt: %w", job.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finish %s: commit: %w", job.ID, err)
	}
	return nil
}

// Release returns a claimed notification to pending without an attempt.
func (s *Queue) Release(ctx context.Context, job core.Job, dueAt time.Time) error {
	if err := core.Transition(core.StatusProcessing, core.StatusPending); err != nil {
		return err
	}
	// One autocommit statement: a deadlock or lock wait timeout rolled it
	// back, so trying it again cannot apply it twice.
	return retryLockConflict(ctx, func() error {
		affected, err := s.q.ReleaseLease(ctx, sqlc.ReleaseLeaseParams{DelayUs: delayUs(dueAt), ID: job.ID, ClaimVersion: job.ClaimVersion})
		if err != nil {
			return fmt.Errorf("release %s: %w", job.ID, err)
		}
		if affected == 0 {
			return fmt.Errorf("release %s: %w", job.ID, core.ErrLeaseLost)
		}
		return nil
	})
}

// ReapExpired returns notifications whose lease ran out to pending and
// reports them (ID, BatchID, Channel, Attempts, CorrelationID), so the caller
// can publish the change. Each batch of up to 1000 is locked, updated and
// committed in one READ COMMITTED transaction; concurrent reapers skip each
// other's rows.
func (s *Queue) ReapExpired(ctx context.Context) ([]core.Job, error) {
	if err := core.Transition(core.StatusProcessing, core.StatusPending); err != nil {
		return nil, err
	}
	var all []core.Job
	for {
		var jobs []core.Job
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

func (s *Queue) reapOnce(ctx context.Context) ([]core.Job, error) {
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
	jobs := make([]core.Job, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
		jobs[i] = core.Job{
			ID: r.ID, BatchID: r.BatchID.String, Channel: core.Channel(r.Channel),
			Attempts: int(r.Attempts), CorrelationID: r.CorrelationID,
		}
	}
	n, err := q.ReapLeases(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("reap: update: %w", err)
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

// lockRetries is how many times in all a transaction is run while InnoDB
// rolls it back as a deadlock victim or after a lock wait timeout.
const lockRetries = 4

// lockBackoff is the mean wait before the first retry; the n-th retry waits
// n times as long, jittered by ±50% so two victims of one deadlock do not
// collide again in step.
const lockBackoff = 2 * time.Millisecond

// lockJitter returns a uniform value in [0, n); a variable so a test can pin it.
var lockJitter = func(n int64) int64 { return rand.Int64N(n) } //nolint:gosec // jitter, not security

// retryLockConflict runs fn again when InnoDB rolled it back for a lock
// conflict; fn must be a whole transaction (or one autocommit statement),
// never a single statement inside one. A failed fn must leave nothing
// committed: a lock conflict error guarantees it, since the statement that
// got it was rolled back and the transaction is rolled back before fn returns.
// It gives up when ctx is done and returns the last error.
func retryLockConflict(ctx context.Context, fn func() error) error {
	var err error
	for i := range lockRetries {
		if err = fn(); err == nil || !IsLockConflict(err) {
			return err
		}
		if i == lockRetries-1 {
			break
		}
		base := int64(i+1) * int64(lockBackoff)
		t := time.NewTimer(time.Duration(base/2 + lockJitter(base)))
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
	}
	return err
}

// delayUs is how far dueAt lies ahead of the local clock, in microseconds; the
// due time is then computed on the database clock (NOW(3) + delay).
func delayUs(dueAt time.Time) int64 {
	return max(time.Until(dueAt).Microseconds(), 0)
}

func nullString(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

// cut shortens s to at most n characters, on a rune boundary.
func cut(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n])
}
