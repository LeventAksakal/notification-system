package store

import (
	"context"
	"math/rand/v2"
	"time"
)

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
