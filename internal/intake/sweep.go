package intake

import (
	"context"
	"log/slog"
	"time"

	"github.com/LeventAksakal/notification-system/internal/store"
)

// sweepBatch is the LIMIT of one sweep statement (queries/intake.sql).
const sweepBatch = 1000

// SweepIdempotencyKeys deletes expired Idempotency-Keys every interval until
// ctx ends; it returns nil then. A key is otherwise only forgotten when it is
// reused, so without the sweep the table grows with every keyed create.
// Several API processes may sweep at once: a lost lock race is skipped.
func (s *Service) SweepIdempotencyKeys(ctx context.Context, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		n, err := s.sweepOnce(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil && !store.IsLockConflict(err):
			slog.WarnContext(ctx, "idempotency key sweep", "deleted", n, "err", err)
		case n > 0:
			slog.DebugContext(ctx, "idempotency key sweep", "deleted", n)
		}
	}
}

// sweepOnce deletes expired keys in statements of sweepBatch rows until one
// deletes fewer, so a backlog is cleared in one tick without a long lock.
func (s *Service) sweepOnce(ctx context.Context) (int64, error) {
	var total int64
	for ctx.Err() == nil {
		n, err := s.q.SweepIdempotencyKeys(ctx, s.retention.Milliseconds())
		total += n
		if err != nil {
			return total, err
		}
		if n < sweepBatch {
			break
		}
	}
	return total, nil
}
