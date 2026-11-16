package intake

import (
	"context"
	"log/slog"
	"time"

	"github.com/LeventAksakal/notification-system/internal/store"
)

// SweepFinished deletes finished notifications older than keep, with their
// attempts and emptied batches, every interval until ctx ends; it returns nil
// then (decision 0025). It runs beside SweepIdempotencyKeys in every API
// process: concurrent sweepers skip each other's rows.
func (s *Service) SweepFinished(ctx context.Context, interval, keep time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		n, err := store.SweepFinished(ctx, s.db, keep)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil && !store.IsLockConflict(err):
			slog.WarnContext(ctx, "retention sweep", "notifications", n.Notifications, "attempts", n.Attempts, "batches", n.Batches, "err", err)
		case n.Notifications > 0 || n.Batches > 0:
			slog.InfoContext(ctx, "retention sweep", "notifications", n.Notifications, "attempts", n.Attempts, "batches", n.Batches)
		}
	}
}
