package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// reaper returns expired leases to pending and publishes each change.
func (w *Worker) reaper(ctx context.Context) {
	t := time.NewTicker(w.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		jobs, err := w.deps.Queue.ReapExpired(ctx)
		if err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "reap expired leases", "err", err)
		}
		if len(jobs) > 0 {
			slog.InfoContext(ctx, "returned expired leases to pending", "count", len(jobs))
		}
		// What was reaped is committed even when a later batch failed. A
		// reaped job carries no priority: every priority of its channel is
		// hinted.
		var hints []notification.Wake
		seen := map[notification.Channel]bool{}
		now := time.Now()
		for _, job := range jobs {
			if !seen[job.Channel] {
				seen[job.Channel] = true
				for _, p := range notification.Priorities {
					hints = append(hints, notification.Wake{Channel: job.Channel, Priority: p, Due: now})
				}
			}
		}
		w.wakeAll(ctx, hints...)
		w.publishAll(ctx, jobs, notification.StatusPending)
	}
}
