package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// release returns an unsent job to pending, due now, without an attempt, and
// publishes that after announced is closed.
func (w *Worker) release(ctx context.Context, job notification.Job, announced <-chan struct{}, reason string) {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()
	if err := w.deps.Queue.Release(wctx, job, time.Now()); err != nil {
		slog.WarnContext(ctx, "release failed; the lease will expire", "reason", reason, "err", err)
		return
	}
	slog.DebugContext(ctx, "released unsent notification", "channel", job.Channel, "reason", reason)
	w.wakeAll(ctx, notification.Wake{Channel: job.Channel, Priority: job.Priority, Due: time.Now()})
	w.publish(ctx, job, announced, notification.StatusPending, job.Attempts, "", job.StatusSeq+1) // fenced, as a result
}
