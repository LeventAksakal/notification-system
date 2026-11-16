package delivery

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
)

// giveUpAttemptsSpent: calls whose result is unknown (counted by the reaper,
// decision 0005) already spent the budget; the notification fails without a
// call.
const giveUpAttemptsSpent giveUp = "attempts_spent"

// spent ends job as failed (exhausted) without a call when its budget spent
// already reached MaxAttempts, and reports whether it did. The reaper counts
// a call left in flight by a dead worker (a message that kills its worker
// must end), outside the result path that checks the budget (decision 0026).
func (w *Worker) spent(ctx context.Context, cr *channelRun, job notification.Job, announced <-chan struct{}, tk health.Ticket) bool {
	if job.BudgetSpent < w.cfg.MaxAttempts {
		return false
	}
	w.cancel(ctx, cr, tk)
	w.m.gaveUp(ctx, job.Channel, giveUpAttemptsSpent, 1)
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()
	reason := fmt.Sprintf("attempt budget spent (%d of %d calls counted), some without a recorded result", job.BudgetSpent, job.Attempts)
	if err := w.deps.Queue.MarkFailed(wctx, job, notification.Attempt{Error: reason}, notification.FailureExhausted); err != nil {
		slog.WarnContext(ctx, "failing a spent notification failed; the lease will expire", "err", err)
		return true
	}
	slog.InfoContext(ctx, "attempt budget spent by unknown calls; failed without a call", "channel", job.Channel,
		"attempts", job.Attempts, "budget_spent", job.BudgetSpent)
	w.publish(ctx, job, announced, notification.StatusFailed, job.Attempts, reason)
	return true
}
