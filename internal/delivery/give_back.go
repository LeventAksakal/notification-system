package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// giveBack returns the unused slots of grant g, n of them, from the moment
// from on (the limiter's clock) to the limiter, when the claim found fewer
// rows than slots, or none: the next
// Take, of any worker, then gets them instead of slots up to a horizon
// further out, and a row due right after a backlog ended finds the grid free
// (group.flex) rather than behind the unused reservation. It is one Valkey
// call per group that ended a backlog, made while the channel's claiming
// place is still held, so this worker's next Take sees its effect. A failure
// only leaves the slots unused, as they were before.
// A group sent early (group.flex) gives back from where its sends ended, not
// from its unused slot: a row due right after a lone send then waits about
// one interval, not a lead plus a horizon.
func (w *Worker) giveBack(ctx context.Context, cr *channelRun, g notification.Grant, from time.Time, n int) {
	ok, err := w.deps.Limiter.GiveBack(ctx, cr.ch, g, from)
	if err != nil && ctx.Err() == nil {
		slog.WarnContext(ctx, "giving unused send slots back failed", "channel", cr.ch, "slots", n, "err", err)
		return
	}
	slog.DebugContext(ctx, "unused send slots given back", "channel", cr.ch, "slots", n, "given_back", ok)
}
