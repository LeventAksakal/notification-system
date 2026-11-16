package delivery

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
)

// sendAt sends job at its slot moment at, or releases it unsent when that
// cannot happen within tolerance or a clock step has voided the slot's
// generation gen. Its result is published after announced is closed (nil:
// at once).
func (w *Worker) sendAt(ctx, jctx context.Context, cr *channelRun, job notification.Job, announced <-chan struct{}, at time.Time, tk health.Ticket, tolerance time.Duration, gen uint64) {
	if w.spent(jctx, cr, job, announced, tk) {
		return
	}
	sleep(ctx, time.Until(at))
	if ctx.Err() != nil {
		w.cancel(ctx, cr, tk)
		w.release(jctx, job, announced, "shutdown before the send")
		return
	}
	late := at.Add(tolerance)
	give, ok := cr.calling.take(ctx, late)
	if !ok {
		w.cancel(ctx, cr, tk)
		reason := "no free provider call at its slot"
		if ctx.Err() != nil {
			reason = "shutdown before the send"
		} else {
			w.m.gaveUp(ctx, cr.ch, giveUpNoFreeCall, 1)
		}
		w.release(jctx, job, announced, reason)
		return
	}
	// The place is held for the provider call only, not for the database
	// writes around it.
	var once sync.Once
	callDone := func() { once.Do(give) }
	defer callDone()
	if time.Now().After(late) {
		callDone()
		w.cancel(ctx, cr, tk)
		w.m.gaveUp(ctx, cr.ch, giveUpSlotMissed, 1)
		w.release(jctx, job, announced, "slot missed")
		return
	}
	if !w.clock.current(gen) {
		// A forward clock step of an interval or more (clockMap.gen): this
		// slot is on the old mapping and could leave beside the new one's.
		callDone()
		w.cancel(ctx, cr, tk)
		w.m.gaveUp(ctx, cr.ch, giveUpClockStepped, 1)
		w.release(jctx, job, announced, "limiter clock stepped")
		return
	}
	if !cr.gate.Current(tk) {
		// An outage was declared after the claim, as this worker last saw the
		// shared health: nothing goes out (decision 0026).
		callDone()
		w.m.gaveUp(ctx, cr.ch, giveUpBreakerOpened, 1)
		w.release(jctx, job, announced, "provider outage declared before the send")
		return
	}
	// Slot and leaving moment on this process's clock: beside the provider's
	// arrival times they tell a dense grid from requests bunched in transit.
	slog.DebugContext(jctx, "request leaving", "channel", cr.ch, "id", job.ID, "slot_us", at.UnixMicro(), "leave_us", time.Now().UnixMicro())
	w.deliver(jctx, job, announced, cr.gate, tk, callDone)
}
