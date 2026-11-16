package delivery

import (
	"context"
	"log/slog"
	"time"
)

// settleEmpty asks the database when the channel's next notification of each
// priority in idx is due and sleeps each until then: nothing pending, or due
// later than SafetyPoll, sleeps SafetyPoll as a safety check (a lost hint
// costs at most that); due within it sleeps exactly until it (the wait is on
// the database's clock, so skew does not matter); due now sleeps dueNow (a
// row this round did not get: another claimer holds it, or it was committed
// after the claim began, in which case its hint wakes the channel anyway).
// It runs inside round r, holding the channel's claiming place.
func (w *Worker) settleEmpty(ctx context.Context, cr *channelRun, idx []int, r round, dueNow time.Duration) {
	next, err := w.deps.Queue.NextDue(ctx, cr.ch)
	now := time.Now()
	if err != nil && ctx.Err() == nil {
		slog.ErrorContext(ctx, "reading the next due time failed", "channel", cr.ch, "err", err)
	}
	cr.mu.Lock()
	defer cr.mu.Unlock()
	defer func() {
		if cr.sel.revived { // a hint kept over this round's "empty": wake the loop's pause
			select {
			case cr.wake <- struct{}{}:
			default:
			}
		}
	}()
	for _, i := range idx {
		wait, pending := next[priorityName(i)]
		switch {
		case err != nil:
			cr.sel.settle(i, now.Add(w.cfg.IdleMax), true, r) // ask again after the claim-failure pause
		case !pending || wait >= w.cfg.SafetyPoll:
			cr.sel.settle(i, now.Add(w.cfg.SafetyPoll), true, r)
		case wait > 0:
			cr.sel.settle(i, now.Add(wait), false, r)
		default:
			cr.sel.settle(i, now.Add(dueNow), false, r)
			cr.sel.raced[i] = dueNow > 0
		}
	}
}

// peek runs a round of NextDue alone for the priorities whose safety poll
// came due; one due now is claimed at once.
func (w *Worker) peek(ctx context.Context, cr *channelRun, idx []int) {
	select {
	case cr.claiming <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-cr.claiming }()
	cr.mu.Lock()
	r := cr.sel.beginRound()
	cr.mu.Unlock()
	w.settleEmpty(ctx, cr, idx, r, 0)
}
