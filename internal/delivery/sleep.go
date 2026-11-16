package delivery

import (
	"context"
	"time"
)

// pauseWait spaces a channel's reservations (the pause after a Take, see
// horizon) but ends early when a hint or due time makes a priority that was
// found empty claimable again (selector.revived): that row is not behind
// any reservation, and on an idle channel the pause was the only thing it
// waited for (75-85 ms of the 100 ms refill measured for a row posted right
// after a lone send, TestGapDeliveryR7SecondRowAfterALoneSend). A channel
// with a backlog has no empty priority to revive, so its Takes stay a refill
// apart. It reports whether it ended early: the loop was then woken like
// from idle, as the other workers were by the same hint (woke).
func (w *Worker) pauseWait(ctx context.Context, cr *channelRun, d time.Duration) bool {
	if d <= 0 {
		return false
	}
	t := time.NewTimer(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return false
		case <-cr.wake:
			cr.mu.Lock()
			revived := cr.sel.revived
			cr.sel.revived = false
			cr.mu.Unlock()
			if revived {
				return true
			}
		}
	}
}

// idleWait sleeps for d, until ctx ends, or until a wake hint for the channel.
func (w *Worker) idleWait(ctx context.Context, cr *channelRun, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	case <-cr.wake:
	}
}

func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
