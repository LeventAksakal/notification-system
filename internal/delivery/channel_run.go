package delivery

import (
	"sync"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
)

// channelRun is what a channel's loop, its claim groups and its sends share.
type channelRun struct {
	ch   notification.Channel
	gate *health.Gate // the channel's shared provider health, as this worker sees it
	// calling bounds the provider calls in flight on this channel. While no
	// place is free the loop reserves no slots.
	calling callPlaces
	// claiming holds the one claim in flight on this channel. The next slots
	// are reserved only once the previous claim is done: a claim normally
	// takes a fraction of refill, and while the database is slow this worker
	// reserves less and the other workers take the slots. Claims of one
	// worker so stay in slot order, and a slot is reserved only by a worker
	// that can claim for it at once; overlapping claims near the end of a
	// backlog would hand the last rows to later slots and leave earlier ones
	// empty.
	claiming chan struct{}
	// latency is this worker's measured claim time on the channel: written
	// by the claim in flight, read by the loop before each reservation.
	latency claimLatency
	// wake cuts the channel loop's idle wait short when a hint arrives.
	wake chan struct{}

	mu  sync.Mutex
	sel *selector
}

func (w *Worker) newChannelRun(ch notification.Channel, calling callPlaces) *channelRun {
	return &channelRun{
		ch: ch, gate: w.gates[ch],
		calling: calling, claiming: make(chan struct{}, 1), wake: make(chan struct{}, 1),
		sel: newSelector(w.cfg.Weights),
	}
}

// pause makes the channel loop reserve nothing until until.
func (cr *channelRun) pause(until time.Time) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	for i := range notification.Priorities {
		cr.sel.emptyUntil[i], cr.sel.check[i] = until, false
	}
}
