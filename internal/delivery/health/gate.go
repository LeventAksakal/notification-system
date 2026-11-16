package health

import (
	"context"
	"sync"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// Ticket ties a call to the state generation that admitted it.
type Ticket struct {
	gen   int64
	probe bool
	until time.Time // a probe's lease end on the local clock
}

// Probe reports whether this ticket is the probe: the caller makes exactly
// one call with it.
func (t Ticket) Probe() bool { return t.probe }

// Gate is one worker's view of one channel's shared health. It keeps the
// newest reply it has seen (largest generation) from any of its steps, so a
// send can check locally, without a round trip, that its ticket still holds.
// Safe for concurrent use.
type Gate struct {
	state State
	ch    notification.Channel

	mu   sync.Mutex
	gen  int64
	open bool
}

// NewGate returns the gate of ch on state.
func NewGate(state State, ch notification.Channel) *Gate { return &Gate{state: state, ch: ch} }

// Allow asks whether a call may start. When it may not, wait is how long
// until asking again makes sense. A probe is held for probeLease.
func (g *Gate) Allow(ctx context.Context, probeLease time.Duration) (t Ticket, ok bool, wait time.Duration, err error) {
	g.mu.Lock()
	seen := g.gen
	g.mu.Unlock()
	asked := time.Now()
	r, err := g.state.Admit(ctx, g.ch, probeLease, seen)
	if err != nil {
		return Ticket{}, false, 0, err
	}
	g.observe(r)
	switch r.Kind {
	case Closed:
		return Ticket{gen: r.Gen}, true, 0, nil
	case Probe:
		return Ticket{gen: r.Gen, probe: true, until: asked.Add(probeLease)}, true, 0, nil
	}
	return Ticket{}, false, r.Wait, nil
}

// Done reports the result of a call that reached the provider (or timed out)
// and returns the state after it: Reply.Healthy says whether a failure counts
// against its message's budget.
func (g *Gate) Done(ctx context.Context, t Ticket, fault bool, retryAfter time.Duration) (Reply, error) {
	r, err := g.state.Report(ctx, g.ch, t.gen, t.probe, fault, retryAfter)
	if err != nil {
		return Reply{}, err
	}
	g.observe(r)
	return r, nil
}

// Cancel ends a ticket whose call was not made. Only a probe needs it: the
// next caller may then probe at once. If the state cannot be reached, the
// probe's lease frees it.
func (g *Gate) Cancel(ctx context.Context, t Ticket) error {
	if !t.probe {
		return nil
	}
	r, err := g.state.Cancel(ctx, g.ch, t.gen)
	if err != nil {
		return err
	}
	g.observe(r)
	return nil
}

// Current reports whether t still holds in this worker's newest view: a
// call admitted while closed does not once an outage was declared, and a
// probe does not once another generation began or its lease ran out (the
// state may have handed the probe to another worker by then).
func (g *Gate) Current(t Ticket) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if t.probe {
		return g.gen == t.gen && time.Now().Before(t.until)
	}
	return !g.open && g.gen == t.gen
}

func (g *Gate) observe(r Reply) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if r.Gen < g.gen {
		return // an older reply that arrived late
	}
	g.gen, g.open = r.Gen, r.Kind != Closed
}
