// Package health is the provider health of each channel, shared by every
// worker (decision 0026). One state per channel lives in Valkey and changes
// only in one atomic step per call (ValkeyState), so the fleet learns an
// outage once, sends one probe per cool-down, and resumes together.
//
// The rules, which ValkeyState (Lua) and MemoryState (Go, for tests) both
// implement:
//
//   - Closed: calls flow. Failures counts consecutive provider-fault results
//     fleet-wide; any other result resets it. The Failures-th fault declares an
//     outage: the state opens and no probe may go before the cool-down (or the
//     failure's Retry-After, if longer) has passed.
//   - Open: nobody calls, except one probe per cool-down. The first worker to
//     ask once the cool-down is over gets the probe, held for the probe lease
//     it asked for; while it is out every other worker waits. A probe that
//     succeeds closes the state; one that fails restarts the cool-down. A
//     probe whose holder died is freed by its lease expiry. A result from an
//     older generation changes nothing.
//   - Every step returns the state after it. A failure counts against its
//     message's attempt budget only if that state is closed: the failure that
//     declares an outage, every failure of a call in flight when it was
//     declared and every probe failure are recorded but not counted.
//   - Generations are the state's clock (Valkey's, in microseconds) at the
//     last change, never less than the previous one plus 1, and never less
//     than one the asking worker has seen plus 1: they keep growing when
//     Valkey loses the state or restores an older one, so a worker's cached
//     view (newest generation wins) never sticks on a state that is gone.
package health

import (
	"context"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// Config is the health dials.
type Config struct {
	// Failures is APP_BREAKER_FAILURES: consecutive provider-fault results on
	// a channel, fleet-wide, that declare an outage.
	Failures int
	// Cooldown is APP_BREAKER_COOLDOWN: the least time between an outage
	// being declared (or a probe failing) and the next probe.
	Cooldown time.Duration
}

// Kind is what a step found.
type Kind int

// Closed: calls flow. Probe: the caller holds the probe and makes exactly one
// call with it. Wait: open, the caller may ask again after Reply.Wait. Open:
// the state is open (a report or cancel reply; Reply.Wait is the time until
// the next probe may go).
const (
	Closed Kind = iota
	Probe
	Wait
	Open
)

func (k Kind) String() string {
	switch k {
	case Probe:
		return "probe"
	case Wait:
		return "wait"
	case Open:
		return "open"
	}
	return "closed"
}

// Transition is the change a report made, for the one log line per outage.
type Transition int

// None: no change of state. Opened: this report declared an outage.
// Recovered: this report (a successful probe) ended it.
const (
	None Transition = iota
	Opened
	Recovered
)

// Reply is the state after one step.
type Reply struct {
	Kind       Kind
	Gen        int64         // generation of the state after the step
	Wait       time.Duration // Wait and Open: until asking again (or probing) makes sense
	Transition Transition
}

// Healthy reports whether the state after the step is closed: a failure
// reported in this step counts against its message's budget.
func (r Reply) Healthy() bool { return r.Kind == Closed || r.Kind == Probe }

// State is where the channels' health is kept. Every method is one atomic
// step and returns the state after it; an error means the state could not be
// reached.
type State interface {
	// Admit asks whether a call may start on ch. It returns Closed, Probe
	// (the caller holds the probe for probeLease) or Wait. seen is the newest
	// generation the caller has seen; a state below it (restored from an
	// older snapshot) is raised above it, so the caller's tickets hold again.
	Admit(ctx context.Context, ch notification.Channel, probeLease time.Duration, seen int64) (Reply, error)
	// Report records the result of a call admitted under generation gen
	// (probe: it was the probe). fault is provider.Verdict.ProviderFault;
	// retryAfter is the provider's Retry-After (0 if none).
	Report(ctx context.Context, ch notification.Channel, gen int64, probe, fault bool, retryAfter time.Duration) (Reply, error)
	// Cancel frees the probe of generation gen that made no call (nothing to
	// claim, shutdown): the next caller may probe at once.
	Cancel(ctx context.Context, ch notification.Channel, gen int64) (Reply, error)
}
