// Package breaker is the per-channel circuit breaker of one worker process
// (decision 0008). While it is open the worker does not claim the channel's
// notifications, so an outage burns no attempts.
package breaker

import (
	"sync"
	"time"
)

// State is where a breaker is.
type State int

// Closed: calls flow. Open: no calls until the cool-down ends. HalfOpen: one
// probe call is in flight; its result closes or reopens the breaker.
const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	}
	return "closed"
}

// Config is the breaker's dials (APP_BREAKER_*).
type Config struct {
	Window       time.Duration // failure ratio measured over this window
	MinRequests  int           // calls in the window before the ratio counts
	FailureRatio float64       // opens at or above
	Cooldown     time.Duration // open time before one probe
}

// probeWait is how long Allow asks the caller to wait while the probe is out.
const probeWait = 20 * time.Millisecond

// Ticket ties a call to the breaker state that admitted it. Every state
// change starts a new generation; a result carrying an older generation is
// ignored, so a call admitted while closed can never decide a half-open
// breaker, and only the probe's own result (or Cancel) ends a half-open state.
type Ticket struct {
	gen   uint64
	probe bool
}

// Probe reports whether this ticket is the half-open probe: the caller should
// make exactly one call with it.
func (t Ticket) Probe() bool { return t.probe }

type result struct {
	at   time.Time
	fail bool
}

// Breaker is safe for concurrent use. Only real provider answers and
// timeouts are recorded (Done); a call that never happened is Cancelled.
type Breaker struct {
	cfg Config
	now func() time.Time

	mu       sync.Mutex
	state    State
	gen      uint64 // bumped on every state change
	openedAt time.Time
	results  []result // closed state only, oldest first
	fails    int      // failures in results
}

// New returns a closed breaker reading time from now (time.Now in production).
func New(cfg Config, now func() time.Time) *Breaker {
	return &Breaker{cfg: cfg, now: now}
}

// State reports the current state, moving open to half-open only through Allow.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow reports whether a call may start now and returns its ticket, which
// the call must end with Done or Cancel. When it may not, wait is how long
// until asking again makes sense. In half-open exactly one caller is allowed:
// the probe.
func (b *Breaker) Allow() (Ticket, bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		if left := b.openedAt.Add(b.cfg.Cooldown).Sub(b.now()); left > 0 {
			return Ticket{}, false, left
		}
		b.set(HalfOpen)
		return Ticket{gen: b.gen, probe: true}, true, 0
	case HalfOpen:
		return Ticket{}, false, probeWait
	}
	return Ticket{gen: b.gen}, true, 0
}

// Done records the result of a call that reached the provider (or timed
// out). A ticket from an older generation changes nothing.
func (b *Breaker) Done(t Ticket, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t.gen != b.gen {
		return
	}
	now := b.now()
	switch b.state {
	case HalfOpen:
		if !t.probe {
			return
		}
		if success {
			b.close()
		} else {
			b.open(now)
		}
	case Closed:
		b.results = append(b.results, result{at: now, fail: !success})
		if !success {
			b.fails++
		}
		b.prune(now)
		if len(b.results) >= b.cfg.MinRequests &&
			float64(b.fails)/float64(len(b.results)) >= b.cfg.FailureRatio {
			b.open(now)
		}
	case Open:
		// Unreachable: opening starts a new generation.
	}
}

// Cancel ends an allowed call that never reached the provider (nothing to
// claim, shutdown). A cancelled probe lets the next caller probe; any other
// cancel changes nothing.
func (b *Breaker) Cancel(t Ticket) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t.gen != b.gen || !t.probe || b.state != HalfOpen {
		return
	}
	// Back to open with the cool-down already spent: the next Allow probes.
	b.set(Open)
	b.openedAt = b.now().Add(-b.cfg.Cooldown)
}

// Current reports whether t still belongs to the breaker's current state: a
// call admitted while closed is not current once the breaker opened, and
// should not be made.
func (b *Breaker) Current(t Ticket) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return t.gen == b.gen
}

func (b *Breaker) set(s State) {
	b.state = s
	b.gen++
}

func (b *Breaker) open(now time.Time) {
	b.set(Open)
	b.openedAt = now
	b.results, b.fails = b.results[:0], 0
}

func (b *Breaker) close() {
	b.set(Closed)
	b.results, b.fails = b.results[:0], 0
}

// prune drops results older than the window.
func (b *Breaker) prune(now time.Time) {
	cut := now.Add(-b.cfg.Window)
	i := 0
	for i < len(b.results) && !b.results[i].at.After(cut) {
		if b.results[i].fail {
			b.fails--
		}
		i++
	}
	if i > 0 {
		b.results = append(b.results[:0], b.results[i:]...)
	}
}
