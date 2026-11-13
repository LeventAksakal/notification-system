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
	openedAt time.Time
	results  []result // closed state only, oldest first
	fails    int      // failures in results
}

// New returns a closed breaker reading time from now (time.Now in production).
func New(cfg Config, now func() time.Time) *Breaker {
	return &Breaker{cfg: cfg, now: now}
}

// State reports the current state, moving open to half-open-eligible only
// through Allow.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow reports whether a call may start now. When it may not, wait is how
// long until asking again makes sense. In half-open exactly one caller is
// allowed (the probe); it must end with Done or Cancel.
func (b *Breaker) Allow() (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		if left := b.openedAt.Add(b.cfg.Cooldown).Sub(b.now()); left > 0 {
			return false, left
		}
		b.state = HalfOpen
		return true, 0
	case HalfOpen:
		return false, probeWait
	}
	return true, 0
}

// Done records the result of a call that reached the provider (or timed out).
func (b *Breaker) Done(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	switch b.state {
	case HalfOpen:
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
		// A call that started before the breaker opened; it changes nothing.
	}
}

// Cancel ends an allowed call that never reached the provider (nothing to
// claim, shutdown). A cancelled probe lets the next caller probe.
func (b *Breaker) Cancel() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == HalfOpen {
		// Back to open with the cool-down already spent: the next Allow probes.
		b.state = Open
		b.openedAt = b.now().Add(-b.cfg.Cooldown)
	}
}

func (b *Breaker) open(now time.Time) {
	b.state = Open
	b.openedAt = now
	b.results, b.fails = b.results[:0], 0
}

func (b *Breaker) close() {
	b.state = Closed
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
