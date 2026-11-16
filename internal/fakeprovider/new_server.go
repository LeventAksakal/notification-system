// Package fakeprovider is a local stand-in for the external notification
// provider. It accepts the provider's request shape, records every request,
// can deduplicate by Idempotency-Key, injects scripted faults, and counts
// exactly what was delivered so tests can assert on it.
package fakeprovider

import (
	mrand "math/rand/v2"
	"sync"
	"time"
)

// DefaultHistoryLimit caps the stored request history; counters stay exact.
const DefaultHistoryLimit = 100_000

// Option configures a Server.
type Option func(*Server)

// WithClock sets the time source; it must not go backwards.
func WithClock(now func() time.Time) Option { return func(s *Server) { s.now = now } }

// WithDedupe sets the initial (and post-Reset) dedupe switch. Default on.
func WithDedupe(on bool) Option { return func(s *Server) { s.dedupeDefault = on } }

// WithHistoryLimit caps the stored request history (oldest dropped).
func WithHistoryLimit(n int) Option { return func(s *Server) { s.historyLimit = max(n, 1) } }

// WithRand sets the [0,1) source used for ratio rules.
func WithRand(f func() float64) Option { return func(s *Server) { s.rand = f } }

// Server is the provider double. Safe for concurrent use.
type Server struct {
	now           func() time.Time
	rand          func() float64
	dedupeDefault bool
	historyLimit  int

	// One mutex guards everything below: Go maps are not safe for concurrent
	// writes, and handlers run on one goroutine per request.
	mu       sync.Mutex
	epoch    uint64 // bumped by Reset; requests begun in an older epoch are not counted
	dedupe   bool
	faults   []Fault
	history  ring
	channels map[string]*channelState
	keys     map[string]*keyState
}

type channelState struct {
	received, deliveries      int
	responses                 map[int]int
	requestWin, deliveriesWin window
}

type keyState struct {
	body                 []byte // first accepted response; nil until one is accepted
	requests, deliveries int
}

// New returns a Server with dedupe on, no faults and an empty history.
func New(opts ...Option) *Server {
	s := &Server{
		now:           time.Now,
		rand:          mrand.Float64, //nolint:gosec // fault sampling, not security
		dedupeDefault: true,
		historyLimit:  DefaultHistoryLimit,
	}
	for _, o := range opts {
		o(s)
	}
	s.Reset()
	return s
}

// Reset clears history, counters, remembered keys and faults, and restores
// the dedupe switch to its configured default.
func (s *Server) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	s.dedupe = s.dedupeDefault
	s.faults = nil
	s.history = ring{limit: s.historyLimit}
	s.channels = map[string]*channelState{}
	s.keys = map[string]*keyState{}
}

// SetDedupe turns Idempotency-Key deduplication on or off.
func (s *Server) SetDedupe(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dedupe = on
}
