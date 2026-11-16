package health

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// MemoryState is the State of one process: the same rules as ValkeyState's
// script, for tests and for a worker given no shared state. The contract
// test runs both against the same cases.
type MemoryState struct {
	cfg Config
	now func() time.Time

	mu sync.Mutex
	ch map[notification.Channel]*memChannel
}

type memChannel struct {
	gen                    int64
	fails                  int
	open                   bool
	probeAfter, probeUntil int64 // us on now's clock; probeUntil 0: no probe out
}

var _ State = (*MemoryState)(nil)

// NewMemory returns a state reading time from now (time.Now in production).
func NewMemory(cfg Config, now func() time.Time) *MemoryState {
	return &MemoryState{cfg: cfg, now: now, ch: map[notification.Channel]*memChannel{}}
}

// NeverOpens is a state that admits every call: Failures is never reached.
func NeverOpens() *MemoryState {
	return NewMemory(Config{Failures: math.MaxInt, Cooldown: time.Second}, time.Now)
}

func (s *MemoryState) channel(ch notification.Channel, now int64) *memChannel {
	c := s.ch[ch]
	if c == nil {
		c = &memChannel{gen: now}
		s.ch[ch] = c
	}
	return c
}

func (c *memChannel) bump(now int64) { c.gen = max(now, c.gen+1) }

func (c *memChannel) current(now int64) Reply {
	if c.open {
		return c.opened(now, None)
	}
	return Reply{Kind: Closed, Gen: c.gen}
}

func (c *memChannel) opened(now int64, tr Transition) Reply {
	return Reply{Kind: Open, Gen: c.gen, Wait: time.Duration(max(c.probeAfter-now, 0)) * time.Microsecond, Transition: tr}
}

// Admit implements State.
func (s *MemoryState) Admit(_ context.Context, ch notification.Channel, probeLease time.Duration, seen int64) (Reply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UnixMicro()
	c := s.channel(ch, now)
	if c.gen < seen {
		c.gen = max(now, seen+1)
	}
	switch {
	case !c.open:
		return Reply{Kind: Closed, Gen: c.gen}, nil
	case c.probeUntil > now:
		return Reply{Kind: Wait, Gen: c.gen, Wait: time.Duration(c.probeUntil-now) * time.Microsecond}, nil
	case now < c.probeAfter:
		return Reply{Kind: Wait, Gen: c.gen, Wait: time.Duration(c.probeAfter-now) * time.Microsecond}, nil
	}
	c.bump(now)
	c.probeUntil = now + max(probeLease, 0).Microseconds()
	return Reply{Kind: Probe, Gen: c.gen}, nil
}

// Report implements State.
func (s *MemoryState) Report(_ context.Context, ch notification.Channel, gen int64, probe, fault bool, retryAfter time.Duration) (Reply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UnixMicro()
	c := s.channel(ch, now)
	pause := max(s.cfg.Cooldown, retryAfter, 0).Microseconds()
	if probe {
		if gen != c.gen || !c.open || c.probeUntil == 0 {
			return c.current(now), nil
		}
		c.bump(now)
		c.probeUntil = 0
		if fault {
			c.probeAfter = now + pause
			return c.opened(now, None), nil
		}
		c.open, c.fails, c.probeAfter = false, 0, 0
		return Reply{Kind: Closed, Gen: c.gen, Transition: Recovered}, nil
	}
	if c.open {
		return c.opened(now, None), nil
	}
	if !fault {
		c.fails = 0
		return Reply{Kind: Closed, Gen: c.gen}, nil
	}
	c.fails++
	if c.fails >= s.cfg.Failures {
		c.bump(now)
		c.open, c.fails, c.probeAfter, c.probeUntil = true, 0, now+pause, 0
		return c.opened(now, Opened), nil
	}
	return Reply{Kind: Closed, Gen: c.gen}, nil
}

// Cancel implements State.
func (s *MemoryState) Cancel(_ context.Context, ch notification.Channel, gen int64) (Reply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UnixMicro()
	c := s.channel(ch, now)
	if c.open && gen == c.gen && c.probeUntil > 0 {
		c.bump(now)
		c.probeUntil, c.probeAfter = 0, now
	}
	return c.current(now), nil
}
