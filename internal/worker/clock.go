package worker

import (
	"sync"
	"time"
)

// clockWindow is how long a Take's clock sample is used: the local clock and
// Valkey's drift apart, so an old sample's bounds stop being true.
const clockWindow = 10 * time.Second

// clockMap maps the limiter's clock (Valkey's) to the local monotonic clock.
// It is shared by every channel of one worker process and fed by every Take:
// the local moment just before the call (sent), just after the reply (got)
// and the limiter's clock at the reservation (Grant.At). The true offset
// (local minus Valkey) lies between sent-At and got-At; over the window the
// tightest bounds are kept: lower = max(sent-At), upper = min(got-At).
//
// Local moments are measured on the monotonic clock from epoch, so a step of
// the local wall clock changes nothing; a step of Valkey's clock shows as
// crossed bounds, and the mapping restarts from the newest sample.
type clockMap struct {
	epoch  time.Time // carries a monotonic reading
	window time.Duration

	mu      sync.Mutex
	samples []clockSample // in order of got
}

type clockSample struct {
	got    time.Duration // local, since epoch
	lo, hi time.Duration // bounds of the offset this sample gives
}

func newClockMap(epoch time.Time, window time.Duration) *clockMap {
	return &clockMap{epoch: epoch, window: window}
}

// local is t on the map's local scale.
func (c *clockMap) local(t time.Time) time.Duration { return t.Sub(c.epoch) }

// remote is a limiter moment on the same scale as offsets are measured.
func remote(at time.Time) time.Duration { return time.Duration(at.UnixNano()) }

// observe adds one Take's sample and returns the offset bounds over the
// window (upper >= lower).
func (c *clockMap) observe(sent, got, at time.Time) (lower, upper time.Duration) {
	r := remote(at)
	s := clockSample{got: c.local(got), lo: c.local(sent) - r, hi: c.local(got) - r}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Drop what fell out of the window, measured from the newest sample.
	keep := 0
	for keep < len(c.samples) && c.samples[keep].got < s.got-c.window {
		keep++
	}
	c.samples = append(c.samples[keep:], s)
	lower, upper = c.bounds()
	if lower > upper {
		// A clock stepped: the old samples contradict the new one.
		c.samples = append(c.samples[:0], s)
		lower, upper = s.lo, s.hi
	}
	return lower, upper
}

func (c *clockMap) bounds() (lower, upper time.Duration) {
	lower, upper = c.samples[0].lo, c.samples[0].hi
	for _, s := range c.samples[1:] {
		lower = max(lower, s.lo)
		upper = min(upper, s.hi)
	}
	return lower, upper
}

// toLocal is the local moment of limiter moment at under offset off.
func (c *clockMap) toLocal(at time.Time, off time.Duration) time.Time {
	return c.epoch.Add(remote(at) + off)
}
