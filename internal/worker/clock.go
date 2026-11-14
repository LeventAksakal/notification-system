package worker

import (
	"math"
	"sync"
	"time"
)

// clockWindow is how long a Take's clock sample is used.
const clockWindow = 10 * time.Second

// clockDriftPPM is the drift allowed between the local clock and Valkey's,
// in parts per million: a sample's bounds widen by its age times this before
// they are intersected. Quartz clocks drift by tens of ppm; NTP slews at most
// 500 ppm, but only while correcting; 200 is generous for ordinary running.
const clockDriftPPM = 200

// clockMap maps the limiter's clock (Valkey's) to the local monotonic clock.
// It is shared by every channel of one worker process and fed by every Take:
// the local moment just before the call (sent), just after the reply (got)
// and the limiter's clock at the reservation (Grant.At). When the sample was
// taken the true offset (local minus Valkey) lay between sent-At and got-At.
// The clocks drift apart, so a sample of age a bounds the offset now only
// within its bounds widened by a*clockDriftPPM on each side. Over the window
// the tightest of those widened bounds are kept.
//
// Local moments are measured on the monotonic clock from epoch, so a step of
// the local wall clock changes nothing; a step of Valkey's clock beyond the
// allowance shows as crossed bounds, and the mapping restarts from the newest
// sample.
type clockMap struct {
	epoch  time.Time // carries a monotonic reading
	window time.Duration

	mu      sync.Mutex
	samples []clockSample // in order of got
	// gen counts restarts. Slots placed under one generation are void once
	// it has moved on: the step that restarted the mapping may also have
	// restarted the limiter's schedule at its new "now", and slots handed
	// out on the old timeline would overlap the new ones.
	gen uint64
}

type clockSample struct {
	got    time.Duration // local, since epoch
	lo, hi time.Duration // bounds of the offset when the sample was taken
}

// unknown is the uncertainty of a mapping with no sample.
const unknown = time.Duration(math.MaxInt64)

func newClockMap(epoch time.Time, window time.Duration) *clockMap {
	return &clockMap{epoch: epoch, window: window}
}

// local is t on the map's local scale.
func (c *clockMap) local(t time.Time) time.Duration { return t.Sub(c.epoch) }

// remote is a limiter moment on the same scale as offsets are measured.
func remote(at time.Time) time.Duration { return time.Duration(at.UnixNano()) }

// drift is how far the offset may have moved over age.
func drift(age time.Duration) time.Duration { return age / 1_000_000 * clockDriftPPM }

// observe adds one Take's sample and returns the offset bounds as of got
// (upper >= lower), the generation they belong to, and whether the sample
// contradicted the window, so the mapping restarted from it alone and the
// generation moved on.
func (c *clockMap) observe(sent, got, at time.Time) (lower, upper time.Duration, gen uint64, restarted bool) {
	r := remote(at)
	s := clockSample{got: c.local(got), lo: c.local(sent) - r, hi: c.local(got) - r}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expire(s.got)
	c.samples = append(c.samples, s)
	lower, upper = c.bounds(s.got)
	if lower > upper {
		// A clock stepped (or drifted beyond the allowance): the old samples
		// contradict the new one.
		c.samples = append(c.samples[:0], s)
		c.gen++
		return s.lo, s.hi, c.gen, true
	}
	return lower, upper, c.gen, false
}

// current reports whether gen is still the mapping's generation: no clock
// step has been seen since slots were placed under it.
func (c *clockMap) current(gen uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen == gen
}

// uncertainty is upper - lower as of now from the samples in the window, or
// unknown when there is none (a cold start, a long idle).
func (c *clockMap) uncertainty(now time.Time) time.Duration {
	at := c.local(now)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expire(at)
	if len(c.samples) == 0 {
		return unknown
	}
	lower, upper := c.bounds(at)
	if lower > upper {
		return unknown // crossed: only a new sample can say
	}
	return upper - lower
}

// expire drops the samples taken more than the window before at.
func (c *clockMap) expire(at time.Duration) {
	keep := 0
	for keep < len(c.samples) && c.samples[keep].got < at-c.window {
		keep++
	}
	c.samples = c.samples[keep:]
}

// bounds intersects the samples' bounds, each widened by its drift until at.
func (c *clockMap) bounds(at time.Duration) (lower, upper time.Duration) {
	lower, upper = math.MinInt64, math.MaxInt64
	for _, s := range c.samples {
		d := drift(max(at-s.got, 0))
		lower = max(lower, s.lo-d)
		upper = min(upper, s.hi+d)
	}
	return lower, upper
}

// toLocal is the local moment of limiter moment at under offset off.
func (c *clockMap) toLocal(at time.Time, off time.Duration) time.Time {
	return c.epoch.Add(remote(at) + off)
}
