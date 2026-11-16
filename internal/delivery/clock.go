package delivery

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
	// span is how long a forward step's overlap stays where a later step's
	// can fall into the same second: one second plus the largest lead and
	// horizon, which bound how far ahead slots of the old mapping wait.
	span time.Duration

	mu      sync.Mutex
	samples []clockSample // in order of got
	// gen counts the restarts that void. A forward step of s makes the new
	// slots arrive s early against those placed with the old mapping: the
	// two streams overlap for s (the limiter's schedule continues on Valkey's
	// timeline; if it restarted at its new "now", s covers all it had
	// pending). Slots placed under an older generation are void, so a restart
	// moves gen on only when the overlap could cost the cap: the forward
	// steps not voided over the last span, this one included, add up to at
	// least the smallest interval. Below that they add at most one request
	// to any second, which the paced rate leaves room for; a backward step
	// leaves a gap and never voids.
	gen uint64
	// steps are the forward step estimates of restarts that did not void,
	// in order of got; minInterval is the smallest interval observed.
	steps       []clockStep
	minInterval time.Duration
}

type clockStep struct {
	got, size time.Duration
}

// clockRestart tells how a sample restarted the mapping. step is the largest
// forward step of Valkey's clock the bounds allow (old upper bound minus the
// new sample's lower, as of the new sample); negative for a backward step.
type clockRestart struct {
	happened bool
	voids    bool // the generation moved on: slots placed before are void
	step     time.Duration
}

type clockSample struct {
	got    time.Duration // local, since epoch
	lo, hi time.Duration // bounds of the offset when the sample was taken
}

// unknown is the uncertainty of a mapping with no sample.
const unknown = time.Duration(math.MaxInt64)

func newClockMap(epoch time.Time, window, span time.Duration) *clockMap {
	return &clockMap{epoch: epoch, window: window, span: span}
}

// local is t on the map's local scale.
func (c *clockMap) local(t time.Time) time.Duration { return t.Sub(c.epoch) }

// remote is a limiter moment on the same scale as offsets are measured.
func remote(at time.Time) time.Duration { return time.Duration(at.UnixNano()) }

// drift is how far the offset may have moved over age.
func drift(age time.Duration) time.Duration { return age / 1_000_000 * clockDriftPPM }

// observe adds one Take's sample, the grant's interval (0 if it carries
// none) among them, and returns the offset bounds as of got (upper >= lower),
// the generation they belong to, and whether the sample contradicted the
// window, so the mapping restarted from it alone.
//
// A restart voids (moves the generation on) when the forward step it shows,
// added to those of the restarts not voided over the last span, reaches the
// smallest interval observed (or none is known yet). Several small steps
// inside one second overlap the streams by their sum, so they count
// together. A smaller forward step or a backward one only restarts the
// mapping: the slots placed before stay valid.
func (c *clockMap) observe(sent, got, at time.Time, interval time.Duration) (lower, upper time.Duration, gen uint64, r clockRestart) {
	rt := remote(at)
	s := clockSample{got: c.local(got), lo: c.local(sent) - rt, hi: c.local(got) - rt}
	c.mu.Lock()
	defer c.mu.Unlock()
	if interval > 0 && (c.minInterval == 0 || interval < c.minInterval) {
		c.minInterval = interval
	}
	c.expire(s.got)
	if len(c.samples) == 0 {
		c.samples = append(c.samples, s)
		return s.lo, s.hi, c.gen, r
	}
	_, oldUpper := c.bounds(s.got)
	c.samples = append(c.samples, s)
	lower, upper = c.bounds(s.got)
	if lower <= upper {
		return lower, upper, c.gen, r
	}
	// A clock stepped (or drifted beyond the allowance): the old samples
	// contradict the new one. The true offset was at most oldUpper and is
	// now at least s.lo, so Valkey's clock moved forward by at most this.
	c.samples = append(c.samples[:0], s)
	r = clockRestart{happened: true, step: oldUpper - s.lo}
	keep := 0
	for keep < len(c.steps) && c.steps[keep].got < s.got-c.span {
		keep++
	}
	c.steps = c.steps[keep:]
	if r.step <= 0 {
		return s.lo, s.hi, c.gen, r // backward: a gap, never an overlap
	}
	total := r.step
	for _, st := range c.steps {
		total += st.size
	}
	if c.minInterval == 0 || total >= c.minInterval {
		// The overlap could put more than one request extra in a second:
		// what was placed before is void, and so is the overlap it carried.
		r.voids = true
		c.gen++
		c.steps = c.steps[:0]
		return s.lo, s.hi, c.gen, r
	}
	c.steps = append(c.steps, clockStep{got: s.got, size: r.step})
	return s.lo, s.hi, c.gen, r
}

// current reports whether gen is still the mapping's generation: no restart
// that voids has been seen since slots were placed under it.
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
