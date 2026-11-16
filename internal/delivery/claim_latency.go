package delivery

import (
	"sync"
	"time"
)

// claimLatency is what one worker has measured of its claims on one channel:
// for each claim, the time from the moment its slots were asked for (just
// before the Take) to the moment its rows were in hand. The send alignment is
// derived from it (lead): a group's first slot is reserved that far ahead, so
// its rows are normally claimed before it is due.
//
// It keeps the newest claimSamples (20) claims and takes the second largest,
// their p95; until it holds 20 it takes the largest.
//   - p95, not higher: a claim slower than the lead does not lose its group,
//     only the slots it overran, (claim - lead) / interval of them. On a
//     healthy channel the claims above the p95 overrun it by milliseconds, a
//     slot or so in one claim of twenty; a higher percentile would hold every
//     send back longer to save that slot.
//   - Second largest, not the largest: one outlier (a lock wait, a pause of
//     the process) costs only its own group's slots and leaves the lead
//     alone. A second slow claim among the newest twenty lifts it at once,
//     which is how it follows a database that turns slow: two claims after
//     the first slow one, whatever the claim rate.
//   - Twenty, counted in claims rather than seconds: a time window holds
//     about 100 claims on a healthy channel (one claim per refill, 100 ms)
//     but only a few while claims take seconds, where its second largest
//     would be a low percentile; and a worker that makes no claim (a channel
//     with nothing due, a grid reserved far ahead by the others) would come
//     back with nothing measured. Twenty healthy claims are 2 s (Takes stay
//     refill(APP_RATE_LIMIT_HORIZON) = 100 ms apart, see horizon), so a slow
//     spell is forgotten about 2 s after claims are fast again.
//   - A claim's time runs from just before its Take to its rows in hand; a
//     deferred claim (group.deferClaim) starts it after its wait, which is
//     not claim time.
//   - Until twenty are measured (a start) the largest is taken, which errs
//     toward waiting longer while the start-up claims (about 170 ms, record
//     0009) are in the window. With fewer than two, startLead is a floor: a
//     first claim measured slow is believed at once, a first claim measured
//     fast is not, until a second confirms it.
//
// A falling lead loses nothing; a rising one leaves no gap on a busy grid
// (notification.SlotRequest.Lead), so the lead needs no smoothing either way.
// It does not oscillate: a claim's own time hardly depends on the lead (only
// through the batch size, horizon/interval).
type claimLatency struct {
	mu      sync.Mutex
	samples [claimSamples]time.Duration
	n, next int // samples held; where the next one goes
}

const (
	// claimSamples is how many of the newest claims count; see claimLatency.
	claimSamples = 20
	// startLead is the lead before two claims were measured: above the claim
	// time seen while a stack starts (about 170 ms, record 0009). It is what
	// the fixed alignment was.
	startLead = 200 * time.Millisecond
)

// observe records a claim that took d.
func (c *claimLatency) observe(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples[c.next] = d
	c.next = (c.next + 1) % claimSamples
	c.n = min(c.n+1, claimSamples)
}

// lead is the alignment the next reservation uses, within [lo, hi].
func (c *claimLatency) lead(lo, hi time.Duration) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first, second time.Duration
	for _, d := range c.samples[:c.n] {
		switch {
		case d >= first:
			first, second = d, first
		case d > second:
			second = d
		}
	}
	var d time.Duration
	switch {
	case c.n == 0:
		d = startLead
	case c.n == 1:
		d = max(startLead, first)
	case c.n < claimSamples:
		d = first
	default:
		d = second
	}
	return min(max(d, lo), hi)
}
