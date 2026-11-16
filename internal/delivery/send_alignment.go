package delivery

import (
	"time"
)

// The send alignment (the lead) is the time this worker's claim for a group
// of slots is expected to take. Slots are reserved before the claim, so a
// first slot due at once would be missed by every claim; a slot whose row is
// not in hand within one interval of it is given up, never sent late.
//
// Why a changing lead cannot bring two sends closer than the grid does: the
// lead is not added to the slots on the worker (as the fixed 200 ms
// alignment was, the same for every worker); it goes to the limiter
// (notification.SlotRequest.Lead), and every slot handed out is the send
// moment itself, on the one grid all workers share. The grid's next free
// slot (TAT) only moves forward, so slots are one interval apart whatever
// lead each Take asked with, and each request still leaves in [slot, slot +
// one interval) as before. A per-worker offset added locally would not have
// that property: two workers with leads 1.5 s apart could each send a full
// second's slots into the same second.
//
// What a change of lead does to the grid: a free grid restarts at now+lead
// (nobody else is sending before it); a busy grid continues at its next free
// slot even when that is sooner than the lead, so a rise leaves no gap the
// other workers could have filled (a slot this worker's claim then misses is
// given up), and a fall reserves nothing until the grid already reserved
// comes within lead+horizon, losing nothing.

// alignFloor is the least lead: the fixed alignment this replaces, so the
// lead only ever grows past what was there. The claims it has to cover are
// not the typical ones but the jumps: on a busy channel the claim time rose
// from 20-70 ms to 200-445 ms within one second as a drain ended (nine
// claimers on the last rows, each release waking all of them; acceptance R8,
// debug trace, 16 Nov), faster than any window of past claims can follow, and
// a claim that overruns its group loses those slots for every worker. With a
// floor of 20 ms (the median healthy claim, 16 ms, rounded up) R8 drained at
// 76-82 per second against 89-92 with the fixed 200 ms. What the floor costs
// is latency on a busy grid only: a lone send on an idle grid leaves right
// after its claim whatever the lead (group.flex).
const alignFloor = 200 * time.Millisecond

// alignCeiling is the largest lead for a lease of lease: an eighth of it. The
// horizon follows the lead (horizon), so a claimed row waits at most lead +
// horizon = a quarter of its lease for its slot (7.5 s of the default 30 s),
// and three quarters are left for the provider call (APP_PROVIDER_TIMEOUT,
// default 5 s) and the result write, which a slow database also slows;
// deliver still cuts any call 250 ms before the lease ends. Half the lease
// (a quarter as the ceiling) lost leases in the disturbed-worker test's 2 s
// lease on a loaded machine (verifier, 16 Nov). The default ceiling, 3.75 s,
// is above the claims of owner item 0011 (1.6 s; the p95 of claims spread
// over 0 to 3.2 s is about 3 s). Claims slower than the ceiling lose slots
// again: the database is then several seconds per query behind.
func alignCeiling(lease time.Duration) time.Duration {
	if lease <= 0 {
		lease = 30 * time.Second // APP_LEASE_DURATION's default
	}
	return lease / 8
}

// lead is the send alignment of the channel's next reservation: the claim
// time measured by this worker on this channel (claimLatency), within
// [AlignFloor, AlignCeiling].
func (w *Worker) lead(cr *channelRun) time.Duration {
	return cr.latency.lead(w.cfg.AlignFloor, w.cfg.AlignCeiling)
}

// horizon is how far past the lead one Take reserves: the larger of Horizon
// and the lead.
//
// Horizon (APP_RATE_LIMIT_HORIZON, 200 ms) comes from a budget of limiter
// calls: a busy channel asks again every refill(Horizon) = 100 ms, so 200 ms
// is 10 Takes per second per channel per worker (90 per second for three
// workers and three channels, each an O(1) script on Valkey), and one Take
// reserves about 100 ms of slots, 9 or 10 at 95 per second.
//
// Not less than the lead: a worker has one claim in flight per channel and
// asks again only once it is done, so it reserves at most once per claim
// time. With a horizon shorter than the claim time a lone worker would
// reserve horizon of slots per claim time and send at horizon/claim of the
// paced rate: 200 ms per 1.6 s claim is an eighth. With horizon >= lead >=
// claim time, the grant of one Take reaches past the moment the next Take is
// made, so the reserved time never runs out while one worker is alive. The
// spacing of Takes stays refill(Horizon), not half the grown horizon: a slow
// claim spaces them by itself (the next Take waits for it), and once claims
// are fast again they come every 100 ms, so the twenty claims that forget a
// slow spell take about 2 s (claimLatency), not 19 half-leads.
func (w *Worker) horizon(lead time.Duration) time.Duration {
	return max(w.cfg.Horizon, lead)
}

// refill is how long after a reservation the next is asked for, horizon
// being APP_RATE_LIMIT_HORIZON: by then about horizon/2 of slots can be
// reserved again (see horizon).
func refill(horizon time.Duration) time.Duration { return max(horizon/2, time.Millisecond) }
