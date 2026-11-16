// Package leasebudget holds how a claim's lease is spent: the wait for its
// send slot (the send alignment and the horizon), the provider call, and the
// margin left for the result write. The worker (internal/delivery) spends it;
// config refuses a lease too short to hold it. It imports nothing of either,
// so both can import it.
package leasebudget

import "time"

// Margin is how long before its lease ends a provider call is cut, so the
// attempt is recorded while the claim still holds. It also covers the skew
// between the worker's clock and the database's (lease_until is set on the
// database's clock and read on the worker's). Source: the fenced result
// write alone measured p99 75, 138 and 452 ms in three runs on a loaded
// machine (decision 0018, "As built"), plus NTP's tens of milliseconds of
// skew, rounded up.
const Margin = 500 * time.Millisecond

// AlignFloor is the least lead: the fixed alignment this replaces, so the
// lead only ever grows past what was there. The claims it has to cover are
// not the typical ones but the jumps: on a busy channel the claim time rose
// from 20-70 ms to 200-445 ms within one second as a drain ended (nine
// claimers on the last rows, each release waking all of them; acceptance R8,
// debug trace, 16 Nov), faster than any window of past claims can follow, and
// a claim that overruns its group loses those slots for every worker. With a
// floor of 20 ms (the median healthy claim, 16 ms, rounded up) R8 drained at
// 76-82 per second against 89-92 with the fixed 200 ms. What the floor costs
// is latency on a busy grid only: a lone send on an idle grid leaves right
// after its claim whatever the lead (group.flex in internal/delivery).
const AlignFloor = 200 * time.Millisecond

// AlignCeiling is the largest lead for a lease of lease: an eighth of it. The
// horizon follows the lead, so a claimed row waits at most lead + horizon = a
// quarter of its lease for its slot (7.5 s of the default 30 s; below a 1.6 s
// lease the floor sets the wait at 400 ms, see SlotWait), and three
// quarters are left for the provider call (APP_PROVIDER_TIMEOUT, default 5 s)
// and the result write, which a slow database also slows; the call is still
// cut Margin before the lease ends. Half the lease (a quarter as the ceiling)
// lost leases in the disturbed-worker test's 2 s lease on a loaded machine
// (verifier, 16 Nov). The default ceiling, 3.75 s, is above the claims of
// owner item 0011 (1.6 s; the p95 of claims spread over 0 to 3.2 s is about
// 3 s). Claims slower than the ceiling lose slots again: the database is then
// several seconds per query behind.
func AlignCeiling(lease time.Duration) time.Duration {
	if lease <= 0 {
		lease = 30 * time.Second // APP_LEASE_DURATION's default
	}
	return lease / 8
}

// SlotWait is the longest a claimed row waits for its slot under a lease of
// lease and an APP_RATE_LIMIT_HORIZON of horizon: the lead at its ceiling
// plus the horizon one Take reserves past it, which is the larger of horizon
// and the lead.
func SlotWait(lease, horizon time.Duration) time.Duration {
	ceiling := max(AlignCeiling(lease), AlignFloor)
	return ceiling + max(horizon, ceiling)
}
