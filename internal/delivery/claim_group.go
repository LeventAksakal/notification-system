package delivery

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
)

// group is one grant's slots as placed on the local clock: their moments,
// health tickets, how late each may leave, the clock mapping's generation
// they were placed under, and when they were asked for (where the claim time
// that claimLatency measures starts).
type group struct {
	slots     []time.Time
	tickets   []health.Ticket
	tolerance time.Duration
	gen       uint64
	asked     time.Time
	// grant is the limiter's grant the slots come from: slot k is its slot
	// k. The slots no row went to are given back through it (giveBack).
	grant notification.Grant
	// lead and horizon are what the grant was asked with.
	lead, horizon time.Duration
	// notBefore is one interval after the grid's TAT before the grant (the
	// limiter's now of the grant less Grant.Idle), and never before that
	// now, on the local clock, placed like the slots (never early on
	// Valkey's clock): no flex send leaves before it.
	notBefore time.Time
	// flex: the limiter's grid was free when this grant was made
	// (Grant.Idle > 0), so the group may leave as soon as it is claimed
	// instead of at its slots, never before notBefore. On a free
	// grid the first slot is the lead after the limiter's now, and the lead
	// is the p95 of this worker's claims, so most claims return before it:
	// flex sends them on at once. It does so only when its claim found fewer
	// rows than slots: the backlog ended within the group, and the slots
	// after its rows go unused either way. A full group keeps its moments,
	// because the next grant continues the busy grid right after them, and
	// moving the full group earlier would leave a hole of up to the lead
	// between the two on a channel with a backlog (measured with the fixed
	// 200 ms alignment: R9 low alone fell to 91.6/s against its floor of
	// 93). Why the cap holds, all times on Valkey's clock (V), I the
	// interval, T the TAT before this grant, N the limiter's now of it:
	//  - Allowed lateness and clock offset. A worker places a slot with the
	//    upper bound of its offset to V (clockMap), so on V it is never early
	//    and at most that worker's uncertainty u late; the send then leaves
	//    within its tolerance, I - u. So a request of slot s leaves in
	//    [s, s+I) on V, whichever worker sends it and whatever its offset, as
	//    long as the offset lies in the bounds clockMap keeps (a step outside
	//    them voids the generation, as before). notBefore is placed the same
	//    way, so a flex send is never before max(N, T+I) on V.
	//  - Earlier grants: their last slot is at most T-I, so every request of
	//    theirs has left before T on V, the latest one included.
	//  - Idle is N-T, both read by the limiter on V: no worker clock in it.
	//    notBefore is N + max(I - Idle, 0) = max(N, T+I), so the first flex
	//    send is at T+I or later: more than I after the last earlier send.
	//    (It required Idle >= I until 16 Nov, which made a row due a few
	//    milliseconds after a lone send wait for a restart a lead out: the
	//    cap never needed N itself past T+I, only the send.) The unbroken grid only
	//    guarantees a gap above 0 between neighbours (a late send, then its
	//    successor on time); the seam is sparser than the grid, so no second
	//    holds more than on a busy channel.
	//  - Within the group the slots move together, I apart, and each still
	//    leaves in [slot, slot+I) on V. Later grants' slots start I after
	//    this group's last unmoved slot; this group only left earlier, so
	//    the gap grows, and a later flex grant repeats this argument with
	//    this group as the earlier one (its sends end before its unmoved
	//    last slot + I, the TAT that grant sees).
	flex bool
	// deferClaim: the group is not flex but its worker has just woken from
	// idle, so another worker woken by the same hint may hold the flex group
	// just ahead of it, ending a few microseconds past its horizon, so the
	// next taker gets the slots right after them. Both would claim at once
	// and SKIP LOCKED gives the row to whichever locks first: measured on the
	// harness with the fixed 200 ms alignment, the hinted row then left with
	// this group about 400 ms after its creation in about half the samples.
	// Its claim waits until its first slot less its lead (the time its
	// claims take), so the flex worker claims first. Its own sends keep
	// their moments; a busy channel never defers (woke is false). A woken
	// grant further out than the lead is not deferred but given back at
	// once (channelLoop), so it does not sit on top of the near groups;
	// deferral is left for grants within the lead.
	deferClaim bool
}

// claimGroup claims rows for a group of slots and starts one send per row,
// each at its slot. A slot whose moment passed (by more than tolerance)
// before a row was there for it is lost; so is a slot no row was found for,
// and every slot of a group whose clock generation has moved on.
func (w *Worker) claimGroup(ctx, sendCtx context.Context, cr *channelRun, gr group, inflight *sync.WaitGroup) {
	slots, tickets, tolerance := gr.slots, gr.tickets, gr.tolerance
	i := 0
	// stepped gives up the slots left when a clock step voided the group.
	stepped := func() bool {
		if w.clock.current(gr.gen) {
			return false
		}
		w.m.gaveUp(ctx, cr.ch, giveUpClockStepped, len(slots)-i)
		for ; i < len(slots); i++ {
			w.cancel(ctx, cr, tickets[i])
		}
		return true
	}
	if gr.deferClaim {
		sleep(ctx, time.Until(slots[0].Add(-gr.lead)))
		gr.asked = time.Now() // the wait is not claim time (claimLatency)
	}
	if stepped() {
		return
	}
	skipMissed := func(now time.Time) {
		from := i
		for i < len(slots) && now.After(slots[i].Add(tolerance)) {
			w.cancel(ctx, cr, tickets[i])
			i++
		}
		w.m.gaveUp(ctx, cr.ch, giveUpSlotMissed, i-from)
	}
	skipMissed(time.Now())
	jobs, drained, polled, r, queried, err := w.claim(ctx, cr, len(slots)-i)
	if err != nil && ctx.Err() == nil {
		slog.ErrorContext(ctx, "claim failed", "channel", cr.ch, "err", err)
		cr.pause(time.Now().Add(w.cfg.IdleMax))
	}
	if queried && err == nil {
		cr.latency.observe(time.Since(gr.asked))
	}
	var moved time.Duration // how much earlier than its slots the group leaves
	if gr.flex && len(jobs) > 0 && len(jobs) < len(slots)-i {
		// The backlog ended within this group: leave right after the
		// claim, never before notBefore (group.flex).
		if d := slots[i].Sub(later(gr.notBefore, time.Now())); d > 0 {
			moved = d
			for k := i; k < len(slots); k++ {
				slots[k] = slots[k].Add(-d)
			}
		}
	}
	skipMissed(time.Now())
	slog.DebugContext(ctx, "group claimed", "channel", cr.ch, "since_asked_ms", time.Since(gr.asked).Milliseconds(),
		"slots", len(slots), "slots_missed", i, "rows", len(jobs), "flex", gr.flex)
	if len(drained) > 0 && err == nil {
		// After the sends are started, so the database read costs them
		// nothing; still inside the round (claiming is held).
		defer w.settleEmpty(ctx, cr, drained, r, refill(w.cfg.Horizon))
	}
	if len(polled) > 0 && err == nil {
		// The priorities the claim did not ask (claim): asked when their
		// next row is due, also after the sends are started, and claimed by
		// the next round if one is due now.
		defer w.settleEmpty(ctx, cr, polled, r, 0)
	}
	voided := stepped() // the claimed rows then all go back below
	// The group's claims are announced in one publish, beside the sends: a
	// slow Valkey costs no slot. Each job's result event waits for it, so a
	// job's processing event still goes out before its result.
	announced := w.announce(sendCtx, jobs, inflight)
	// Jobs without a slot go back off the claim path too: their release event
	// waits for the publish, which must not hold up the channel's next claim.
	var back []notification.Job
	reason := "claim outlasted its slots"
	if voided {
		reason = "limiter clock stepped" // the slots are counted above
	}
	defer func() {
		for _, job := range back {
			inflight.Add(1)
			go func() { // owned by Run through inflight
				defer inflight.Done()
				w.release(jobContext(sendCtx, job), job, announced, reason)
			}()
		}
	}()
	for _, job := range jobs {
		jctx := jobContext(sendCtx, job)
		if voided {
			back = append(back, job)
			continue
		}
		if i == len(slots) {
			w.m.gaveUp(ctx, cr.ch, giveUpClaimOutlasted, 1)
			back = append(back, job)
			continue
		}
		at, tk := slots[i], tickets[i]
		i++
		inflight.Add(1)
		go func() { // owned by Run through inflight
			defer inflight.Done()
			w.sendAt(ctx, jctx, cr, job, announced, at, tk, tolerance, gr.gen)
		}()
	}
	if !voided && i < gr.grant.N {
		// Slot i is the first no request went to: every request sent on
		// this grant leaves before it, moved as the group was.
		from := gr.grant.At.Add(gr.grant.First + time.Duration(i)*gr.grant.Interval - moved)
		if len(jobs) == 0 && gr.grant.Idle > 0 {
			// Nothing went out on a grant that restarted a free grid a lead
			// out: give it all back to the grid's next free slot before it
			// (the limiter's now less Idle), as if it had never been taken.
			// Given back from its first slot instead, the grid looked busy
			// for a lead after an empty claim, and a row due meanwhile
			// waited for that restart (158 ms with a 150 ms lead, three
			// workers, TestASecondRowAfterALoneSendIsNotHeldBehindTheOtherWorkersSlots).
			from = gr.grant.At.Add(-gr.grant.Idle)
		}
		w.giveBack(ctx, cr, gr.grant, from, gr.grant.N-i)
	}
	for ; i < len(slots); i++ {
		w.cancel(ctx, cr, tickets[i])
	}
}

// later returns the later of a and b.
func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
