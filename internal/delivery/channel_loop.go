package delivery

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
)

// channelLoop reserves slots and hands each group to a claim task. It
// reserves nothing while every priority is known to be empty, while the
// provider health refuses (an outage is declared and this worker does not
// hold the probe), while the channel's previous claim is running, or while
// the channel's provider calls are all in flight.
func (w *Worker) channelLoop(ctx, sendCtx context.Context, cr *channelRun, inflight *sync.WaitGroup) {
	limiterDown := false
	clockUncertain := false
	probeWait := w.cfg.IdleMin
	interval := time.Millisecond // until the first grant says
	// woke: the channel was woken from idle by a hint or a due time, as every
	// other worker may have been at the same moment (see group.deferClaim).
	// Not when it only waited for rows another claimer held, nor when its
	// safety poll found rows: both mean other workers are draining a backlog,
	// and a deferred claim there takes the rows after later slots, leaving
	// the earlier ones unused (measured: R8's whole drain fell from 90.3 to
	// 88.4 requests per second, the tail's gaps from ~180 to ~480 ms).
	woke := false
	unreachable := func(err error) {
		if !limiterDown {
			slog.ErrorContext(ctx, "rate limiter unreachable; channel sends nothing", "channel", cr.ch, "err", err)
			limiterDown = true
		}
	}
	reachable := func() {
		if limiterDown {
			slog.InfoContext(ctx, "rate limiter reachable again", "channel", cr.ch)
			limiterDown = false
		}
	}
	// The health gate fails closed: without the shared state nothing is
	// claimed (the limiter lives in the same Valkey and is down too).
	healthDown := false
	for ctx.Err() == nil {
		now := time.Now()
		cr.mu.Lock()
		at := cr.sel.nextCheck(now)
		claim, peek := cr.sel.due(now)
		contended := cr.sel.contended()
		revived := cr.sel.revived
		cr.sel.revived = false // seen now
		cr.mu.Unlock()
		if revived && !contended {
			// A hint (or due time) made a priority found empty claimable
			// again and no wait reported it: it came as the pause's timer
			// fired (pauseWait's select took the timer) or after the pause
			// ended. The channel is woken by it all the same, as every other
			// worker the hint reached is, so the grant it is about to take
			// on their busy grid must not claim at once (woke): unreported,
			// it claimed the hinted row for its slot a lead or more out while
			// the worker holding the near flex group still waited for its
			// reply (600 ms with a 300 ms lead,
			// TestWorkersWokenTogetherLeaveTheHintedRowToTheEarlyGroup on two
			// cores, about one run in a hundred). Not on a contended channel,
			// as after idleWait.
			woke = true
		}
		if !at.IsZero() {
			w.idleWait(ctx, cr, at.Sub(now))
			woke = !contended
			continue
		}
		if !claim {
			// Only safety polls came due: ask the database whether anything
			// is due before reserving slots and claiming.
			w.peek(ctx, cr, peek)
			woke = false
			continue
		}
		if cr.calling.full() {
			sleep(ctx, w.cfg.IdleMin)
			continue
		}
		if w.clock.uncertainty(time.Now()) >= interval {
			// Slots could not be placed within one interval (a cold start, a
			// long idle, a clock step, slow replies): read Valkey's clock with
			// a Take of no slots, which costs the other workers nothing, and
			// reserve only once the offset is known well enough.
			sent := time.Now()
			g, err := w.deps.Limiter.Take(ctx, cr.ch, notification.SlotRequest{})
			got := time.Now()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				unreachable(err)
				sleep(ctx, w.cfg.IdleMax)
				continue
			}
			reachable()
			if g.Interval > 0 {
				interval = g.Interval
			}
			uncertainty := unknown // a grant without At cannot be placed
			if !g.At.IsZero() {
				lower, upper, _, restart := w.clock.observe(sent, got, g.At, g.Interval)
				uncertainty = upper - lower
				w.logClockStep(ctx, cr.ch, restart, uncertainty)
			}
			if uncertainty >= interval {
				if !clockUncertain {
					slog.WarnContext(ctx, "limiter clock offset too uncertain to hold the rate; channel sends nothing",
						"channel", cr.ch, "uncertainty_us", uncertainty.Microseconds(), "interval_us", interval.Microseconds())
					clockUncertain = true
				}
				sleep(ctx, probeWait)
				probeWait = min(2*probeWait, w.cfg.IdleMax)
				continue
			}
			probeWait = w.cfg.IdleMin
			if clockUncertain {
				slog.InfoContext(ctx, "limiter clock offset known again", "channel", cr.ch,
					"uncertainty_us", uncertainty.Microseconds())
				clockUncertain = false
			}
		}
		first, ok, wait, err := cr.gate.Allow(ctx, w.probeLease(interval))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !healthDown {
				slog.ErrorContext(ctx, "provider health unreachable; channel sends nothing", "channel", cr.ch, "err", err)
				healthDown = true
			}
			sleep(ctx, w.cfg.IdleMax)
			continue
		}
		if healthDown {
			slog.InfoContext(ctx, "provider health reachable again", "channel", cr.ch)
			healthDown = false
		}
		if !ok {
			// An outage is declared: wait out the cool-down or the probe that
			// is out, asking again at least every IdleMax so a recovery that
			// another worker's probe saw (or a lost state) is noticed within it.
			sleep(ctx, min(wait, w.cfg.IdleMax))
			continue
		}
		select {
		case cr.claiming <- struct{}{}:
		case <-ctx.Done():
			w.cancel(ctx, cr, first)
			return
		}
		sent := time.Now()
		lead := w.lead(cr)
		horizon := w.horizon(lead)
		want := int(horizon/interval) + 1
		if first.Probe() {
			want = 1 // the probe is one call
		}
		g, err := w.deps.Limiter.Take(ctx, cr.ch, notification.SlotRequest{N: want, Lead: lead, Horizon: horizon})
		got := time.Now()
		slog.DebugContext(ctx, "send slots reserved", "channel", cr.ch, "lead_ms", lead.Milliseconds(), "horizon_ms", horizon.Milliseconds(),
			"asked", want, "reserved", g.N, "first_ms", g.First.Milliseconds(), "wait_ms", g.Wait.Milliseconds())
		if err != nil {
			<-cr.claiming
			w.cancel(ctx, cr, first)
			if ctx.Err() != nil {
				return
			}
			unreachable(err)
			sleep(ctx, w.cfg.IdleMax)
			continue
		}
		reachable()
		if g.Interval > 0 {
			interval = g.Interval
		}
		pause := g.Wait + refill(w.cfg.Horizon)
		if first.Probe() {
			pause = g.Wait
		}
		// Valkey's clock maps to the local one with an offset in [lower,
		// upper] (clockMap). Slots are placed with upper, so none is early
		// whatever the reply's delay; a request may so be up to uncertainty
		// late from the mapping alone, and the group's tolerance gives that
		// up: mapping error plus timer lateness stay within one interval.
		uncertainty := unknown // a grant without At cannot be placed
		var upper time.Duration
		var gen uint64
		if !g.At.IsZero() {
			var lower time.Duration
			var restart clockRestart
			lower, upper, gen, restart = w.clock.observe(sent, got, g.At, g.Interval)
			uncertainty = upper - lower
			w.logClockStep(ctx, cr.ch, restart, uncertainty)
			if restart.voids && g.N > 0 {
				// Valkey's clock stepped forward by up to an interval or more
				// (clockMap.gen): this grant's slots would overlap the pending
				// ones of the old mapping by that much (or the limiter's
				// schedule restarted at its new "now" beside all of them). The
				// new generation voids those, and this grant, which could
				// leave beside them, is not used either. A restart that does
				// not void showed a forward step under an interval, which adds
				// at most one request to a second, or a backward one, which
				// leaves a gap: the grant is used as placed.
				<-cr.claiming
				w.cancel(ctx, cr, first)
				w.m.gaveUp(ctx, cr.ch, giveUpClockStepped, g.N)
				continue
			}
		}
		if g.N > 0 && uncertainty >= g.Interval {
			// The cap cannot be guaranteed: none of these slots is used. Rare:
			// the mapping was checked before the Take, and a sample that
			// restarted it and voids is handled above (one that restarted it
			// without voiding may land here, its round trip being slow); the
			// next turn reads the clock without reserving.
			<-cr.claiming
			w.cancel(ctx, cr, first)
			w.m.gaveUp(ctx, cr.ch, giveUpClockUncertain, g.N)
			if !clockUncertain {
				slog.WarnContext(ctx, "limiter clock offset too uncertain to hold the rate; channel sends nothing",
					"channel", cr.ch, "uncertainty_us", uncertainty.Microseconds(), "interval_us", g.Interval.Microseconds())
				clockUncertain = true
			}
			continue
		}
		if g.N > 0 && clockUncertain {
			slog.InfoContext(ctx, "limiter clock offset known again", "channel", cr.ch,
				"uncertainty_us", uncertainty.Microseconds())
			clockUncertain = false
		}
		if g.N == 0 {
			<-cr.claiming
			w.cancel(ctx, cr, first)
			sleep(ctx, pause)
			continue
		}
		if !cr.gate.Current(first) {
			// An outage was declared meanwhile (one of this worker's results
			// said so): the slots are lost.
			<-cr.claiming
			w.cancel(ctx, cr, first)
			w.m.gaveUp(ctx, cr.ch, giveUpBreakerOpened, g.N)
			continue
		}
		flex := g.Idle > 0
		if woke && !flex && g.First > lead {
			// Woken from idle onto a grid already reserved further out than
			// this worker's claim (a restart would be no later): other
			// workers woken by the same hint (or due time) hold the near
			// slots, the flex group if the grid was idle. These slots are
			// given back at once rather than held for a deferred claim.
			// Held, they sat on top of the near groups until that claim, so
			// the near groups' give-backs waited behind them, and the claim
			// took a row posted meanwhile to send at these far slots: a row
			// due right after a lone send waited a lead or more (385 ms on
			// the three-worker acceptance stack,
			// TestGapDeliveryR7SecondRowAfterALoneSend; 158 ms with a 150 ms
			// lead in TestASecondRowAfterALoneSendIsNotHeldBehindTheOtherWorkersSlots,
			// where the far grant started one interval past the lead).
			// The cost, only when more rows than the near groups hold arrive
			// right after a lone send: the next Take, a pause later and no
			// longer woken, finds the same slots a pause sooner, so a claim
			// as slow as the lead misses at most a pause of them, once
			// (refill/interval: about 9 slots at 95/s with the 200 ms
			// horizon). The claim asks only for the slots still ahead, so
			// those rows wait for the next group.
			w.giveBack(ctx, cr, g, g.At.Add(g.First), g.N)
			<-cr.claiming
			w.cancel(ctx, cr, first)
			woke = w.pauseWait(ctx, cr, time.Until(got.Add(pause)))
			continue
		}
		// One admission covers the grant: every slot carries the same ticket
		// (a probe's grant has one slot).
		tickets := make([]health.Ticket, g.N)
		for i := range tickets {
			tickets[i] = first
		}
		base := w.clock.toLocal(g.At.Add(g.First), upper)
		slots := make([]time.Time, len(tickets))
		for i := range slots {
			slots[i] = base.Add(time.Duration(i) * g.Interval)
		}
		tolerance := g.Interval - uncertainty // > 0: checked above
		// The grid was free (Idle > 0): every request of an earlier slot
		// left before the old TAT, the grant's now less Idle; a flex send
		// waits until an interval after it (see group.flex).
		notBefore := w.clock.toLocal(g.At.Add(max(g.Interval-g.Idle, 0)), upper)
		deferClaim := woke && !flex
		inflight.Add(1)
		go func() { // owned by Run through inflight
			defer inflight.Done()
			defer func() { <-cr.claiming }()
			w.claimGroup(ctx, sendCtx, cr, group{slots: slots, tickets: tickets, tolerance: tolerance, gen: gen,
				asked: sent, lead: lead, horizon: horizon, notBefore: notBefore, flex: flex, deferClaim: deferClaim, grant: g}, inflight)
		}()
		woke = w.pauseWait(ctx, cr, time.Until(got.Add(pause)))
	}
}

// logClockStep reports a restart of the clock mapping: a sample contradicted
// the window beyond the drift allowance, so one of the clocks stepped by up to
// step_us (negative: backward). A restart that voids the slots placed with the
// old mapping (group.gen) costs throughput and is a warning; a small one, as
// a VM's periodic time sync makes, voids nothing and is only debug.
func (w *Worker) logClockStep(ctx context.Context, ch notification.Channel, r clockRestart, uncertainty time.Duration) {
	switch {
	case r.voids:
		slog.WarnContext(ctx, "limiter clock stepped; offset mapping restarted, pending slots void", "channel", ch,
			"step_us", r.step.Microseconds(), "uncertainty_us", uncertainty.Microseconds())
	case r.happened:
		slog.DebugContext(ctx, "limiter clock stepped slightly; offset mapping restarted", "channel", ch,
			"step_us", r.step.Microseconds(), "uncertainty_us", uncertainty.Microseconds())
	}
}
