// Package delivery is the delivery loop of `app worker` (rows R7, R8, R9, R11,
// R12, B1, B2). Per channel it asks the channel's shared provider health
// (decision 0026), reserves the send slots of
// the next horizon in one limiter call, splits them between the priorities by
// weight, claims the rows with one query per priority, and sends each job at
// its slot. It classifies the answer and writes the result.
//
// Timing of one job: the slot comes from the limiter (Valkey's clock, exactly
// one interval apart across all workers), reserved a lead ahead that follows
// the worker's measured claim time (lead); the request leaves at its slot,
// never earlier. The slot is mapped to the local clock through the
// offset learnt from every Take's round trip (clockMap), never from the moment
// the reply arrived, so a late reply does not shift its group late. A job that
// cannot leave within one interval of that moment (slow claim, no free
// provider call, a timer late under load, the mapping's own uncertainty) is
// released unsent and claimed again for a later slot: it is never sent late on
// top of the full rate. Before the call the lease is checked: the call ends
// before the lease does, or it is not made.
package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"golang.org/x/sync/errgroup"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
	"github.com/LeventAksakal/notification-system/internal/wake"
)

// Sender makes one provider call (provider.Client). It must end the call when
// ctx ends.
type Sender interface {
	Send(ctx context.Context, m provider.Message) (provider.Response, error)
}

// Config is the worker's dials.
type Config struct {
	// Concurrency is APP_WORKER_CONCURRENCY: provider calls in flight per
	// process. Each channel has places of its own (Concurrency/(2*channels),
	// at least 1) and shares the rest with the others, so a channel whose
	// provider hangs can hold at most its own places and the shared ones,
	// never another channel's.
	Concurrency     int
	MaxAttempts     int           // APP_MAX_ATTEMPTS: counted failures before a notification is exhausted
	RetryBase       time.Duration // APP_RETRY_BASE
	RetryCap        time.Duration // APP_RETRY_CAP
	Lease           time.Duration // APP_LEASE_DURATION
	ReapInterval    time.Duration // APP_REAP_INTERVAL
	ShutdownTimeout time.Duration // APP_SHUTDOWN_TIMEOUT
	Weights         [3]int        // APP_PRIORITY_WEIGHTS
	Channels        []notification.Channel
	// Horizon is APP_RATE_LIMIT_HORIZON: the least span of slots one Take
	// reserves past the lead (see horizon). Default 200ms.
	Horizon time.Duration
	// AlignFloor and AlignCeiling bound the send alignment, the lead a
	// group's first slot is reserved with (see lead). Defaults alignFloor and
	// alignCeiling(Lease). Not operator dials: tests fix the alignment by
	// setting both to one value.
	AlignFloor, AlignCeiling time.Duration
	// IdleMin and IdleMax bound the retry waits of a channel that cannot
	// reserve now: IdleMin while all its provider calls are in flight (a call
	// place frees within a call's time), and the clock probe's back-off from
	// IdleMin doubling to IdleMax; IdleMax after the limiter or a claim failed
	// and as the cap of a refused health check's wait. Defaults 50ms and 250ms. A channel
	// with nothing due does not poll: it sleeps until its next due time, a
	// wake hint, or SafetyPoll.
	IdleMin, IdleMax time.Duration
	// SafetyPoll is the longest an empty priority sleeps without asking the
	// database (Queue.NextDue): it bounds the delay a lost wake hint can
	// cause. Default wake.SafetyPoll; not an operator dial (tests shorten it).
	SafetyPoll time.Duration
	// ProviderTimeout is APP_PROVIDER_TIMEOUT, the Sender's own limit on one
	// call; it bounds the probe lease (probeLease). 0: the lease bounds it.
	ProviderTimeout time.Duration
	// HealthCooldown is APP_BREAKER_COOLDOWN. Rows whose failure belongs to an
	// outage are spread over one cool-down after the next probe may go, so
	// they do not come due together and declare the outage again by
	// themselves (decision 0026). 0: no spread.
	HealthCooldown time.Duration
}

// Deps are the worker's collaborators.
type Deps struct {
	Queue     notification.Queue
	Limiter   notification.Limiter
	Sender    Sender
	Publisher notification.Publisher
	// Health is the channels' provider health shared by every worker
	// (health.ValkeyState in production); nil: a state that never opens.
	Health health.State
	Tracer trace.Tracer
	// Rand returns a uniform value in [0, n]; default math/rand/v2.
	Rand func(n int64) int64
	// Now is the clock a job's lease is checked against; default time.Now.
	Now func() time.Time
	// Meter provides the delivery metrics; default the global provider.
	Meter metric.MeterProvider
	// Waker tells the other workers (and this one) that a job went back to
	// pending: released, rescheduled or reaped. Optional.
	Waker notification.Waker
	// Wakes delivers the hints published for the worker's channels
	// (wake.Listener). Optional: without it an empty priority is asked again
	// after its due time or SafetyPoll only.
	Wakes Wakes
}

// Wakes is a source of wake hints (wake.Listener). Run holds a subscription
// until ctx ends, calling onHint for each hint and onSubscribed after every
// (re)subscribe; both must not block.
type Wakes interface {
	Run(ctx context.Context, channels []notification.Channel, onHint func(notification.Wake), onSubscribed func()) error
}

// Worker runs the delivery loop; Run once.
type Worker struct {
	cfg   Config
	deps  Deps
	gates map[notification.Channel]*health.Gate
	clock *clockMap // Valkey's clock to the local one, for every channel
	m     metrics
	// runs is set by Run before anything that reads it starts.
	runs map[notification.Channel]*channelRun
}

// New returns a worker; zero dials get their defaults.
func New(cfg Config, deps Deps) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	if cfg.IdleMin <= 0 {
		cfg.IdleMin = 50 * time.Millisecond
	}
	if cfg.Horizon <= 0 {
		cfg.Horizon = 200 * time.Millisecond
	}
	if cfg.AlignFloor <= 0 {
		cfg.AlignFloor = alignFloor
	}
	if cfg.AlignCeiling <= 0 {
		cfg.AlignCeiling = alignCeiling(cfg.Lease)
	}
	cfg.AlignCeiling = max(cfg.AlignCeiling, cfg.AlignFloor)
	if cfg.IdleMax < cfg.IdleMin {
		cfg.IdleMax = max(250*time.Millisecond, cfg.IdleMin)
	}
	if len(cfg.Channels) == 0 {
		cfg.Channels = notification.Channels
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 5 * time.Second
	}
	if cfg.SafetyPoll <= 0 {
		cfg.SafetyPoll = wake.SafetyPoll
	}
	if deps.Rand == nil {
		deps.Rand = func(n int64) int64 { return rand.Int64N(n + 1) } //nolint:gosec // jitter, not security
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Tracer == nil {
		deps.Tracer = noop.NewTracerProvider().Tracer("worker")
	}
	if deps.Health == nil {
		deps.Health = health.NeverOpens()
	}
	gates := make(map[notification.Channel]*health.Gate, len(cfg.Channels))
	for _, ch := range cfg.Channels {
		gates[ch] = health.NewGate(deps.Health, ch)
	}
	// A placed slot waits at most the lead plus the horizon, both at their largest.
	span := time.Second + cfg.AlignCeiling + max(cfg.Horizon, cfg.AlignCeiling)
	return &Worker{cfg: cfg, deps: deps, gates: gates, clock: newClockMap(time.Now(), clockWindow, span), m: newMetrics(deps.Meter)}
}

// storeTimeout bounds one result write; it is not cut short by shutdown, so a
// finished send is always recorded when the database answers.
const storeTimeout = 10 * time.Second

// leaseMargin is how long before its lease ends a provider call is cut, so
// the attempt is recorded while the claim still holds. It also covers the
// skew between the local clock and the database's.
const leaseMargin = 250 * time.Millisecond

// minSendTime is the least call time worth starting; with less lease left
// the job is released instead.
const minSendTime = 100 * time.Millisecond

// errLeaseCut is the cause of a call ended by the lease, not the provider timeout.
var errLeaseCut = errors.New("call cut before the lease ends")

// Run delivers until ctx is cancelled. Then it stops claiming, lets in-flight
// sends finish for up to ShutdownTimeout, releases what was claimed but not
// sent, and returns after every goroutine it started has ended.
func (w *Worker) Run(ctx context.Context) error {
	// Sends outlive ctx by up to the shutdown grace.
	sendCtx, cancelSends := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSends()

	var inflight sync.WaitGroup
	n := len(w.cfg.Channels)
	own := max(1, w.cfg.Concurrency/(2*n))
	shared := make(chan struct{}, max(0, w.cfg.Concurrency-own*n))
	g, gctx := errgroup.WithContext(ctx)
	w.runs = make(map[notification.Channel]*channelRun, n)
	for _, ch := range w.cfg.Channels {
		w.runs[ch] = w.newChannelRun(ch, callPlaces{own: make(chan struct{}, own), shared: shared})
	}
	for _, cr := range w.runs {
		g.Go(func() error {
			w.channelLoop(gctx, sendCtx, cr, &inflight)
			return nil
		})
	}
	if w.deps.Wakes != nil {
		g.Go(func() error {
			return w.deps.Wakes.Run(gctx, w.cfg.Channels, w.onHint, w.onSubscribed)
		})
	}
	g.Go(func() error {
		w.reaper(gctx)
		return nil
	})
	err := g.Wait()

	done := make(chan struct{})
	go func() { // owned here: Run waits on done below before returning
		inflight.Wait()
		close(done)
	}()
	grace := time.NewTimer(w.cfg.ShutdownTimeout)
	defer grace.Stop()
	select {
	case <-done:
	case <-grace.C:
		slog.WarnContext(sendCtx, "shutdown grace over; aborting in-flight sends", "grace", w.cfg.ShutdownTimeout.String())
		cancelSends()
		<-done
	}
	return err
}

// channelRun is what a channel's loop, its claim groups and its sends share.
type channelRun struct {
	ch   notification.Channel
	gate *health.Gate // the channel's shared provider health, as this worker sees it
	// calling bounds the provider calls in flight on this channel. While no
	// place is free the loop reserves no slots.
	calling callPlaces
	// claiming holds the one claim in flight on this channel. The next slots
	// are reserved only once the previous claim is done: a claim normally
	// takes a fraction of refill, and while the database is slow this worker
	// reserves less and the other workers take the slots. Claims of one
	// worker so stay in slot order, and a slot is reserved only by a worker
	// that can claim for it at once; overlapping claims near the end of a
	// backlog would hand the last rows to later slots and leave earlier ones
	// empty.
	claiming chan struct{}
	// latency is this worker's measured claim time on the channel: written
	// by the claim in flight, read by the loop before each reservation.
	latency claimLatency
	// wake cuts the channel loop's idle wait short when a hint arrives.
	wake chan struct{}

	mu  sync.Mutex
	sel *selector
}

func (w *Worker) newChannelRun(ch notification.Channel, calling callPlaces) *channelRun {
	return &channelRun{
		ch: ch, gate: w.gates[ch],
		calling: calling, claiming: make(chan struct{}, 1), wake: make(chan struct{}, 1),
		sel: newSelector(w.cfg.Weights),
	}
}

// callPlaces are the places a channel's provider calls take: the channel's
// own first, then the process's shared ones.
type callPlaces struct {
	own    chan struct{}
	shared chan struct{}
}

// full reports whether no place is free now.
func (p callPlaces) full() bool {
	return len(p.own) == cap(p.own) && len(p.shared) == cap(p.shared)
}

// take takes a place, waiting until deadline at most; it returns the func
// that gives the place back, or false when none came free in time or ctx ended.
func (p callPlaces) take(ctx context.Context, deadline time.Time) (func(), bool) {
	giveOwn := func() { <-p.own }
	giveShared := func() { <-p.shared }
	select {
	case p.own <- struct{}{}:
		return giveOwn, true
	default:
	}
	select {
	case p.shared <- struct{}{}:
		return giveShared, true
	default:
	}
	t := time.NewTimer(time.Until(deadline))
	defer t.Stop()
	select {
	case p.own <- struct{}{}:
		return giveOwn, true
	case p.shared <- struct{}{}:
		return giveShared, true
	case <-t.C:
	case <-ctx.Done():
	}
	return nil, false
}

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
		cr.mu.Unlock()
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
		// The grid was free for an interval: every request of an earlier
		// slot left more than an interval before the limiter's now of this
		// grant, lateness and clock offset included (see group.flex).
		notBefore := w.clock.toLocal(g.At, upper)
		flex := g.Idle >= g.Interval
		deferClaim := woke && !flex
		woke = false
		inflight.Add(1)
		go func() { // owned by Run through inflight
			defer inflight.Done()
			defer func() { <-cr.claiming }()
			w.claimGroup(ctx, sendCtx, cr, group{slots: slots, tickets: tickets, tolerance: tolerance, gen: gen,
				asked: sent, lead: lead, horizon: horizon, notBefore: notBefore, flex: flex, deferClaim: deferClaim, grant: g}, inflight)
		}()
		sleep(ctx, time.Until(got.Add(pause)))
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
	// notBefore is the limiter's now of the grant on the local clock, placed
	// like the slots (never early on Valkey's clock): no flex send leaves
	// before it.
	notBefore time.Time
	// flex: the limiter's grid had been free for at least one interval
	// before this grant (Grant.Idle), so the group may leave as soon as it
	// is claimed instead of at its slots, never before notBefore. On a free
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
	//    way, so a flex send is never before N on V.
	//  - Earlier grants: their last slot is at most T-I, so every request of
	//    theirs has left before T on V, the latest one included.
	//  - Idle is N-T, both read by the limiter on V: no worker clock in it.
	//    Idle >= I puts N, and so the first flex send, at T+I or later: more
	//    than I after the last earlier send. The unbroken grid only
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
	// their moments; a busy channel never defers (woke is false).
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
		w.giveBack(ctx, cr, gr.grant, from, gr.grant.N-i)
	}
	for ; i < len(slots); i++ {
		w.cancel(ctx, cr, tickets[i])
	}
}

// announce publishes that jobs are processing, in a goroutine owned by Run
// through inflight. The returned channel is closed once that publish is over;
// it is nil when there is nothing to publish.
func (w *Worker) announce(ctx context.Context, jobs []notification.Job, inflight *sync.WaitGroup) <-chan struct{} {
	if w.deps.Publisher == nil || len(jobs) == 0 {
		return nil
	}
	done := make(chan struct{})
	inflight.Add(1)
	go func() { // owned by Run through inflight; one bounded publish
		defer inflight.Done()
		defer close(done)
		w.publishAll(ctx, jobs, notification.StatusProcessing)
	}()
	return done
}

// sendAt sends job at its slot moment at, or releases it unsent when that
// cannot happen within tolerance or a clock step has voided the slot's
// generation gen. Its result is published after announced is closed (nil:
// at once).
func (w *Worker) sendAt(ctx, jctx context.Context, cr *channelRun, job notification.Job, announced <-chan struct{}, at time.Time, tk health.Ticket, tolerance time.Duration, gen uint64) {
	if w.spent(jctx, cr, job, announced, tk) {
		return
	}
	sleep(ctx, time.Until(at))
	if ctx.Err() != nil {
		w.cancel(ctx, cr, tk)
		w.release(jctx, job, announced, "shutdown before the send")
		return
	}
	late := at.Add(tolerance)
	give, ok := cr.calling.take(ctx, late)
	if !ok {
		w.cancel(ctx, cr, tk)
		reason := "no free provider call at its slot"
		if ctx.Err() != nil {
			reason = "shutdown before the send"
		} else {
			w.m.gaveUp(ctx, cr.ch, giveUpNoFreeCall, 1)
		}
		w.release(jctx, job, announced, reason)
		return
	}
	// The place is held for the provider call only, not for the database
	// writes around it.
	var once sync.Once
	callDone := func() { once.Do(give) }
	defer callDone()
	if time.Now().After(late) {
		callDone()
		w.cancel(ctx, cr, tk)
		w.m.gaveUp(ctx, cr.ch, giveUpSlotMissed, 1)
		w.release(jctx, job, announced, "slot missed")
		return
	}
	if !w.clock.current(gen) {
		// A forward clock step of an interval or more (clockMap.gen): this
		// slot is on the old mapping and could leave beside the new one's.
		callDone()
		w.cancel(ctx, cr, tk)
		w.m.gaveUp(ctx, cr.ch, giveUpClockStepped, 1)
		w.release(jctx, job, announced, "limiter clock stepped")
		return
	}
	if !cr.gate.Current(tk) {
		// An outage was declared after the claim, as this worker last saw the
		// shared health: nothing goes out (decision 0026).
		callDone()
		w.m.gaveUp(ctx, cr.ch, giveUpBreakerOpened, 1)
		w.release(jctx, job, announced, "provider outage declared before the send")
		return
	}
	// Slot and leaving moment on this process's clock: beside the provider's
	// arrival times they tell a dense grid from requests bunched in transit.
	slog.DebugContext(jctx, "request leaving", "channel", cr.ch, "id", job.ID, "slot_us", at.UnixMicro(), "leave_us", time.Now().UnixMicro())
	w.deliver(jctx, job, announced, cr.gate, tk, callDone)
}

// pause makes the channel loop reserve nothing until until.
func (cr *channelRun) pause(until time.Time) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	for i := range notification.Priorities {
		cr.sel.emptyUntil[i], cr.sel.check[i] = until, false
	}
}

// claim claims up to n due notifications of the channel: the selector splits
// n by weight, then each priority is asked once, high to low, for its share
// plus what the priorities above it could not use. What is still unused after
// low goes up to the priorities that may have more. It returns the priorities
// that returned fewer rows than asked (drained) and the round they belong to:
// the caller settles them with settleEmpty once the sends are started.
// queried reports whether the database was asked at all.
//
// A priority is not asked only to fill slots another could not use when it
// is known to be empty, its safety poll is due, or it was never answered for
// (selector.known: a priority with no weight is reached only that way). Each
// such ask is one more claim query in the same claim, and while the database
// is slow (owner item 0011: 1.6 s a query) one short priority tripled the
// claim time, every worker missed its whole group at once and the channel
// stopped for 4 s. Those priorities are returned in polled instead: the
// caller asks Queue.NextDue for them after the sends are started, and the
// next round claims one that has a row due.
func (w *Worker) claim(ctx context.Context, cr *channelRun, n int) (jobs []notification.Job, drained, polled []int, r round, queried bool, err error) {
	if n <= 0 {
		return nil, nil, nil, round{}, false, nil
	}
	now := time.Now()
	cr.mu.Lock()
	r = cr.sel.beginRound()
	counts := cr.sel.plan(n, now)
	var empty, unknown [3]bool
	for i := range empty {
		empty[i] = cr.sel.empty(i, now)
		unknown[i] = !cr.sel.known[i]
	}
	_, polled = cr.sel.due(now) // safety polls due: NextDue, not claimed
	cr.mu.Unlock()

	var short [3]bool // asked and got fewer than asked
	ask := func(i, k int) (int, error) {
		queried = true
		unknown[i] = false
		got, err := w.deps.Queue.Claim(ctx, cr.ch, priorityName(i), k, w.cfg.Lease)
		if err != nil {
			return 0, err
		}
		jobs = append(jobs, got...)
		if len(got) < k {
			short[i] = true
		} else {
			cr.mu.Lock()
			cr.sel.markBusy(i)
			cr.mu.Unlock()
		}
		return len(got), nil
	}
	// lookUp records that a borrowed slot skipped priority i because it was
	// never answered for: NextDue answers it after the claim.
	lookUp := func(i int) {
		if !slices.Contains(polled, i) {
			polled = append(polled, i)
		}
	}
	carry := 0
	for i := range 3 {
		k := counts[i] + carry
		carry = 0
		if k == 0 {
			continue
		}
		if empty[i] {
			carry = k
			continue
		}
		if counts[i] == 0 && unknown[i] {
			carry = k
			lookUp(i)
			continue
		}
		var got int
		if got, err = ask(i, k); err != nil {
			break
		}
		carry = k - got
	}
	for i := 2; err == nil && i >= 0 && carry > 0; i-- {
		if empty[i] || short[i] {
			continue
		}
		if unknown[i] {
			lookUp(i)
			continue
		}
		var got int
		if got, err = ask(i, carry); err != nil {
			break
		}
		carry -= got
	}
	for i, d := range short {
		if d {
			drained = append(drained, i)
		}
	}
	return jobs, drained, polled, r, queried, err
}

// settleEmpty asks the database when the channel's next notification of each
// priority in idx is due and sleeps each until then: nothing pending, or due
// later than SafetyPoll, sleeps SafetyPoll as a safety check (a lost hint
// costs at most that); due within it sleeps exactly until it (the wait is on
// the database's clock, so skew does not matter); due now sleeps dueNow (a
// row this round did not get: another claimer holds it, or it was committed
// after the claim began, in which case its hint wakes the channel anyway).
// It runs inside round r, holding the channel's claiming place.
func (w *Worker) settleEmpty(ctx context.Context, cr *channelRun, idx []int, r round, dueNow time.Duration) {
	next, err := w.deps.Queue.NextDue(ctx, cr.ch)
	now := time.Now()
	if err != nil && ctx.Err() == nil {
		slog.ErrorContext(ctx, "reading the next due time failed", "channel", cr.ch, "err", err)
	}
	cr.mu.Lock()
	defer cr.mu.Unlock()
	for _, i := range idx {
		wait, pending := next[priorityName(i)]
		switch {
		case err != nil:
			cr.sel.settle(i, now.Add(w.cfg.IdleMax), true, r) // ask again after the claim-failure pause
		case !pending || wait >= w.cfg.SafetyPoll:
			cr.sel.settle(i, now.Add(w.cfg.SafetyPoll), true, r)
		case wait > 0:
			cr.sel.settle(i, now.Add(wait), false, r)
		default:
			cr.sel.settle(i, now.Add(dueNow), false, r)
			cr.sel.raced[i] = dueNow > 0
		}
	}
}

// peek runs a round of NextDue alone for the priorities whose safety poll
// came due; one due now is claimed at once.
func (w *Worker) peek(ctx context.Context, cr *channelRun, idx []int) {
	select {
	case cr.claiming <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-cr.claiming }()
	cr.mu.Lock()
	r := cr.sel.beginRound()
	cr.mu.Unlock()
	w.settleEmpty(ctx, cr, idx, r, 0)
}

// idleWait sleeps for d, until ctx ends, or until a wake hint for the channel.
func (w *Worker) idleWait(ctx context.Context, cr *channelRun, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	case <-cr.wake:
	}
}

// onHint applies a wake hint: its priority is asked again by its due time.
func (w *Worker) onHint(h notification.Wake) {
	if cr := w.runs[h.Channel]; cr != nil {
		cr.hint(h.Priority, h.Due)
	}
}

// onSubscribed asks every priority of every channel at once: hints published
// while the subscription was down are lost, the rows they announced are not.
func (w *Worker) onSubscribed() {
	for _, cr := range w.runs {
		for _, p := range notification.Priorities {
			cr.hint(p, time.Time{})
		}
	}
}

// hint lowers priority p's sleep to due and wakes the channel loop.
func (cr *channelRun) hint(p notification.Priority, due time.Time) {
	i := priorityIndex(p)
	if i < 0 {
		return
	}
	cr.mu.Lock()
	cr.sel.hint(i, due)
	cr.mu.Unlock()
	select {
	case cr.wake <- struct{}{}:
	default: // a wake is already pending
	}
}

// wakeAll hints that jobs became pending: to this worker at once and, through
// Deps.Waker, to the others. Best effort.
func (w *Worker) wakeAll(ctx context.Context, hints ...notification.Wake) {
	for _, h := range hints {
		w.onHint(h)
	}
	if w.deps.Waker == nil || len(hints) == 0 {
		return
	}
	if err := w.deps.Waker.Wake(ctx, hints...); err != nil {
		slog.WarnContext(ctx, "wake hint", "count", len(hints), "err", err)
	}
}

// later returns the later of a and b.
func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// jobContext carries the ids every log line about the job needs.
func jobContext(ctx context.Context, job notification.Job) context.Context {
	return telemetry.WithCorrelationID(telemetry.WithNotificationID(ctx, job.ID), job.CorrelationID)
}

// deliver makes one provider call for a claimed job and writes the result.
// The call ends leaseMargin before the lease does (or at the provider
// timeout, whichever is first); with less than minSendTime of lease left it is
// not made and the job is released. callDone is called once the provider
// call is over (or will not be made). The result is published after
// announced is closed.
func (w *Worker) deliver(ctx context.Context, job notification.Job, announced <-chan struct{}, gate *health.Gate, tk health.Ticket, callDone func()) {
	left := job.LeaseUntil.Sub(w.deps.Now()) - leaseMargin
	if left < minSendTime {
		callDone()
		w.cancelGate(ctx, gate, tk)
		slog.WarnContext(ctx, "lease too close to its end to send; released", "channel", job.Channel,
			"lease_left_ms", (left + leaseMargin).Milliseconds())
		w.m.gaveUp(ctx, job.Channel, giveUpLeaseTooShort, 1)
		w.release(ctx, job, announced, "lease too short")
		return
	}
	// The delivery span continues the accepting request's trace (B5): it is a
	// child of the stored traceparent, or the root of a new trace without one.
	if job.Traceparent != "" {
		ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": job.Traceparent})
	}
	ctx, span := w.deps.Tracer.Start(ctx, "deliver "+string(job.Channel), trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("notification.id", job.ID),
			attribute.String("notification.channel", string(job.Channel)),
			attribute.String("notification.priority", string(job.Priority)),
			attribute.Int("notification.attempt", job.Attempts+1),
			attribute.String("correlation.id", job.CorrelationID),
		))
	defer span.End()

	start := time.Now()
	callCtx, cancelCall := context.WithTimeoutCause(ctx, left, errLeaseCut)
	res, err := w.deps.Sender.Send(callCtx, provider.Message{
		Channel: job.Channel, To: job.To, Content: job.Content, Key: job.ID, CorrelationID: job.CorrelationID,
	})
	cut := errors.Is(context.Cause(callCtx), errLeaseCut)
	cancelCall()
	elapsed := time.Since(start)
	callDone()
	var v provider.Verdict
	var nx next
	if err != nil && ctx.Err() != nil {
		// Shutdown aborted the call after the request may have left: an
		// unknown attempt (decision 0005), due again at once. It says nothing
		// about the provider or the message, so the shared health learns
		// nothing and the budget is not spent (0026). No response: no
		// duration or status is kept.
		w.cancelGate(ctx, gate, tk)
		v = provider.Verdict{Outcome: notification.OutcomeUnknown, Error: "shutdown aborted the call"}
		res, elapsed = provider.Response{}, 0
		nx = next{status: notification.StatusPending, budgetSpent: job.BudgetSpent}
	} else {
		if err != nil && cut {
			// No answer within the lease: a timeout, recorded now, while the
			// claim still holds.
			err = fmt.Errorf("%w: %w (%s before its end)", provider.ErrTimeout, errLeaseCut, leaseMargin)
		}
		v = provider.Classify(res.Status, err, res.Header)
		healthy, healthWait := w.reportHealth(ctx, job.Channel, gate, tk, v)
		nx = decide(v, healthy, healthWait, job.BudgetSpent, w.cfg, w.deps.Rand)
	}
	w.m.attempt(ctx, job.Channel, v.Outcome, elapsed.Seconds())
	a := notification.Attempt{
		StartedAt: start, Duration: elapsed, Outcome: v.Outcome, HTTPStatus: res.Status,
		Error: v.Error, ProviderMessageID: res.MessageID, Counted: nx.counted,
	}
	span.SetAttributes(attribute.Int("http.response.status_code", res.Status), attribute.String("delivery.outcome", string(v.Outcome)))
	if v.Outcome != notification.OutcomeAccepted {
		span.SetStatus(codes.Error, v.Error)
	}

	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()
	attempts := job.Attempts + 1
	log := []any{"channel", job.Channel, "priority", job.Priority, "attempt", attempts,
		"http_status", res.Status, "outcome", v.Outcome, "duration_ms", elapsed.Milliseconds()}
	if v.Outcome != notification.OutcomeAccepted {
		log = append(log, "counted", nx.counted, "budget_spent", nx.budgetSpent)
	}

	status := nx.status
	var retryAt time.Time // set when the job is rescheduled
	var werr error
	switch status {
	case notification.StatusSent:
		if res.MessageID == "" {
			slog.WarnContext(ctx, "provider accepted without messageId; counted as sent", log...)
		}
		werr = w.deps.Queue.MarkSent(wctx, job, a)
	case notification.StatusFailed:
		log = append(log, "failure_kind", nx.kind, "error", v.Error)
		werr = w.deps.Queue.MarkFailed(wctx, job, a, nx.kind)
	default:
		log = append(log, "retry_in_ms", nx.retryIn.Milliseconds(), "error", v.Error)
		retryAt = time.Now().Add(nx.retryIn)
		werr = w.deps.Queue.Reschedule(wctx, job, a, retryAt)
	}
	switch {
	case errors.Is(werr, notification.ErrLeaseLost):
		slog.WarnContext(ctx, "lease lost before the result was written; result discarded", log...)
		return
	case werr != nil:
		// The lease expires and the reaper returns the row: it is sent again.
		slog.ErrorContext(ctx, "writing the delivery result failed", append(log, "err", werr)...)
		return
	}
	slog.InfoContext(ctx, "delivery attempt", append(log, "status", status)...)
	if status == notification.StatusPending {
		w.wakeAll(ctx, notification.Wake{Channel: job.Channel, Priority: job.Priority, Due: retryAt})
	}
	errText := ""
	if status != notification.StatusSent {
		errText = v.Error
	}
	// The write was fenced by the claim's claim_version: it is the first
	// change since the claim.
	w.publish(ctx, job, announced, status, attempts, errText, job.StatusSeq+1)
}

// release returns an unsent job to pending, due now, without an attempt, and
// publishes that after announced is closed.
func (w *Worker) release(ctx context.Context, job notification.Job, announced <-chan struct{}, reason string) {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()
	if err := w.deps.Queue.Release(wctx, job, time.Now()); err != nil {
		slog.WarnContext(ctx, "release failed; the lease will expire", "reason", reason, "err", err)
		return
	}
	slog.DebugContext(ctx, "released unsent notification", "channel", job.Channel, "reason", reason)
	w.wakeAll(ctx, notification.Wake{Channel: job.Channel, Priority: job.Priority, Due: time.Now()})
	w.publish(ctx, job, announced, notification.StatusPending, job.Attempts, "", job.StatusSeq+1) // fenced, as a result
}

// publish announces job's move to st once after is closed (nil: at once), so
// it follows the event published before it.
// seq is the notification's status_seq after the change.
func (w *Worker) publish(ctx context.Context, job notification.Job, after <-chan struct{}, st notification.Status, attempts int, errText string, seq uint64) {
	if w.deps.Publisher == nil {
		return
	}
	if after != nil {
		<-after // closed when one bounded publish is over
	}
	err := w.deps.Publisher.Publish(ctx, notification.Event{
		NotificationID: job.ID, BatchID: job.BatchID, Status: st, Attempts: attempts,
		Error: errText, At: time.Now().UTC(), CorrelationID: job.CorrelationID, Seq: seq,
	})
	if err != nil {
		slog.WarnContext(ctx, "publish status change", "status", st, "err", err)
	}
}

// batchPublisher is a Publisher that sends many events as one call with one
// bounded wait (realtime.Publisher).
type batchPublisher interface {
	PublishAll(ctx context.Context, es []notification.Event) error
}

// publishAll announces that jobs moved to st (their attempts unchanged, their
// StatusSeq the change's), in order: in one call when the publisher has a
// batch form, else one by one.
func (w *Worker) publishAll(ctx context.Context, jobs []notification.Job, st notification.Status) {
	if w.deps.Publisher == nil || len(jobs) == 0 {
		return
	}
	bp, ok := w.deps.Publisher.(batchPublisher)
	if !ok {
		for _, job := range jobs {
			w.publish(jobContext(ctx, job), job, nil, st, job.Attempts, "", job.StatusSeq)
		}
		return
	}
	now := time.Now().UTC()
	es := make([]notification.Event, len(jobs))
	for i, job := range jobs {
		es[i] = notification.Event{NotificationID: job.ID, BatchID: job.BatchID, Status: st, Attempts: job.Attempts,
			At: now, CorrelationID: job.CorrelationID, Seq: job.StatusSeq}
	}
	if err := bp.PublishAll(ctx, es); err != nil {
		slog.WarnContext(ctx, "publish status changes", "status", st, "count", len(es), "err", err)
	}
}

// reaper returns expired leases to pending and publishes each change.
func (w *Worker) reaper(ctx context.Context) {
	t := time.NewTicker(w.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		jobs, err := w.deps.Queue.ReapExpired(ctx)
		if err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "reap expired leases", "err", err)
		}
		if len(jobs) > 0 {
			slog.InfoContext(ctx, "returned expired leases to pending", "count", len(jobs))
		}
		// What was reaped is committed even when a later batch failed. A
		// reaped job carries no priority: every priority of its channel is
		// hinted.
		var hints []notification.Wake
		seen := map[notification.Channel]bool{}
		now := time.Now()
		for _, job := range jobs {
			if !seen[job.Channel] {
				seen[job.Channel] = true
				for _, p := range notification.Priorities {
					hints = append(hints, notification.Wake{Channel: job.Channel, Priority: p, Due: now})
				}
			}
		}
		w.wakeAll(ctx, hints...)
		w.publishAll(ctx, jobs, notification.StatusPending)
	}
}

// Backoff is the wait before retry number attempt (1-based: the provider
// calls made so far): random(0, min(cap, base*2^attempt)), never shorter than
// the provider's Retry-After (decision 0007). rnd returns a value in [0, n].
func Backoff(attempt int, base, ceiling, retryAfter time.Duration, rnd func(n int64) int64) time.Duration {
	bound := ceiling
	if attempt < 62 && base > 0 && base <= ceiling>>uint(attempt) { //nolint:gosec // attempt < 62
		bound = base << uint(attempt) //nolint:gosec // attempt < 62
	}
	d := time.Duration(0)
	if bound > 0 {
		d = time.Duration(rnd(int64(bound)))
	}
	return max(d, retryAfter)
}

func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// String is for logs of a worker's configuration.
func (c Config) String() string {
	return fmt.Sprintf("concurrency=%d max_attempts=%d weights=%v horizon=%s send_align=[%s,%s]",
		c.Concurrency, c.MaxAttempts, c.Weights, c.Horizon, c.AlignFloor, c.AlignCeiling)
}

// giveBack returns the unused slots of grant g, n of them, from the moment
// from on (the limiter's clock) to the limiter, when the claim found fewer
// rows than slots, or none: the next
// Take, of any worker, then gets them instead of slots up to a horizon
// further out, and a row due right after a backlog ended finds the grid free
// (group.flex) rather than behind the unused reservation. It is one Valkey
// call per group that ended a backlog, made while the channel's claiming
// place is still held, so this worker's next Take sees its effect. A failure
// only leaves the slots unused, as they were before.
// A group sent early (group.flex) gives back from where its sends ended, not
// from its unused slot: a row due right after a lone send then waits about
// one interval, not a lead plus a horizon.
func (w *Worker) giveBack(ctx context.Context, cr *channelRun, g notification.Grant, from time.Time, n int) {
	ok, err := w.deps.Limiter.GiveBack(ctx, cr.ch, g, from)
	if err != nil && ctx.Err() == nil {
		slog.WarnContext(ctx, "giving unused send slots back failed", "channel", cr.ch, "slots", n, "err", err)
		return
	}
	slog.DebugContext(ctx, "unused send slots given back", "channel", cr.ch, "slots", n, "given_back", ok)
}
