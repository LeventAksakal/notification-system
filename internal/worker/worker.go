// Package worker is the delivery loop of `app worker` (rows R7, R8, R9, R11,
// R12, B1, B2). Per channel it asks the breaker, reserves the send slots of
// the next horizon in one limiter call, splits them between the priorities by
// weight, claims the rows with one query per priority, and sends each job at
// its slot. It classifies the answer and writes the result.
//
// Timing of one job: the slot comes from the limiter (Valkey's clock, exactly
// one interval apart across all workers); the request leaves at slot +
// SendAlign, never earlier. The slot is mapped to the local clock through the
// offset learnt from every Take's round trip (clockMap), never from the moment
// the reply arrived, so a late reply does not shift its group late. A job that
// cannot leave within one interval of that moment (slow claim, no free
// provider call, a timer late under load, the mapping's own uncertainty) is
// released unsent and claimed again for a later slot: it is never sent late on
// top of the full rate. Before the call the lease is checked: the call ends
// before the lease does, or it is not made.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"golang.org/x/sync/errgroup"

	"github.com/LeventAksakal/notification-system/internal/breaker"
	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/obs"
	"github.com/LeventAksakal/notification-system/internal/provider"
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
	MaxAttempts     int           // APP_MAX_ATTEMPTS
	RetryBase       time.Duration // APP_RETRY_BASE
	RetryCap        time.Duration // APP_RETRY_CAP
	Lease           time.Duration // APP_LEASE_DURATION
	ReapInterval    time.Duration // APP_REAP_INTERVAL
	ShutdownTimeout time.Duration // APP_SHUTDOWN_TIMEOUT
	Weights         [3]int        // APP_PRIORITY_WEIGHTS
	Channels        []core.Channel
	// Horizon is APP_RATE_LIMIT_HORIZON: how far ahead slots are reserved;
	// the limiter is built with the same value. Default 200ms.
	Horizon time.Duration
	// SendAlign is added to every slot (default DefaultSendAlign). It is the
	// time a claim may take: slots are reserved before the claim, so the
	// first slot of a group can be due at once; without the offset a claim of
	// 16 ms (50 ms at p99, 170 ms at start-up on the Docker setup) would miss
	// most first slots. Every request is shifted by the same amount, so the
	// spacing at the provider stays the limiter's.
	SendAlign time.Duration
	// IdleMin and IdleMax bound the idle poll of a channel with nothing due:
	// it starts at IdleMin and doubles up to IdleMax. Defaults 50ms and 250ms.
	IdleMin, IdleMax time.Duration
}

// DefaultSendAlign is SendAlign's default: above the claim's start-up p99.
const DefaultSendAlign = 200 * time.Millisecond

// Deps are the worker's collaborators.
type Deps struct {
	Queue     core.Queue
	Limiter   core.Limiter
	Sender    Sender
	Publisher core.Publisher
	// Breakers holds one breaker per channel; a channel without one gets a
	// breaker that never opens.
	Breakers map[core.Channel]*breaker.Breaker
	Tracer   trace.Tracer
	// Rand returns a uniform value in [0, n]; default math/rand/v2.
	Rand func(n int64) int64
	// Now is the clock a job's lease is checked against; default time.Now.
	Now func() time.Time
	// Meter provides the delivery metrics; default the global provider.
	Meter metric.MeterProvider
}

// Worker runs the delivery loop; Run once.
type Worker struct {
	cfg   Config
	deps  Deps
	clock *clockMap // Valkey's clock to the local one, for every channel
	m     metrics
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
	if cfg.SendAlign <= 0 {
		cfg.SendAlign = DefaultSendAlign
	}
	if cfg.Horizon <= 0 {
		cfg.Horizon = 200 * time.Millisecond
	}
	if cfg.IdleMax < cfg.IdleMin {
		cfg.IdleMax = max(250*time.Millisecond, cfg.IdleMin)
	}
	if len(cfg.Channels) == 0 {
		cfg.Channels = core.Channels
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 5 * time.Second
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
	breakers := make(map[core.Channel]*breaker.Breaker, len(cfg.Channels))
	for _, ch := range cfg.Channels {
		br := deps.Breakers[ch]
		if br == nil {
			br = breaker.New(breaker.Config{Window: time.Second, MinRequests: math.MaxInt, FailureRatio: 1, Cooldown: time.Second}, time.Now)
		}
		breakers[ch] = br
	}
	deps.Breakers = breakers
	return &Worker{cfg: cfg, deps: deps, clock: newClockMap(time.Now(), clockWindow), m: newMetrics(deps.Meter)}
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
	for _, ch := range w.cfg.Channels {
		cr := w.newChannelRun(ch, callPlaces{own: make(chan struct{}, own), shared: shared})
		g.Go(func() error {
			w.channelLoop(gctx, sendCtx, cr, &inflight)
			return nil
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
	ch core.Channel
	br *breaker.Breaker
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

	mu  sync.Mutex
	sel *selector
}

func (w *Worker) newChannelRun(ch core.Channel, calling callPlaces) *channelRun {
	return &channelRun{
		ch: ch, br: w.deps.Breakers[ch],
		calling: calling, claiming: make(chan struct{}, 1),
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

// refill is how long after a reservation the next is asked for: by then half
// the horizon can be reserved again, so one call reserves about Horizon/2 of
// slots, and the reserved time never runs out while one worker is alive.
func (w *Worker) refill() time.Duration { return max(w.cfg.Horizon/2, time.Millisecond) }

// channelLoop reserves slots and hands each group to a claim task. It
// reserves nothing while every priority is known to be empty, while the
// breaker refuses, while the channel's previous claim is running, or while
// the channel's provider calls are all in flight.
func (w *Worker) channelLoop(ctx, sendCtx context.Context, cr *channelRun, inflight *sync.WaitGroup) {
	limiterDown := false
	clockUncertain := false
	probeWait := w.cfg.IdleMin
	interval := time.Millisecond // until the first grant says
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
	for ctx.Err() == nil {
		now := time.Now()
		cr.mu.Lock()
		at := cr.sel.nextCheck(now)
		cr.mu.Unlock()
		if !at.IsZero() {
			sleep(ctx, at.Sub(now))
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
			g, err := w.deps.Limiter.Take(ctx, cr.ch, 0)
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
				lower, upper, _, restarted := w.clock.observe(sent, got, g.At)
				uncertainty = upper - lower
				w.logClockStep(ctx, cr.ch, restarted, uncertainty)
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
		first, ok, wait := cr.br.Allow()
		if !ok {
			sleep(ctx, min(wait, w.cfg.IdleMax))
			continue
		}
		select {
		case cr.claiming <- struct{}{}:
		case <-ctx.Done():
			cr.br.Cancel(first)
			return
		}
		want := int(w.cfg.Horizon/interval) + 1
		if first.Probe() {
			want = 1 // the probe is one call
		}
		sent := time.Now()
		g, err := w.deps.Limiter.Take(ctx, cr.ch, want)
		got := time.Now()
		if err != nil {
			<-cr.claiming
			cr.br.Cancel(first)
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
		pause := g.Wait + w.refill()
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
			var restarted bool
			lower, upper, gen, restarted = w.clock.observe(sent, got, g.At)
			uncertainty = upper - lower
			w.logClockStep(ctx, cr.ch, restarted, uncertainty)
			if restarted && g.N > 0 {
				// A clock stepped: the limiter's schedule may have restarted at
				// its new "now" while slots of the old one are still pending
				// (the new generation voids those). This grant is on the new
				// schedule and could leave beside them: it is not used.
				<-cr.claiming
				cr.br.Cancel(first)
				w.m.gaveUp(ctx, cr.ch, giveUpClockStepped, g.N)
				continue
			}
		}
		if g.N > 0 && uncertainty >= g.Interval {
			// The cap cannot be guaranteed: none of these slots is used. Rare:
			// the mapping was checked before the Take, and a sample that
			// restarted it is handled above; the next turn reads the clock
			// without reserving.
			<-cr.claiming
			cr.br.Cancel(first)
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
			cr.br.Cancel(first)
			sleep(ctx, pause)
			continue
		}
		tickets := make([]breaker.Ticket, 1, g.N)
		tickets[0] = first
		for len(tickets) < g.N {
			t, ok, _ := cr.br.Allow()
			if !ok {
				break // the breaker opened meanwhile: the other slots are lost
			}
			tickets = append(tickets, t)
		}
		w.m.gaveUp(ctx, cr.ch, giveUpBreakerOpened, g.N-len(tickets))
		base := w.clock.toLocal(g.At.Add(g.First), upper).Add(w.cfg.SendAlign)
		slots := make([]time.Time, len(tickets))
		for i := range slots {
			slots[i] = base.Add(time.Duration(i) * g.Interval)
		}
		tolerance := g.Interval - uncertainty // > 0: checked above
		inflight.Add(1)
		go func() { // owned by Run through inflight
			defer inflight.Done()
			defer func() { <-cr.claiming }()
			w.claimGroup(ctx, sendCtx, cr, group{slots: slots, tickets: tickets, tolerance: tolerance, gen: gen}, inflight)
		}()
		sleep(ctx, time.Until(got.Add(pause)))
	}
}

// logClockStep reports a restart of the clock mapping: a sample contradicted
// the window beyond the drift allowance, so one of the clocks stepped. Slots
// placed with the old mapping are void from then on (group.gen).
func (w *Worker) logClockStep(ctx context.Context, ch core.Channel, restarted bool, uncertainty time.Duration) {
	if restarted {
		slog.WarnContext(ctx, "limiter clock stepped; offset mapping restarted", "channel", ch,
			"uncertainty_us", uncertainty.Microseconds())
	}
}

// group is one grant's slots as placed on the local clock: their moments,
// breaker tickets, how late each may leave, and the clock mapping's
// generation they were placed under.
type group struct {
	slots     []time.Time
	tickets   []breaker.Ticket
	tolerance time.Duration
	gen       uint64
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
			cr.br.Cancel(tickets[i])
		}
		return true
	}
	if stepped() {
		return
	}
	skipMissed := func(now time.Time) {
		from := i
		for i < len(slots) && now.After(slots[i].Add(tolerance)) {
			cr.br.Cancel(tickets[i])
			i++
		}
		w.m.gaveUp(ctx, cr.ch, giveUpSlotMissed, i-from)
	}
	skipMissed(time.Now())
	jobs, err := w.claim(ctx, cr, len(slots)-i)
	if err != nil && ctx.Err() == nil {
		slog.ErrorContext(ctx, "claim failed", "channel", cr.ch, "err", err)
		cr.pause(time.Now().Add(w.cfg.IdleMax))
	}
	skipMissed(time.Now())
	voided := stepped() // the claimed rows then all go back below
	// The group's claims are announced in one publish, beside the sends: a
	// slow Valkey costs no slot. Each job's result event waits for it, so a
	// job's processing event still goes out before its result.
	announced := w.announce(sendCtx, jobs, inflight)
	// Jobs without a slot go back off the claim path too: their release event
	// waits for the publish, which must not hold up the channel's next claim.
	var back []core.Job
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
	for ; i < len(slots); i++ {
		cr.br.Cancel(tickets[i])
	}
}

// announce publishes that jobs are processing, in a goroutine owned by Run
// through inflight. The returned channel is closed once that publish is over;
// it is nil when there is nothing to publish.
func (w *Worker) announce(ctx context.Context, jobs []core.Job, inflight *sync.WaitGroup) <-chan struct{} {
	if w.deps.Publisher == nil || len(jobs) == 0 {
		return nil
	}
	done := make(chan struct{})
	inflight.Add(1)
	go func() { // owned by Run through inflight; one bounded publish
		defer inflight.Done()
		defer close(done)
		w.publishAll(ctx, jobs, core.StatusProcessing)
	}()
	return done
}

// sendAt sends job at its slot moment at, or releases it unsent when that
// cannot happen within tolerance or a clock step has voided the slot's
// generation gen. Its result is published after announced is closed (nil:
// at once).
func (w *Worker) sendAt(ctx, jctx context.Context, cr *channelRun, job core.Job, announced <-chan struct{}, at time.Time, tk breaker.Ticket, tolerance time.Duration, gen uint64) {
	sleep(ctx, time.Until(at))
	if ctx.Err() != nil {
		cr.br.Cancel(tk)
		w.release(jctx, job, announced, "shutdown before the send")
		return
	}
	late := at.Add(tolerance)
	give, ok := cr.calling.take(ctx, late)
	if !ok {
		cr.br.Cancel(tk)
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
		cr.br.Cancel(tk)
		w.m.gaveUp(ctx, cr.ch, giveUpSlotMissed, 1)
		w.release(jctx, job, announced, "slot missed")
		return
	}
	if !w.clock.current(gen) {
		// A clock step may have restarted the limiter's schedule: this slot
		// is on the old one and could leave beside the new one's.
		callDone()
		cr.br.Cancel(tk)
		w.m.gaveUp(ctx, cr.ch, giveUpClockStepped, 1)
		w.release(jctx, job, announced, "limiter clock stepped")
		return
	}
	if !cr.br.Current(tk) {
		// The breaker opened after the claim: nothing goes out (decision 0008).
		callDone()
		w.m.gaveUp(ctx, cr.ch, giveUpBreakerOpened, 1)
		w.release(jctx, job, announced, "breaker opened before the send")
		return
	}
	w.deliver(jctx, job, announced, cr.br, tk, callDone)
}

// pause makes the channel loop reserve nothing until until.
func (cr *channelRun) pause(until time.Time) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	for i := range core.Priorities {
		cr.sel.emptyUntil[i] = until
	}
}

// claim claims up to n due notifications of the channel: the selector splits
// n by weight, then each priority is asked once, high to low, for its share
// plus what the priorities above it could not use. What is still unused after
// low goes up to the priorities that may have more. A priority that returned
// fewer rows than asked is skipped for its idle poll: IdleMin after it had
// rows, doubling up to IdleMax while it keeps having none.
func (w *Worker) claim(ctx context.Context, cr *channelRun, n int) ([]core.Job, error) {
	if n <= 0 {
		return nil, nil
	}
	now := time.Now()
	cr.mu.Lock()
	counts := cr.sel.plan(n, now)
	var empty [3]bool
	for i := range empty {
		empty[i] = cr.sel.empty(i, now)
	}
	cr.mu.Unlock()

	var jobs []core.Job
	var drained [3]bool // asked and got fewer than asked
	ask := func(i, k int) (int, error) {
		got, err := w.deps.Queue.Claim(ctx, cr.ch, priorityName(i), k, w.cfg.Lease)
		if err != nil {
			return 0, err
		}
		jobs = append(jobs, got...)
		cr.mu.Lock()
		if len(got) < k {
			drained[i] = true
			cr.sel.markEmpty(i, now, len(got) > 0, w.cfg.IdleMin, w.cfg.IdleMax)
		} else {
			cr.sel.markBusy(i, now)
		}
		cr.mu.Unlock()
		return len(got), nil
	}
	carry := 0
	var err error
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
		var got int
		if got, err = ask(i, k); err != nil {
			break
		}
		carry = k - got
	}
	for i := 2; err == nil && i >= 0 && carry > 0; i-- {
		if empty[i] || drained[i] {
			continue
		}
		var got int
		if got, err = ask(i, carry); err != nil {
			break
		}
		carry -= got
	}
	return jobs, err
}

// jobContext carries the ids every log line about the job needs.
func jobContext(ctx context.Context, job core.Job) context.Context {
	return obs.WithCorrelationID(obs.WithNotificationID(ctx, job.ID), job.CorrelationID)
}

// deliver makes one provider call for a claimed job and writes the result.
// The call ends leaseMargin before the lease does (or at the provider
// timeout, whichever is first); with less than minSendTime of lease left it is
// not made and the job is released. callDone is called once the provider
// call is over (or will not be made). The result is published after
// announced is closed.
func (w *Worker) deliver(ctx context.Context, job core.Job, announced <-chan struct{}, br *breaker.Breaker, tk breaker.Ticket, callDone func()) {
	left := job.LeaseUntil.Sub(w.deps.Now()) - leaseMargin
	if left < minSendTime {
		callDone()
		br.Cancel(tk)
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
	if err != nil && ctx.Err() != nil {
		// Shutdown aborted the call: not an attempt; give the job back.
		br.Cancel(tk)
		span.SetStatus(codes.Error, "aborted by shutdown")
		w.release(ctx, job, announced, "shutdown aborted the call")
		return
	}
	if err != nil && cut {
		// No answer within the lease: a timeout, recorded now, while the
		// claim still holds.
		err = fmt.Errorf("%w: %w (%s before its end)", provider.ErrTimeout, errLeaseCut, leaseMargin)
	}
	v := provider.Classify(res.Status, err, res.Header)
	br.Done(tk, !v.ProviderFault)
	w.m.attempt(ctx, job.Channel, v.Outcome, elapsed.Seconds())
	a := core.Attempt{
		StartedAt: start, Duration: elapsed, Outcome: v.Outcome, HTTPStatus: res.Status,
		Error: v.Error, ProviderMessageID: res.MessageID,
	}
	span.SetAttributes(attribute.Int("http.response.status_code", res.Status), attribute.String("delivery.outcome", string(v.Outcome)))
	if v.Outcome != core.OutcomeAccepted {
		span.SetStatus(codes.Error, v.Error)
	}

	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()
	attempts := job.Attempts + 1
	log := []any{"channel", job.Channel, "priority", job.Priority, "attempt", attempts,
		"http_status", res.Status, "outcome", v.Outcome, "duration_ms", elapsed.Milliseconds()}

	var status core.Status
	var werr error
	switch {
	case v.Outcome == core.OutcomeAccepted:
		status = core.StatusSent
		if res.MessageID == "" {
			slog.WarnContext(ctx, "provider accepted without messageId; counted as sent", log...)
		}
		werr = w.deps.Queue.MarkSent(wctx, job, a)
	case v.Outcome == core.OutcomePermanent:
		status = core.StatusFailed
		log = append(log, "failure_kind", core.FailurePermanent, "error", v.Error)
		werr = w.deps.Queue.MarkFailed(wctx, job, a, core.FailurePermanent)
	case attempts >= w.cfg.MaxAttempts:
		status = core.StatusFailed
		log = append(log, "failure_kind", core.FailureExhausted, "error", v.Error)
		werr = w.deps.Queue.MarkFailed(wctx, job, a, core.FailureExhausted)
	default:
		status = core.StatusPending
		delay := Backoff(attempts, w.cfg.RetryBase, w.cfg.RetryCap, v.RetryAfter, w.deps.Rand)
		log = append(log, "retry_in_ms", delay.Milliseconds(), "error", v.Error)
		werr = w.deps.Queue.Reschedule(wctx, job, a, time.Now().Add(delay))
	}
	switch {
	case errors.Is(werr, core.ErrLeaseLost):
		slog.WarnContext(ctx, "lease lost before the result was written; result discarded", log...)
		return
	case werr != nil:
		// The lease expires and the reaper returns the row: it is sent again.
		slog.ErrorContext(ctx, "writing the delivery result failed", append(log, "err", werr)...)
		return
	}
	slog.InfoContext(ctx, "delivery attempt", append(log, "status", status)...)
	errText := ""
	if status != core.StatusSent {
		errText = v.Error
	}
	w.publish(ctx, job, announced, status, attempts, errText)
}

// release returns an unsent job to pending, due now, without an attempt, and
// publishes that after announced is closed.
func (w *Worker) release(ctx context.Context, job core.Job, announced <-chan struct{}, reason string) {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()
	if err := w.deps.Queue.Release(wctx, job, time.Now()); err != nil {
		slog.WarnContext(ctx, "release failed; the lease will expire", "reason", reason, "err", err)
		return
	}
	slog.DebugContext(ctx, "released unsent notification", "channel", job.Channel, "reason", reason)
	w.publish(ctx, job, announced, core.StatusPending, job.Attempts, "")
}

// publish announces job's move to st once after is closed (nil: at once), so
// it follows the event published before it.
func (w *Worker) publish(ctx context.Context, job core.Job, after <-chan struct{}, st core.Status, attempts int, errText string) {
	if w.deps.Publisher == nil {
		return
	}
	if after != nil {
		<-after // closed when one bounded publish is over
	}
	err := w.deps.Publisher.Publish(ctx, core.Event{
		NotificationID: job.ID, BatchID: job.BatchID, Status: st, Attempts: attempts,
		Error: errText, At: time.Now().UTC(), CorrelationID: job.CorrelationID,
	})
	if err != nil {
		slog.WarnContext(ctx, "publish status change", "status", st, "err", err)
	}
}

// batchPublisher is a Publisher that sends many events as one call with one
// bounded wait (realtime.Publisher).
type batchPublisher interface {
	PublishAll(ctx context.Context, es []core.Event) error
}

// publishAll announces that jobs moved to st (their attempts unchanged), in
// order: in one call when the publisher has a batch form, else one by one.
func (w *Worker) publishAll(ctx context.Context, jobs []core.Job, st core.Status) {
	if w.deps.Publisher == nil || len(jobs) == 0 {
		return
	}
	bp, ok := w.deps.Publisher.(batchPublisher)
	if !ok {
		for _, job := range jobs {
			w.publish(jobContext(ctx, job), job, nil, st, job.Attempts, "")
		}
		return
	}
	now := time.Now().UTC()
	es := make([]core.Event, len(jobs))
	for i, job := range jobs {
		es[i] = core.Event{NotificationID: job.ID, BatchID: job.BatchID, Status: st, Attempts: job.Attempts,
			At: now, CorrelationID: job.CorrelationID}
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
		// What was reaped is committed even when a later batch failed.
		w.publishAll(ctx, jobs, core.StatusPending)
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
	return fmt.Sprintf("concurrency=%d max_attempts=%d weights=%v horizon=%s send_align=%s",
		c.Concurrency, c.MaxAttempts, c.Weights, c.Horizon, c.SendAlign)
}
