// Package worker is the delivery loop of `app worker` (rows R7, R8, R9, R11,
// R12, B1, B2). Per channel it asks the breaker, reserves the send slots of
// the next horizon in one limiter call, splits them between the priorities by
// weight, claims the rows with one query per priority, and sends each job at
// its slot. It classifies the answer and writes the result.
//
// Timing of one job: the slot comes from the limiter (Valkey's clock, exactly
// one interval apart across all workers); the request leaves at slot +
// SendAlign, never earlier. A job that cannot leave within one interval of
// that moment (slow claim, no free provider call, a timer late under load) is
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
	// process. Each channel gets an equal share (at least 1), so a channel
	// whose provider hangs cannot hold the others' calls.
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
}

// Worker runs the delivery loop; Run once.
type Worker struct {
	cfg  Config
	deps Deps
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
	return &Worker{cfg: cfg, deps: deps}
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
	share := max(1, w.cfg.Concurrency/len(w.cfg.Channels))
	g, gctx := errgroup.WithContext(ctx)
	for _, ch := range w.cfg.Channels {
		cr := w.newChannelRun(ch, share)
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
	// calling bounds the provider calls in flight on this channel: its share
	// of Concurrency. While it is full the loop reserves no slots.
	calling chan struct{}
	// claiming bounds the claim groups in flight. A group claimed more than
	// SendAlign after its slots were reserved has missed them, and a group
	// starts every refill, so ceil(SendAlign/refill)+1 groups can be useful
	// at once; more would only claim rows to release them.
	claiming chan struct{}

	mu   sync.Mutex
	sel  *selector
	idle time.Duration // current idle poll; IdleMin while work is found
}

func (w *Worker) newChannelRun(ch core.Channel, share int) *channelRun {
	refill := w.refill()
	groups := int((w.cfg.SendAlign+refill-1)/refill) + 1
	return &channelRun{
		ch: ch, br: w.deps.Breakers[ch],
		calling: make(chan struct{}, share), claiming: make(chan struct{}, groups),
		sel: newSelector(w.cfg.Weights), idle: w.cfg.IdleMin,
	}
}

// refill is how long after a reservation the next is asked for: by then half
// the horizon can be reserved again, so one call reserves about Horizon/2 of
// slots, and the reserved time never runs out while one worker is alive.
func (w *Worker) refill() time.Duration { return max(w.cfg.Horizon/2, time.Millisecond) }

// channelLoop reserves slots and hands each group to a claim task. It never
// waits on the database, so reservations keep their cadence however long a
// claim takes. It reserves nothing while every priority is known to be empty,
// while the breaker refuses, or while the channel's provider calls are all in
// flight.
func (w *Worker) channelLoop(ctx, sendCtx context.Context, cr *channelRun, inflight *sync.WaitGroup) {
	limiterDown := false
	interval := time.Millisecond // until the first grant says
	for ctx.Err() == nil {
		now := time.Now()
		cr.mu.Lock()
		at := cr.sel.nextCheck(now)
		cr.mu.Unlock()
		if !at.IsZero() {
			sleep(ctx, at.Sub(now))
			continue
		}
		if len(cr.calling) == cap(cr.calling) {
			sleep(ctx, w.cfg.IdleMin)
			continue
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
		g, err := w.deps.Limiter.Take(ctx, cr.ch, want)
		takenAt := time.Now()
		if err != nil {
			<-cr.claiming
			cr.br.Cancel(first)
			if ctx.Err() != nil {
				return
			}
			if !limiterDown {
				slog.ErrorContext(ctx, "rate limiter unreachable; channel sends nothing", "channel", cr.ch, "err", err)
				limiterDown = true
			}
			sleep(ctx, w.cfg.IdleMax)
			continue
		}
		if limiterDown {
			slog.InfoContext(ctx, "rate limiter reachable again", "channel", cr.ch)
			limiterDown = false
		}
		if g.Interval > 0 {
			interval = g.Interval
		}
		pause := g.Wait + w.refill()
		if first.Probe() {
			pause = g.Wait
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
		// Server-now maps to a local moment within the Take's round trip;
		// takenAt is its latest bound, so no request leaves before its slot.
		base := takenAt.Add(g.First + w.cfg.SendAlign)
		slots := make([]time.Time, len(tickets))
		for i := range slots {
			slots[i] = base.Add(time.Duration(i) * g.Interval)
		}
		tolerance := max(g.Interval, time.Millisecond)
		inflight.Add(1)
		go func() { // owned by Run through inflight
			defer inflight.Done()
			defer func() { <-cr.claiming }()
			w.claimGroup(ctx, sendCtx, cr, slots, tickets, tolerance, inflight)
		}()
		sleep(ctx, time.Until(takenAt.Add(pause)))
	}
}

// claimGroup claims rows for a group of slots and starts one send per row,
// each at its slot. A slot whose moment passed (by more than tolerance)
// before a row was there for it is lost; so is a slot no row was found for.
func (w *Worker) claimGroup(ctx, sendCtx context.Context, cr *channelRun, slots []time.Time, tickets []breaker.Ticket, tolerance time.Duration, inflight *sync.WaitGroup) {
	i := 0
	skipMissed := func(now time.Time) {
		for i < len(slots) && now.After(slots[i].Add(tolerance)) {
			cr.br.Cancel(tickets[i])
			i++
		}
	}
	skipMissed(time.Now())
	jobs, err := w.claim(ctx, cr, len(slots)-i)
	if err != nil && ctx.Err() == nil {
		slog.ErrorContext(ctx, "claim failed", "channel", cr.ch, "err", err)
		cr.pause(time.Now().Add(w.cfg.IdleMax))
	}
	skipMissed(time.Now())
	for _, job := range jobs {
		jctx := jobContext(sendCtx, job)
		w.publish(jctx, job, core.StatusProcessing, job.Attempts, "")
		if i == len(slots) {
			w.release(jctx, job, "claim outlasted its slots")
			continue
		}
		at, tk := slots[i], tickets[i]
		i++
		inflight.Add(1)
		go func() { // owned by Run through inflight
			defer inflight.Done()
			w.sendAt(ctx, jctx, cr, job, at, tk, tolerance)
		}()
	}
	for ; i < len(slots); i++ {
		cr.br.Cancel(tickets[i])
	}
}

// sendAt sends job at its slot moment at, or releases it unsent when that
// cannot happen within tolerance.
func (w *Worker) sendAt(ctx, jctx context.Context, cr *channelRun, job core.Job, at time.Time, tk breaker.Ticket, tolerance time.Duration) {
	sleep(ctx, time.Until(at))
	if ctx.Err() != nil {
		cr.br.Cancel(tk)
		w.release(jctx, job, "shutdown before the send")
		return
	}
	late := at.Add(tolerance)
	select {
	case cr.calling <- struct{}{}:
	default:
		t := time.NewTimer(time.Until(late))
		select {
		case cr.calling <- struct{}{}:
			t.Stop()
		case <-t.C:
			cr.br.Cancel(tk)
			w.release(jctx, job, "no free provider call at its slot")
			return
		case <-ctx.Done():
			t.Stop()
			cr.br.Cancel(tk)
			w.release(jctx, job, "shutdown before the send")
			return
		}
	}
	defer func() { <-cr.calling }()
	if time.Now().After(late) {
		cr.br.Cancel(tk)
		w.release(jctx, job, "slot missed")
		return
	}
	if !cr.br.Current(tk) {
		// The breaker opened after the claim: nothing goes out (decision 0008).
		w.release(jctx, job, "breaker opened before the send")
		return
	}
	w.deliver(jctx, job, cr.br, tk)
}

// pause makes the channel loop reserve nothing until until.
func (cr *channelRun) pause(until time.Time) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	for i := range core.Priorities {
		cr.sel.markEmpty(i, until)
	}
}

// claim claims up to n due notifications of the channel: the selector splits
// n by weight, then each priority is asked once, high to low, for its share
// plus what the priorities above it could not use. What is still unused after
// low goes up to the priorities that may have more. A priority that returned
// fewer rows than asked is skipped for the current idle poll, which doubles
// while nothing at all is found.
func (w *Worker) claim(ctx context.Context, cr *channelRun, n int) ([]core.Job, error) {
	if n <= 0 {
		return nil, nil
	}
	now := time.Now()
	cr.mu.Lock()
	counts, idle := cr.sel.plan(n, now), cr.idle
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
			cr.sel.markEmpty(i, now.Add(idle))
		} else {
			cr.sel.markBusy(i)
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
	cr.mu.Lock()
	if len(jobs) == 0 {
		cr.idle = min(cr.idle*2, w.cfg.IdleMax)
	} else {
		cr.idle = w.cfg.IdleMin
	}
	cr.mu.Unlock()
	return jobs, err
}

// jobContext carries the ids every log line about the job needs.
func jobContext(ctx context.Context, job core.Job) context.Context {
	return obs.WithCorrelationID(obs.WithNotificationID(ctx, job.ID), job.CorrelationID)
}

// deliver makes one provider call for a claimed job and writes the result.
// The call ends leaseMargin before the lease does (or at the provider
// timeout, whichever is first); with less than minSendTime of lease left it is
// not made and the job is released.
func (w *Worker) deliver(ctx context.Context, job core.Job, br *breaker.Breaker, tk breaker.Ticket) {
	left := job.LeaseUntil.Sub(w.deps.Now()) - leaseMargin
	if left < minSendTime {
		br.Cancel(tk)
		slog.WarnContext(ctx, "lease too close to its end to send; released", "channel", job.Channel,
			"lease_left_ms", (left + leaseMargin).Milliseconds())
		w.release(ctx, job, "lease too short")
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
	if err != nil && ctx.Err() != nil {
		// Shutdown aborted the call: not an attempt; give the job back.
		br.Cancel(tk)
		span.SetStatus(codes.Error, "aborted by shutdown")
		w.release(ctx, job, "shutdown aborted the call")
		return
	}
	if err != nil && cut {
		// No answer within the lease: a timeout, recorded now, while the
		// claim still holds.
		err = fmt.Errorf("%w: %w (%s before its end)", provider.ErrTimeout, errLeaseCut, leaseMargin)
	}
	v := provider.Classify(res.Status, err, res.Header)
	br.Done(tk, !v.ProviderFault)
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
	w.publish(ctx, job, status, attempts, errText)
}

// release returns an unsent job to pending, due now, without an attempt.
func (w *Worker) release(ctx context.Context, job core.Job, reason string) {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()
	if err := w.deps.Queue.Release(wctx, job, time.Now()); err != nil {
		slog.WarnContext(ctx, "release failed; the lease will expire", "reason", reason, "err", err)
		return
	}
	slog.DebugContext(ctx, "released unsent notification", "channel", job.Channel, "reason", reason)
	w.publish(ctx, job, core.StatusPending, job.Attempts, "")
}

func (w *Worker) publish(ctx context.Context, job core.Job, st core.Status, attempts int, errText string) {
	if w.deps.Publisher == nil {
		return
	}
	err := w.deps.Publisher.Publish(ctx, core.Event{
		NotificationID: job.ID, BatchID: job.BatchID, Status: st, Attempts: attempts,
		Error: errText, At: time.Now().UTC(), CorrelationID: job.CorrelationID,
	})
	if err != nil {
		slog.WarnContext(ctx, "publish status change", "status", st, "err", err)
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
		for _, job := range jobs {
			w.publish(jobContext(ctx, job), job, core.StatusPending, job.Attempts, "")
		}
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
