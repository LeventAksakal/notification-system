// Package worker is the delivery loop of `app worker` (rows R7, R8, R9, R12,
// B1, B2): per channel it asks the breaker, takes a rate-limit permit, claims
// one notification for that permit by weighted priority, sends it on a
// bounded pool, classifies the answer and writes the result.
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

// Sender makes one provider call (provider.Client).
type Sender interface {
	Send(ctx context.Context, m provider.Message) (provider.Response, error)
}

// Config is the worker's dials.
type Config struct {
	Concurrency     int           // APP_WORKER_CONCURRENCY
	MaxAttempts     int           // APP_MAX_ATTEMPTS
	RetryBase       time.Duration // APP_RETRY_BASE
	RetryCap        time.Duration // APP_RETRY_CAP
	Lease           time.Duration // APP_LEASE_DURATION
	ReapInterval    time.Duration // APP_REAP_INTERVAL
	ShutdownTimeout time.Duration // APP_SHUTDOWN_TIMEOUT
	Weights         [3]int        // APP_PRIORITY_WEIGHTS
	Channels        []core.Channel
	// IdleMin and IdleMax bound the idle poll of a channel with nothing due:
	// it starts at IdleMin and doubles up to IdleMax. Defaults 25ms and 250ms.
	IdleMin, IdleMax time.Duration
}

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
		cfg.IdleMin = 25 * time.Millisecond
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

// Run delivers until ctx is cancelled. Then it stops claiming, lets in-flight
// sends finish for up to ShutdownTimeout, releases what was claimed but not
// sent, and returns after every goroutine it started has ended.
func (w *Worker) Run(ctx context.Context) error {
	// Sends outlive ctx by up to the shutdown grace.
	sendCtx, cancelSends := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSends()

	slots := make(chan struct{}, w.cfg.Concurrency)
	var inflight sync.WaitGroup

	g, gctx := errgroup.WithContext(ctx)
	for _, ch := range w.cfg.Channels {
		br := w.deps.Breakers[ch]
		g.Go(func() error {
			w.channelLoop(gctx, sendCtx, ch, br, slots, &inflight)
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

func (w *Worker) channelLoop(ctx, sendCtx context.Context, ch core.Channel, br *breaker.Breaker, slots chan struct{}, inflight *sync.WaitGroup) {
	sel := newSelector(w.cfg.Weights)
	idle := w.cfg.IdleMin
	limiterDown := false
	for ctx.Err() == nil {
		now := time.Now()
		if at := sel.nextCheck(now); !at.IsZero() {
			// Every priority had nothing due when last asked: no permit, no query.
			sleep(ctx, at.Sub(now))
			continue
		}
		if ok, wait := br.Allow(); !ok {
			sleep(ctx, min(wait, w.cfg.IdleMax))
			continue
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			br.Cancel()
			return
		}
		free := func() { <-slots }

		granted, wait, err := w.deps.Limiter.Take(ctx, ch, 1)
		if err != nil {
			free()
			br.Cancel()
			if ctx.Err() != nil {
				return
			}
			if !limiterDown {
				slog.ErrorContext(ctx, "rate limiter unreachable; channel sends nothing", "channel", ch, "err", err)
				limiterDown = true
			}
			sleep(ctx, w.cfg.IdleMax)
			continue
		}
		if limiterDown {
			slog.InfoContext(ctx, "rate limiter reachable again", "channel", ch)
			limiterDown = false
		}
		if granted == 0 {
			free()
			br.Cancel()
			sleep(ctx, wait)
			continue
		}

		job, ok, err := w.claimOne(ctx, ch, sel, idle)
		if err != nil {
			free()
			br.Cancel()
			if ctx.Err() == nil {
				slog.ErrorContext(ctx, "claim failed", "channel", ch, "err", err)
				sleep(ctx, w.cfg.IdleMax)
			}
			continue
		}
		if !ok {
			free()
			br.Cancel()
			idle = min(idle*2, w.cfg.IdleMax)
			continue
		}
		idle = w.cfg.IdleMin
		jctx := jobContext(sendCtx, job)
		w.publish(jctx, job, core.StatusProcessing, job.Attempts, "")
		if ctx.Err() != nil {
			// Shutdown between claim and send: give it back unsent.
			w.release(jctx, job)
			br.Cancel()
			free()
			return
		}
		inflight.Add(1)
		go func() { // owned by Run through inflight
			defer inflight.Done()
			defer free()
			w.deliver(jctx, job, br)
		}()
	}
}

// claimOne claims one due notification of ch for one permit, by the
// selector's order. Priorities found empty are skipped for idle.
func (w *Worker) claimOne(ctx context.Context, ch core.Channel, sel *selector, idle time.Duration) (core.Job, bool, error) {
	now := time.Now()
	for _, p := range sel.order(now) {
		jobs, err := w.deps.Queue.Claim(ctx, ch, p, 1, w.cfg.Lease)
		if err != nil {
			return core.Job{}, false, err
		}
		if len(jobs) > 0 {
			sel.markBusy(p)
			return jobs[0], true, nil
		}
		sel.markEmpty(p, now.Add(idle))
	}
	return core.Job{}, false, nil
}

// jobContext carries the ids every log line about the job needs.
func jobContext(ctx context.Context, job core.Job) context.Context {
	return obs.WithCorrelationID(obs.WithNotificationID(ctx, job.ID), job.CorrelationID)
}

// deliver makes one provider call for a claimed job and writes the result.
func (w *Worker) deliver(ctx context.Context, job core.Job, br *breaker.Breaker) {
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
	res, err := w.deps.Sender.Send(ctx, provider.Message{
		Channel: job.Channel, To: job.To, Content: job.Content, Key: job.ID, CorrelationID: job.CorrelationID,
	})
	elapsed := time.Since(start)
	if err != nil && ctx.Err() != nil {
		// Shutdown aborted the call: not an attempt; give the job back.
		br.Cancel()
		span.SetStatus(codes.Error, "aborted by shutdown")
		w.release(ctx, job)
		return
	}
	v := provider.Classify(res.Status, err, res.Header)
	br.Done(!v.ProviderFault)
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
func (w *Worker) release(ctx context.Context, job core.Job) {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()
	if err := w.deps.Queue.Release(wctx, job, time.Now()); err != nil {
		slog.WarnContext(ctx, "release failed; the lease will expire", "err", err)
		return
	}
	slog.InfoContext(ctx, "released unsent notification", "channel", job.Channel)
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

func (w *Worker) reaper(ctx context.Context) {
	t := time.NewTicker(w.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := w.deps.Queue.ReapExpired(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			slog.ErrorContext(ctx, "reap expired leases", "err", err)
		case n > 0:
			slog.InfoContext(ctx, "returned expired leases to pending", "count", n)
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
	return fmt.Sprintf("concurrency=%d max_attempts=%d weights=%v", c.Concurrency, c.MaxAttempts, c.Weights)
}
