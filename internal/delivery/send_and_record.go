package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/delivery/leasebudget"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

// storeTimeout bounds one result write; it is not cut short by shutdown, so a
// finished send is recorded when the database answers in time. Source: at
// least the slowest result write measured under overload (3.3 s, decision
// 0018, "As built"), and at most the 5 s compose's 25 s stop_grace_period
// sets aside for final writes after APP_SHUTDOWN_TIMEOUT (10 s), beside the
// telemetry flush (5 s) and 5 s to spare. A write that misses it is retried by the lease: the row is reaped
// and sent again under the same idempotency key.
const storeTimeout = 5 * time.Second

// leaseMargin is how long before its lease ends a provider call is cut, so
// the attempt is recorded while the claim still holds (leasebudget.Margin,
// where its source is). config.Load refuses a lease too short to hold a whole
// provider call, its slot wait and this margin.
const leaseMargin = leasebudget.Margin

// minSendTime is the least call time worth starting; with less lease left
// the job is released instead. Source: a fifth of the assumed provider p99
// (500 ms, decision 0018); a call given less is more likely cut than
// answered. It is reached only after a pause, since config.Load keeps the
// lease above a whole provider call.
const minSendTime = 100 * time.Millisecond

// errLeaseCut is the cause of a call ended by the lease, not the provider timeout.
var errLeaseCut = errors.New("call cut before the lease ends")

// deliver makes one provider call for a claimed job and writes the result.
// The call ends leaseMargin before the lease does (or at the provider
// timeout, whichever is first); with less than minSendTime of lease left it is
// not made and the job is released. leaveBy is the slot's late moment: a
// request not yet written by then is not written at all (provider.WithLeaveBy)
// and the slot is given up. callDone is called once the provider call is over
// (or will not be made). The result is published after announced is closed.
func (w *Worker) deliver(ctx context.Context, job notification.Job, announced <-chan struct{}, gate *health.Gate, tk health.Ticket, leaveBy time.Time, callDone func()) {
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
	// sendAt's own lateness check is not the last moment: the clock and
	// gate checks, scheduling and the transport's connection handoff come
	// after it, and a request delayed there would leave late and bunch with
	// on-time ones (busiest second 106: TestDeliveryR8RateLimit, 20 runs
	// pinned to two cores, 16 Nov 2026). The provider client refuses a call
	// already past leaveBy, and a first write after leaveBy plus the time
	// the call waited for its connection (provider connUse.got).
	callCtx = provider.WithLeaveBy(callCtx, leaveBy)
	res, err := w.deps.Sender.Send(callCtx, provider.Message{
		Channel: job.Channel, To: job.To, Content: job.Content, Key: job.ID, CorrelationID: job.CorrelationID,
	})
	cut := errors.Is(context.Cause(callCtx), errLeaseCut)
	cancelCall()
	elapsed := time.Since(start)
	callDone()
	if errors.Is(err, provider.ErrNotSent) {
		// The call obtained a connection and wrote nothing to it (a
		// descheduled worker's timeout, shutdown before the write): the
		// provider cannot have seen it, so it is no attempt and says nothing
		// about the provider. Given back like a missed slot (decision 0005),
		// due at once. A failed dial is not this: it is a recorded provider
		// fault, or a refusing provider would loop here and never open its
		// outage.
		//
		// Two releases here do come from a provider in trouble, and spend no
		// budget and tell health nothing; neither loops:
		//   - an idle connection the dying provider already closed: the
		//     transport fails the call before its write starts ("server
		//     closed idle connection"). A write that started and sent no
		//     byte (net/http's nothingWrittenError) still counts as started,
		//     so it is recorded, not released. Each
		//     stale connection fails one call and is dropped, so this is
		//     bounded by the idle pool; the next call dials, and a refused
		//     dial is recorded.
		//   - a dial that completes right at the call's deadline: the call
		//     gets the connection and times out before its write. This needs
		//     the dial to end inside the last moment of the timeout, so it
		//     cannot repeat on every call; a dial slower than the timeout
		//     fails as a dial and is recorded.
		w.cancelGate(ctx, gate, tk)
		if errors.Is(err, provider.ErrTooLate) {
			// The request would have left after its slot's late moment: a
			// missed slot, as in sendAt, given up, never sent late (decision
			// 0009). Under load this is routine, not a warning.
			w.m.gaveUp(ctx, job.Channel, giveUpSlotMissed, 1)
			w.release(ctx, job, announced, "slot missed at the write")
			return
		}
		slog.WarnContext(ctx, "provider call never left; released", "channel", job.Channel,
			"after_ms", elapsed.Milliseconds(), "err", err)
		w.m.gaveUp(ctx, job.Channel, giveUpCallNeverLeft, 1)
		w.release(ctx, job, announced, "the provider call never left")
		return
	}
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
