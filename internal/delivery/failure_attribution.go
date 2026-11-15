package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

// This file decides what a call's result does to its notification, and
// whether a failure counts against the notification's attempt budget
// (decision 0026): only while its provider is believed healthy.

// next is what a call's result does to its notification.
type next struct {
	status      notification.Status
	kind        notification.FailureKind // when status is failed
	counted     bool                     // the failure counts against the budget
	budgetSpent int                      // the budget spent after this result
	retryIn     time.Duration            // when status is pending
}

// decide maps a classified result to the notification's next state.
//
//   - Accepted: sent. Permanent: failed (permanent), counted.
//   - Transient while the provider is believed down after the failure was
//     recorded (healthy false): the failure belongs to the outage. Recorded,
//     not counted; due again once the next probe may go (healthWait) or the
//     provider's Retry-After has passed, whichever is later, plus a uniform
//     draw over one cool-down. The draw keeps rows excused together from
//     coming due together: five message-specific failures in a row would
//     declare the outage again by themselves. Its due time is newer than the
//     backlog's, so the next probe takes another row.
//   - Transient while healthy: counted; exhausted once the budget spent
//     reaches MaxAttempts, otherwise backed off on the budget spent (0007).
func decide(v provider.Verdict, healthy bool, healthWait time.Duration, budgetSpent int, cfg Config, rnd func(int64) int64) next {
	switch {
	case v.Outcome == notification.OutcomeAccepted:
		return next{status: notification.StatusSent, budgetSpent: budgetSpent}
	case v.Outcome == notification.OutcomePermanent:
		return next{status: notification.StatusFailed, kind: notification.FailurePermanent, counted: true, budgetSpent: budgetSpent + 1}
	case !healthy:
		spread := time.Duration(0)
		if cfg.HealthCooldown > 0 {
			spread = time.Duration(rnd(int64(cfg.HealthCooldown)))
		}
		return next{status: notification.StatusPending, budgetSpent: budgetSpent, retryIn: max(healthWait, v.RetryAfter) + spread}
	}
	spent := budgetSpent + 1
	if spent >= cfg.MaxAttempts {
		return next{status: notification.StatusFailed, kind: notification.FailureExhausted, counted: true, budgetSpent: spent}
	}
	return next{status: notification.StatusPending, counted: true, budgetSpent: spent,
		retryIn: Backoff(spent, cfg.RetryBase, cfg.RetryCap, v.RetryAfter, rnd)}
}

// reportHealth records the result in the shared health and returns whether
// the provider is believed healthy after it (a failure then counts) and, when
// it is not, how long until the next probe may go. If the shared state cannot
// be reached the result is attributed by its admission: a probe's failure
// belongs to the outage, a normal call's counts; the wait is then RetryBase,
// so an excused row is not retried at once.
func (w *Worker) reportHealth(ctx context.Context, ch notification.Channel, gate *health.Gate, tk health.Ticket, v provider.Verdict) (healthy bool, wait time.Duration) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout) // part of writing the result
	defer cancel()
	r, err := gate.Done(rctx, tk, v.ProviderFault, v.RetryAfter)
	if err != nil {
		slog.WarnContext(ctx, "provider health unreachable; result attributed by its admission",
			"channel", ch, "probe", tk.Probe(), "err", err)
		return !tk.Probe(), w.cfg.RetryBase
	}
	switch r.Transition {
	case health.Opened:
		slog.WarnContext(ctx, "provider outage declared", "channel", ch, "next_probe_in_ms", r.Wait.Milliseconds())
	case health.Recovered:
		slog.InfoContext(ctx, "provider recovered", "channel", ch)
	case health.None:
	}
	return r.Healthy(), r.Wait
}

// probeLease is how long a channel's probe is held: the longest a live probe
// can take from its admission to its report. Its slot is at most Horizon
// ahead, it leaves at slot + SendAlign and at most one interval late, the
// call ends at ProviderTimeout (or before the row's lease does), and the
// report's round trip fits in leaseMargin. A holder that dies delays the next
// probe by at most this.
func (w *Worker) probeLease(interval time.Duration) time.Duration {
	call := w.cfg.Lease
	if w.cfg.ProviderTimeout > 0 && (call <= 0 || w.cfg.ProviderTimeout < call) {
		call = w.cfg.ProviderTimeout
	}
	return w.cfg.Horizon + w.cfg.SendAlign + interval + call + leaseMargin
}

// cancel ends a ticket of cr's channel whose call will not be made.
func (w *Worker) cancel(ctx context.Context, cr *channelRun, tk health.Ticket) {
	w.cancelGate(ctx, cr.gate, tk)
}

// cancelGate ends a ticket whose call will not be made; only a probe needs a
// round trip. It waits at most one idle poll (IdleMax): past that the probe's
// lease frees it.
// It runs on shutdown too, so it does not inherit ctx's cancellation.
func (w *Worker) cancelGate(ctx context.Context, gate *health.Gate, tk health.Ticket) {
	if !tk.Probe() {
		return
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.IdleMax)
	defer cancel()
	if err := gate.Cancel(cctx, tk); err != nil {
		slog.DebugContext(ctx, "probe not cancelled; its lease frees it", "err", err)
	}
}
