package delivery

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

// reportCounter is a health state that counts every report it is given.
type reportCounter struct {
	health.State
	reports atomic.Int64
}

func (s *reportCounter) Report(ctx context.Context, ch notification.Channel, gen int64, probe, fault bool, ra time.Duration) (health.Reply, error) {
	s.reports.Add(1)
	return s.State.Report(ctx, ch, gen, probe, fault, ra)
}

// afterLeaveBy holds a call until the slot's leave-by moment the worker put
// in its context has passed, as a descheduled worker would, then lets next
// make it. The wait is the scenario's set-up, not an assertion.
func afterLeaveBy(t *testing.T, next func(context.Context, provider.Message) (provider.Response, error)) func(context.Context, provider.Message) (provider.Response, error) {
	return func(ctx context.Context, m provider.Message) (provider.Response, error) {
		by, ok := provider.LeaveBy(ctx)
		if !ok {
			t.Error("the provider call carries no leave-by moment")
			return next(ctx, m)
		}
		for !time.Now().After(by) {
			select {
			case <-time.After(time.Until(by) + time.Microsecond):
			case <-ctx.Done():
				return provider.Response{}, ctx.Err()
			}
		}
		return next(ctx, m)
	}
}

// A request that would leave after its slot's late moment is not written
// (decision 0009: given up, never sent late), even when it passed sendAt's
// own lateness check: it is released, spends no budget, leaves no attempt,
// tells provider health nothing and counts as a missed slot, not as a call
// that never left.
func TestALateRequestIsGivenUpAsAMissedSlot(t *testing.T) {
	// 100 ms: far above the microseconds between sendAt's own check and the
	// call, so a loaded -race run still reaches the provider call; the call
	// is then held past it (afterLeaveBy).
	const tolerance = 100 * time.Millisecond
	sms := attribute.String("channel", "sms")
	cases := []struct {
		name string
		// sender returns the worker's sender and how many requests reached
		// the provider.
		sender func(t *testing.T) (Sender, func() int64)
	}{
		{"the real client refuses the late call", func(t *testing.T) (Sender, func() int64) {
			var got atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				got.Add(1)
				w.WriteHeader(http.StatusAccepted)
			}))
			t.Cleanup(srv.Close)
			// 1 s: far above a local call, never reached; 1 connection: one
			// call. The late call is refused before it takes a connection
			// (the write-time refusal is the provider package's to test), so
			// none outlives the test (goleak).
			c := provider.New(map[notification.Channel]string{notification.ChannelSMS: srv.URL}, time.Second, 1)
			return scriptSender{afterLeaveBy(t, c.Send)}, got.Load
		}},
		{"a client answering ErrTooLate", func(t *testing.T) (Sender, func() int64) {
			late := func(context.Context, provider.Message) (provider.Response, error) {
				return provider.Response{}, fmt.Errorf("%w: %w", provider.ErrNotSent, provider.ErrTooLate)
			}
			return scriptSender{afterLeaveBy(t, late)}, func() int64 { return 0 }
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() { _ = mp.Shutdown(context.Background()) }()
			q := newMemQueue()
			q.add(1, notification.ChannelSMS, notification.PriorityHigh)
			id := q.order[0]
			// A minute's lease: far above the test's duration, never reached.
			jobs, _ := q.Claim(t.Context(), notification.ChannelSMS, notification.PriorityHigh, 1, time.Minute)
			inner, received := tc.sender(t)
			snd := &countingSender{fn: inner.Send}
			// Failures 1: a single reported fault would open the outage; a
			// minute's cool-down: far above the test's duration, so it stays.
			st := &reportCounter{State: health.NewMemory(health.Config{Failures: 1, Cooldown: time.Minute}, time.Now)}
			w := New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: snd, Health: st, Meter: mp})
			cr := w.newChannelRun(notification.ChannelSMS, testPlaces(1, 0))
			// 1 s: the gate's wait bound, never reached with the outage closed.
			tk, _, _, _ := cr.gate.Allow(t.Context(), time.Second)
			// A 1 ms round trip under a 5 ms bound: a certain clock reading,
			// as in TestWorkerDeliveryMetrics; its generation is the slot's.
			now := time.Now()
			_, _, gen, _ := w.clock.observe(now, now.Add(time.Millisecond), now, 5*time.Millisecond)

			w.sendAt(t.Context(), t.Context(), cr, jobs[0], nil, time.Now(), tk, tolerance, gen)

			if n := snd.calls.Load(); n != 1 {
				t.Fatalf("%d provider calls, want 1: the scenario needs the call made after sendAt's own check", n)
			}
			r := q.row(id)
			q.mu.Lock()
			released, results := q.released, len(q.results[id])
			q.mu.Unlock()
			if r.status != notification.StatusPending || released != 1 || results != 0 || r.job.Attempts != 0 || r.job.BudgetSpent != 0 {
				t.Errorf("status %s, %d released, %d attempts recorded, attempts %d, budget spent %d; want pending, 1, 0, 0, 0",
					r.status, released, results, r.job.Attempts, r.job.BudgetSpent)
			}
			if n := received(); n != 0 {
				t.Errorf("the provider received %d requests, want 0", n)
			}
			if n := st.reports.Load(); n != 0 {
				t.Errorf("%d health reports, want 0", n)
			}
			rm := collect(t, reader)
			if n := counted(rm, "notifier.slots.given_up", sms, attribute.String("reason", string(giveUpSlotMissed))); n != 1 {
				t.Errorf("given_up{reason=slot_missed} = %d, want 1", n)
			}
			if n := counted(rm, "notifier.slots.given_up", sms, attribute.String("reason", string(giveUpCallNeverLeft))); n != 0 {
				t.Errorf("given_up{reason=call_never_left} = %d, want 0", n)
			}
			if n := counted(rm, "notifier.delivery.attempts", sms); n != 0 {
				t.Errorf("attempts counted %d, want 0", n)
			}
		})
	}
}
