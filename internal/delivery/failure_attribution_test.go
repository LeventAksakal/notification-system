package delivery

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

func TestAResultMovesItsNotificationByOutcomeAndProviderHealth(t *testing.T) {
	cfg := Config{MaxAttempts: 3, RetryBase: time.Second, RetryCap: time.Minute, HealthCooldown: 2 * time.Second}
	top := func(n int64) int64 { return n } // the largest wait the backoff can draw
	transient := provider.Verdict{Outcome: notification.OutcomeTransient, ProviderFault: true}
	cases := []struct {
		name       string
		v          provider.Verdict
		healthy    bool
		healthWait time.Duration
		spent      int
		want       next
	}{
		{"accepted is sent and spends nothing", provider.Verdict{Outcome: notification.OutcomeAccepted}, true, 0, 1,
			next{status: notification.StatusSent, budgetSpent: 1}},
		{"permanent fails and counts, whatever the health", provider.Verdict{Outcome: notification.OutcomePermanent}, false, 0, 0,
			next{status: notification.StatusFailed, kind: notification.FailurePermanent, counted: true, budgetSpent: 1}},
		{"transient while healthy counts and backs off on the budget spent", transient, true, 0, 1,
			next{status: notification.StatusPending, counted: true, budgetSpent: 2, retryIn: 4 * time.Second}},
		{"transient while healthy that reaches the budget is exhausted", transient, true, 0, 2,
			next{status: notification.StatusFailed, kind: notification.FailureExhausted, counted: true, budgetSpent: 3}},
		{"transient during an outage spends nothing and waits for the next probe, spread over a cool-down", transient, false, 5 * time.Second, 2,
			next{status: notification.StatusPending, budgetSpent: 2, retryIn: 7 * time.Second}},
		{"transient during an outage obeys a longer Retry-After", provider.Verdict{Outcome: notification.OutcomeTransient, ProviderFault: true, RetryAfter: 9 * time.Second},
			false, 5 * time.Second, 0, next{status: notification.StatusPending, retryIn: 11 * time.Second}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := decide(c.v, c.healthy, c.healthWait, c.spent, cfg, top); got != c.want {
				t.Errorf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

// An outage longer than every message's retry span spends at most
// Failures-1 budget units across the backlog, dead-letters nothing, probes
// about once per cool-down, and everything is sent after recovery.
func TestAnOutageSpendsNoBudgetBeyondItsDetection(t *testing.T) {
	const failures, rows = 3, 30
	const cooldown = 50 * time.Millisecond
	q := newMemQueue()
	q.add(rows, notification.ChannelSMS, notification.PriorityNormal)
	var down atomic.Bool
	down.Store(true)
	var mu sync.Mutex
	var during int // calls made while down
	send := func(ctx context.Context, m provider.Message) (provider.Response, error) {
		if down.Load() {
			mu.Lock()
			during++
			mu.Unlock()
			return provider.Response{Status: 503}, nil
		}
		return accept(ctx, m)
	}
	cfg := testConfig() // MaxAttempts 3, retry span of a few ms: far shorter than the outage
	st := health.NewMemory(health.Config{Failures: failures, Cooldown: cooldown}, time.Now)
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{send}, Health: st}))
	outage := 20 * cooldown // 1 s
	start := time.Now()
	for time.Since(start) < outage {
		if n := q.count(notification.StatusFailed); n > 0 {
			t.Fatalf("%d notifications dead-lettered during the outage", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	calls := during
	mu.Unlock()
	down.Store(false)
	waitFor(t, "every notification sent after recovery", func() bool { return q.count(notification.StatusSent) == rows })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	spent := 0
	for _, id := range q.order {
		spent += q.row(id).job.BudgetSpent
	}
	if spent > failures-1 {
		t.Errorf("budget spent across the backlog: %d, want at most %d (the failures before the outage was declared)", spent, failures-1)
	}
	// Detection (failures), the slots of the grant in flight when it was
	// declared (Horizon/interval + 1 = 5 with the test limiter), then one probe
	// per cool-down: 20, plus one at the edge.
	if limit := failures + 5 + int(outage/cooldown) + 1; calls > limit {
		t.Errorf("%d calls during the outage, want at most %d (about one probe per cool-down)", calls, limit)
	}
}

// openCounter counts outage declarations on a State.
type openCounter struct {
	health.State
	opened atomic.Int64
}

func (s *openCounter) Report(ctx context.Context, ch notification.Channel, gen int64, probe, fault bool, ra time.Duration) (health.Reply, error) {
	r, err := s.State.Report(ctx, ch, gen, probe, fault, ra)
	if r.Transition == health.Opened {
		s.opened.Add(1)
	}
	return r, err
}

// Excused rows come due spread over a cool-down after the next probe, not
// together at it (verifier finding 3, 15 Nov 2026).
func TestExcusedFailuresComeDueSpreadOverACooldown(t *testing.T) {
	cfg := Config{MaxAttempts: 3, RetryBase: time.Second, RetryCap: time.Minute, HealthCooldown: 2 * time.Second}
	transient := provider.Verdict{Outcome: notification.OutcomeTransient, ProviderFault: true}
	for _, c := range []struct {
		name string
		rnd  func(int64) int64
		want time.Duration
	}{
		{"the smallest draw waits for the probe", func(int64) int64 { return 0 }, 5 * time.Second},
		{"a middle draw lands inside the cool-down", func(n int64) int64 { return n / 2 }, 6 * time.Second},
		{"the largest draw waits a whole cool-down more", func(n int64) int64 { return n - 1 }, 7*time.Second - time.Nanosecond},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := decide(transient, false, 5*time.Second, 0, cfg, c.rnd).retryIn; got != c.want {
				t.Errorf("retryIn %s, want %s", got, c.want)
			}
		})
	}
}

// Message-specific failures the provider reports as its own fault (a 503 for
// one recipient) can trip the outage rule among sparse traffic, round after
// round. They must still reach their budget: each declaration takes
// Failures-1 counted failures, so declarations stay within
// poison*MaxAttempts/(Failures-1) and every poison row dead-letters.
func TestMessageSpecificFailuresStillExhaustTheirBudget(t *testing.T) {
	const failures, poison, maxAttempts = 3, 8, 4
	q := newMemQueue()
	q.add(poison, notification.ChannelSMS, notification.PriorityNormal) // ids sms-normal-0..7
	bad := func(id string) bool { return len(id) == len("sms-normal-0") && id < "sms-normal-8" }
	send := func(ctx context.Context, m provider.Message) (provider.Response, error) {
		if bad(m.Key) {
			return provider.Response{Status: 503}, nil
		}
		return accept(ctx, m)
	}
	cfg := testConfig()
	cfg.MaxAttempts, cfg.RetryBase, cfg.RetryCap, cfg.HealthCooldown = maxAttempts, 20*time.Millisecond, 100*time.Millisecond, 50*time.Millisecond
	st := &openCounter{State: health.NewMemory(health.Config{Failures: failures, Cooldown: 50 * time.Millisecond}, time.Now)}
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{send}, Health: st}))
	poisonFailed := func() bool {
		for i := range poison {
			if q.row(fmt.Sprintf("sms-normal-%d", i)).status != notification.StatusFailed {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(10 * time.Second)
	for !poisonFailed() {
		if time.Now().After(deadline) {
			t.Fatal("poison rows still pending after 10s: excused failures kept them from their budget")
		}
		q.add(1, notification.ChannelSMS, notification.PriorityNormal) // sparse good traffic
		time.Sleep(100 * time.Millisecond)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if n, limit := st.opened.Load(), int64(poison*maxAttempts/(failures-1)); n > limit {
		t.Errorf("%d outages declared, want at most %d (each needs %d counted failures)", n, limit, failures-1)
	}
}
