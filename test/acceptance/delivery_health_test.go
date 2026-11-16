//go:build integration

package acceptance

// Provider health shared in Valkey (decision 0026): an outage is declared
// fleet-wide after APP_BREAKER_FAILURES consecutive provider faults; the
// declaring failure and every later one until recovery spend no retry budget;
// one probe per cool-down fleet-wide. Helpers here start with "ph".

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// phFirstAccepted returns the arrival of the earliest request in rs that the
// double answered 202, and whether there is one.
func phFirstAccepted(rs []fakeprovider.Request) (time.Time, bool) {
	for _, r := range rs { // sorted by arrival (dlRequests)
		if r.Status == http.StatusAccepted {
			return r.ReceivedAt, true
		}
	}
	return time.Time{}, false
}

// phBudgetSpent is SUM(budget_spent) over the notifications matching where.
func (s *stack) phBudgetSpent(t *testing.T, where string, args ...any) int {
	t.Helper()
	// where is always a constant condition written in the test source; values go through args.
	return s.count(t, "SELECT CAST(COALESCE(SUM(budget_spent), 0) AS SIGNED) FROM notifications WHERE "+where, args...) //nolint:gosec // G202: where is a test-source constant, values are bound via args
}

// phIdlePoll is the worker's IdleMax, 250 ms: how often a channel whose health
// check was refused asks again (internal/delivery/new_worker.go; a code
// default, not an APP_ dial, so the test cannot read it from the env).
const phIdlePoll = 250 * time.Millisecond

// phEnvDuration parses the duration dial key of the stack's env map.
func phEnvDuration(t *testing.T, env map[string]string, key string) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(env[key])
	if err != nil {
		t.Fatalf("%s=%q: %v", key, env[key], err)
	}
	return d
}

// phReapedUnknown counts the delivery_attempts with outcome 'unknown' of the
// notifications matching where: one budget unit each, charged by the lease
// reaper (UnknownOnReap, decision 0026), not by a provider fault.
func (s *stack) phReapedUnknown(t *testing.T, where string, args ...any) int {
	t.Helper()
	// where is always a constant condition written in the test source; values go through args.
	return s.count(t, "SELECT COUNT(*) FROM delivery_attempts WHERE outcome = 'unknown' AND notification_id IN (SELECT id FROM notifications WHERE "+where+")", args...) //nolint:gosec // G202: where is a test-source constant, values are bound via args
}

// phAssertOutageBudget asserts that the budget spent by provider faults on the
// notifications matching where is at most breakerFailures-1 (K-1).
func (s *stack) phAssertOutageBudget(t *testing.T, breakerFailures int, where string, args ...any) {
	t.Helper()
	spent := s.phBudgetSpent(t, where, args...)
	unknown := s.phReapedUnknown(t, where, args...)
	t.Logf("budget: SUM(budget_spent) %d, reaped unknown calls %d, spent by provider faults %d", spent, unknown, spent-unknown)
	// Reaped unknowns are excluded: the lease reaper charges them (UnknownOnReap, decision 0026) when a stalled worker loses its lease, not the outage rule.
	if faults := spent - unknown; faults > breakerFailures-1 {
		t.Errorf("budget spent by provider faults %d (SUM %d minus %d reaped unknown calls, charged by the lease reaper, decision 0026), want at most %d (K-1: only the faults before the outage was declared count)",
			faults, spent, unknown, breakerFailures-1)
	}
}

// phStream creates one notification from item every interval until stop is
// closed, from its own goroutine; wait returns their ids and the first error.
// It stops when the test ends at the latest.
type phStream struct {
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
	mu   sync.Mutex
	ids  []uuid.UUID
	err  error
}

func (st *phStream) halt() {
	st.once.Do(func() { close(st.stop) })
	st.wg.Wait()
}

func (s *stack) phStartStream(t *testing.T, interval time.Duration, item func(i int) apiclient.NotificationCreate) *phStream {
	st := &phStream{stop: make(chan struct{})}
	t.Cleanup(st.halt)
	st.wg.Go(func() {
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for i := 0; ; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			r, err := s.API.CreateNotificationWithResponse(ctx, nil, item(i))
			cancel()
			st.mu.Lock()
			switch {
			case err != nil && st.err == nil:
				st.err = err
			case err == nil && (r.StatusCode() != http.StatusCreated || r.JSON201 == nil) && st.err == nil:
				st.err = fmt.Errorf("create: %s", show(r))
			case err == nil && r.JSON201 != nil:
				st.ids = append(st.ids, r.JSON201.Id)
			}
			st.mu.Unlock()
			select {
			case <-st.stop:
				return
			case <-tick.C:
			}
		}
	})
	return st
}

func (st *phStream) wait(t *testing.T) []uuid.UUID {
	t.Helper()
	st.halt()
	if st.err != nil {
		t.Fatalf("stream of creates: %v", st.err)
	}
	return st.ids
}

// B1: Bonus: Failure Handling ("Provider outage: the channel pauses, no
// attempts are burned, sending resumes"; decision 0026: one probe per
// cool-down fleet-wide, outages spend no retry budget).
// Dials: APP_BREAKER_FAILURES=5, APP_BREAKER_COOLDOWN=1s, APP_RETRY_BASE=100ms,
// APP_RETRY_CAP=1s, APP_MAX_ATTEMPTS=5; 3 workers. Queued: 300 sms, 600 email,
// 600 push, plus one sms every 500 ms during a 12 s sms outage (503): 5 retry
// spans of 0.2 + 0.4 + 0.8 + 1.0 = 2.4 s, and 12 cool-downs.
// The 1.5 s settle before counting probes is 53 ms detection + 200 ms horizon
// + 200 ms send alignment + 500 ms timeout + 250 ms idle poll, rounded up.
func TestDeliveryB1SharedHealthLongOutage(t *testing.T) {
	const perSMS, perFast = 300, 600
	const outage = 12 * time.Second
	const cooldown = time.Second
	const settle = 1500 * time.Millisecond
	const breakerFailures = 5
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(map[string]string{
		"APP_BREAKER_FAILURES": fmt.Sprint(breakerFailures),
		"APP_BREAKER_COOLDOWN": "1s",
		"APP_RETRY_BASE":       "100ms",
		"APP_RETRY_CAP":        "1s",
		"APP_MAX_ATTEMPTS":     "5",
	})})
	s.dlCreateMany(t, perSMS, func(i int) apiclient.NotificationCreate { return msg("sms", fmt.Sprintf("B1 long outage sms %d", i)) })
	fast := []string{"email", "push"}
	s.dlCreateMany(t, 2*perFast, func(i int) apiclient.NotificationCreate {
		return msg(fast[i%2], fmt.Sprintf("B1 long outage fast %d", i))
	})
	s.dlFaults(t, fakeprovider.Fault{Channel: "sms", Mode: fakeprovider.ModeStatus, Status: http.StatusServiceUnavailable})
	for range 3 {
		s.StartWorker()
	}

	start := s.dgFirstArrival(t, "sms")
	end := start.Add(outage)
	stream := s.phStartStream(t, 500*time.Millisecond, func(i int) apiclient.NotificationCreate {
		return msg("sms", fmt.Sprintf("B1 long outage trickle %d", i))
	})
	consistently(t, time.Until(end), func() bool {
		return s.count(t, "SELECT COUNT(*) FROM notifications WHERE status = 'failed'") == 0
	}, "no notification is failed during the sms outage")
	trickle := stream.wait(t)
	dlWaitUntil(end)
	s.dlFaults(t)
	cleared := time.Now()

	smsReqs := s.dlRequests(fakeprovider.Filter{Channel: "sms"})
	window := cleared.Sub(start.Add(settle))
	bound := int(window/cooldown) + 1
	probes := dlReceivedIn(smsReqs, start.Add(settle), cleared)
	t.Logf("sms requests during the outage: %d in the first %s, %d in the %s after it (bound %d: one probe per %s fleet-wide); %d trickle sms created",
		dlReceivedIn(smsReqs, start, start.Add(settle)), settle, probes, window.Round(time.Millisecond), bound, cooldown, len(trickle))
	if probes > bound {
		t.Errorf("sms requests after the %s settle: %d, want at most %d (one probe per cool-down fleet-wide)", settle, probes, bound)
	}

	minFastRate := dlRateFloor(t, "B1 fast channels during the long sms outage", dlPacedRate-7) // 88/s
	for _, ch := range fast {
		s.dlAllSent(t, 30*time.Second, perFast, "channel = ?", ch)
		n, span, rate := dlRate(s.dlRequests(fakeprovider.Filter{Channel: ch}))
		t.Logf("%s during the sms outage: %d requests over %s = %.2f/s", ch, n, span.Round(time.Millisecond), rate)
		if rate < minFastRate {
			t.Errorf("%s drained at %.2f/s during the sms outage, want at least %.2f (other channels keep their rate)", ch, rate, minFastRate)
		}
	}

	s.dlAllSent(t, 60*time.Second, perSMS+len(trickle), "channel = 'sms'")
	first, ok := phFirstAccepted(s.dlRequests(fakeprovider.Filter{Channel: "sms"}))
	switch {
	case !ok:
		t.Errorf("no sms request was accepted after recovery")
	case first.Sub(cleared) > 2*time.Second:
		t.Errorf("first accepted sms %s after recovery, want at most 2s (1 s cool-down plus 1 s slack)", first.Sub(cleared).Round(time.Millisecond))
	default:
		t.Logf("first accepted sms %s after recovery", first.Sub(cleared).Round(time.Millisecond))
	}
	s.phAssertOutageBudget(t, breakerFailures, "1=1")
	st := s.Provider.Stats()
	for _, ch := range []string{"sms", "email", "push"} {
		if m := st.Channels[ch].MaxRequestsPerSecond; m > dlRateLimit {
			t.Errorf("%s: %d requests in one second at the provider, limit %d", ch, m, dlRateLimit)
		}
	}
	t.Logf("sms busiest second %d", st.Channels["sms"].MaxRequestsPerSecond)
}

// R12: Delivery & Retry Logic ("exhausted ones are kept and can be
// replayed"): a failure specific to one message (its recipient), while the
// provider accepts everything else, is not an outage; it spends the message's
// budget and is dead-lettered as exhausted (decision 0026). APP_MAX_ATTEMPTS
// is left unset so the service default applies: 10 (decision 0026, the
// smallest budget whose waits reach APP_RETRY_CAP with the default base and cap).
// Dials: dlFastEnv (base 100 ms, cap 1 s, timeout 500 ms),
// APP_BREAKER_FAILURES=5; one sms to a recipient that always gets 503; a
// stream of sms to another recipient every 50 ms; 3 workers.
func TestDeliveryR12MessageSpecificFailureDeadLetters(t *testing.T) {
	const defaultMaxAttempts = 10
	const badTo = "+905550000001"
	s := startStack(t, stackOptions{Workers: 3, Env: dlFastEnv(map[string]string{
		"APP_BREAKER_FAILURES": "5",
		"APP_MAX_ATTEMPTS":     "", // empty: the service default
	})})
	s.dlFaults(t, fakeprovider.Fault{To: badTo, Mode: fakeprovider.ModeStatus, Status: http.StatusServiceUnavailable})

	stream := s.phStartStream(t, 50*time.Millisecond, func(i int) apiclient.NotificationCreate {
		return msg("sms", fmt.Sprintf("R12 message-specific stream %d", i))
	})
	dlEventually(t, 15*time.Second, "the stream reaches the provider", func() (bool, string) {
		n := s.Provider.Stats().Channels["sms"].Received
		return n > 0, fmt.Sprintf("%d sms received", n)
	})
	body := msg("sms", "R12 message-specific failure")
	body.To = badTo
	bad := s.create(t, body)

	dlEventually(t, 60*time.Second, "the message whose recipient always fails is dead-lettered", func() (bool, string) {
		st := s.storedStatus(t, bad.Id)
		return st == "failed", "stored status " + st
	})
	others := stream.wait(t)

	got := s.get(t, bad.Id)
	if got.Status != "failed" || deref(got.FailureKind) != "exhausted" {
		t.Errorf("dead letter: want failed/exhausted, got %s/%s", got.Status, deref(got.FailureKind))
	}
	if spent := s.phBudgetSpent(t, "id = ?", bad.Id.String()); spent != defaultMaxAttempts {
		t.Errorf("budget_spent: want %d (the default APP_MAX_ATTEMPTS), got %d", defaultMaxAttempts, spent)
	}
	t.Logf("dead letter after %d attempts; %d stream notifications", got.Attempts, len(others))
	s.dlAllSent(t, 30*time.Second, len(others), "recipient <> ?", badTo)
}

// B1: Bonus: Failure Handling ("Provider outage: ... no attempts are burned,
// sending resumes"): a lone message outlives a 12 s outage even with
// APP_MAX_ATTEMPTS equal to APP_BREAKER_FAILURES (the edge case of decision
// 0026: its K-th fault declares the outage and is excused).
// Dials: dgB1Env with APP_MAX_ATTEMPTS=5 (= APP_BREAKER_FAILURES=5); 1 worker.
//
// Bound on the first accepted send after recovery, from the dials:
//   - 2 x APP_BREAKER_COOLDOWN (2 x 1 s = 2 s): an excused fault is due again
//     at max(health wait, Retry-After) + a uniform spread over one cool-down
//     (internal/delivery/failure_attribution.go, decide); the health wait is
//     at most one cool-down and the double sends no Retry-After, so the row
//     can be due up to 2 cool-downs after the last fault before clearing.
//   - + the idle poll, 250 ms (phIdlePoll): a refused health check is asked
//     again at least that often.
//   - + the send alignment ceiling, APP_LEASE_DURATION / 8 (2 s / 8 = 250 ms):
//     the probe's slot is reserved at most that far past its claim.
//   - + a 500 ms scheduling margin: the fault's result, the claim and the
//     Valkey round trips on a loaded machine.
//
// = 2 s + 250 ms + 250 ms + 500 ms = 3 s.
func TestDeliveryB1LoneMessageOutlivesOutage(t *testing.T) {
	const outage = 12 * time.Second
	const breakerFailures = 5
	const margin = 500 * time.Millisecond
	env := dgB1Env(breakerFailures)
	cooldown := phEnvDuration(t, env, "APP_BREAKER_COOLDOWN")
	alignCeiling := phEnvDuration(t, env, "APP_LEASE_DURATION") / 8
	sendBound := 2*cooldown + phIdlePoll + alignCeiling + margin
	s := startStack(t, stackOptions{Workers: 1, Env: env})
	s.dlFaults(t, fakeprovider.Fault{Channel: "sms", Mode: fakeprovider.ModeStatus, Status: http.StatusServiceUnavailable})
	n := s.create(t, msg("sms", "B1 lone message"))
	start := s.dgFirstArrival(t, "sms")
	end := start.Add(outage)
	consistently(t, time.Until(end), func() bool { return s.storedStatus(t, n.Id) != "failed" },
		"the lone message is never failed during the outage")
	dlWaitUntil(end)
	s.dlFaults(t)
	cleared := time.Now()

	s.dlAllSent(t, 30*time.Second, 1, "id = ?", n.Id.String())
	rs := s.dlRequests(fakeprovider.Filter{Key: n.Id.String()})
	first, ok := phFirstAccepted(rs)
	switch {
	case !ok:
		t.Errorf("no request of the lone message was accepted")
	case first.Sub(cleared) > sendBound:
		t.Errorf("sent %s after recovery, want at most %s (2 x cool-down %s + idle poll %s + alignment ceiling %s + margin %s)",
			first.Sub(cleared).Round(time.Millisecond), sendBound, cooldown, phIdlePoll, alignCeiling, margin)
	default:
		t.Logf("sent %s after recovery after %d requests", first.Sub(cleared).Round(time.Millisecond), len(rs))
	}
	s.phAssertOutageBudget(t, breakerFailures, "id = ?", n.Id.String())
	got := s.get(t, n.Id)
	if rows := s.count(t, "SELECT COUNT(*) FROM delivery_attempts WHERE notification_id = ?", n.Id.String()); got.Attempts != rows {
		t.Errorf("attempts %d, want the number of delivery_attempts rows %d", got.Attempts, rows)
	}
}
