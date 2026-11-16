//go:build integration

package acceptance

// Gap tests the delivery verifier named (.agents/reviews/2026-11-14-delivery-verifier.md,
// "Test gaps" and findings 1-6). Helpers here start with "dg".

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// dgAttemptRows returns delivery_attempts rows per notification id.
func (s *stack) dgAttemptRows(t *testing.T) map[string]int {
	t.Helper()
	return s.dgGroupCount(t, "SELECT notification_id, COUNT(*) FROM delivery_attempts GROUP BY notification_id")
}

// dgGroupCount runs a constant two-column (key, count) query.
func (s *stack) dgGroupCount(t *testing.T, query string) map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := s.DB.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]int{}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		out[k] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return out
}

// dgInFlight is how many requests the double has received but not yet
// answered or abandoned: it counts a request on arrival and records its
// history entry when it ends.
func (s *stack) dgInFlight() int {
	recv := 0
	for _, c := range s.Provider.Stats().Channels {
		recv += c.Received
	}
	return recv - len(s.Provider.Requests(fakeprovider.Filter{}))
}

// dgRequestsPerKey counts the double's recorded requests per Idempotency-Key.
func (s *stack) dgRequestsPerKey() map[string]int {
	out := map[string]int{}
	for _, r := range s.Provider.Requests(fakeprovider.Filter{}) {
		out[r.IdempotencyKey]++
	}
	return out
}

// dgAccounted waits (bounded) until no request is in flight at the double and,
// for every id, the delivery_attempts rows equal the provider requests the
// double recorded for that key: no provider call goes unrecorded and no row
// stands for a call that never happened. It returns both maps.
func (s *stack) dgAccounted(t *testing.T, ids []uuid.UUID, timeout time.Duration) (rows, reqs map[string]int) {
	t.Helper()
	dlEventually(t, timeout, "delivery_attempts rows == provider requests for every notification", func() (bool, string) {
		if n := s.dgInFlight(); n != 0 {
			return false, fmt.Sprintf("%d requests still in flight at the double", n)
		}
		reqs = s.dgRequestsPerKey()
		rows = s.dgAttemptRows(t)
		var bad []string
		for _, u := range ids {
			id := u.String()
			if rows[id] != reqs[id] {
				bad = append(bad, fmt.Sprintf("%s rows=%d requests=%d", id, rows[id], reqs[id]))
			}
		}
		return len(bad) == 0, fmt.Sprintf("%d of %d notifications differ, first: %v", len(bad), len(ids), bad[:min(5, len(bad))])
	})
	return rows, reqs
}

func dgSum(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

// R11: Idempotency support to prevent duplicate sends; decision 0018 and the
// contract's delivery rule "A request is sent only if its lease outlives the
// call". config.Load refuses a lease shorter than APP_PROVIDER_TIMEOUT + the
// longest slot wait + one slot interval + the 500 ms lease margin
// (config/cross_checks.go, decision 0018), so a whole call always fits the
// lease; the cap for a worker paused between claim and call stays
// unit-tested (internal/delivery/worker_safety_test.go). This test replaces
// TestGapDeliveryR11CallOutlivesLease (lease 2 s against a 5 s timeout, which
// the rule refuses).
// Dials: dlFastEnv with APP_PROVIDER_TIMEOUT=2s, APP_LEASE_DURATION=3500ms
// (floor 3.35 s), APP_MAX_ATTEMPTS=5, APP_BREAKER_FAILURES=10000 (no provider
// outage is ever declared); provider dedupe off, so a cut or reclaimed call would show as
// a second delivery; 2 workers. The first 6 sms calls are accepted on arrival
// and answered after 1.5 s, inside the timeout and the lease.
func TestGapDeliveryR11SlowCallEndsInsideLease(t *testing.T) {
	const n, held, maxAttempts = 20, 6, 5
	const hold = 1500 * time.Millisecond
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(map[string]string{
		"APP_PROVIDER_TIMEOUT": "2s",
		"APP_LEASE_DURATION":   "3500ms",
		"APP_MAX_ATTEMPTS":     fmt.Sprint(maxAttempts),
		"APP_BREAKER_FAILURES": "10000",
	}), Provider: []fakeprovider.Option{fakeprovider.WithDedupe(false)}})
	ids := s.dlCreateMany(t, n, func(i int) apiclient.NotificationCreate {
		return msg("sms", fmt.Sprintf("gap R11 slow call %d", i))
	})
	s.dlFaults(t, fakeprovider.Fault{Channel: "sms", Mode: fakeprovider.ModeAcceptThenHang, DelayMS: int(hold / time.Millisecond), Count: held})
	s.StartWorker()
	s.StartWorker()

	s.dlAllSent(t, 30*time.Second, n, "1=1")
	rows, reqs := s.dgAccounted(t, ids, 15*time.Second)
	if p := s.count(t, "SELECT COUNT(*) FROM notifications WHERE status = 'processing'"); p != 0 {
		t.Errorf("%d notifications still processing", p)
	}
	for _, id := range ids {
		if c := reqs[id.String()]; c != 1 {
			t.Errorf("%s: %d provider requests, want exactly 1 (a slow call inside the lease is neither cut nor reclaimed)", id, c)
		}
	}
	st := s.Provider.Stats()
	t.Logf("%d delivery_attempts rows, %d provider requests; maxRequestsPerKey %d, maxDeliveriesPerKey %d",
		dgSum(rows), dgSum(reqs), st.MaxRequestsPerKey, st.MaxDeliveriesPerKey)
	if st.MaxDeliveriesPerKey != 1 {
		t.Errorf("maxDeliveriesPerKey: want 1 with dedupe off, got %d", st.MaxDeliveriesPerKey)
	}
	// The hold happened: exactly the held calls took about 1.5 s.
	slow := 0
	for _, id := range ids {
		as := history(s.get(t, id))
		if len(as) == 1 && time.Duration(as[0].DurationMs)*time.Millisecond >= hold-100*time.Millisecond {
			slow++
		}
	}
	if slow != held {
		t.Errorf("%d notifications have one attempt of at least %s, want %d (the held calls)", slow, hold-100*time.Millisecond, held)
	}
}

// R11: Idempotency support to prevent duplicate sends; decision 0018: a
// lease too short for a whole provider call would cut calls (unknown
// outcomes, sent again), so the configuration is refused before anything is
// claimed. `app env check` names the rule, and `app worker` exits non-zero
// with the same message. Baseline: the same environment with an 8 s lease
// passes the check, so the refusal is about the lease.
func TestConfigRefusesALeaseShorterThanAProviderCall(t *testing.T) {
	const want = "APP_LEASE_DURATION 3s is below APP_PROVIDER_TIMEOUT 5s"
	run := func(t *testing.T, lease string, args ...string) (string, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, appBin, args...)
		cmd.Dir = repoRoot
		cmd.Env = childEnv(map[string]string{"APP_DATABASE_DSN": "x", "APP_LEASE_DURATION": lease, "APP_PROVIDER_TIMEOUT": "5s"})
		out, err := cmd.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("app %s did not exit within 30s:\n%s", strings.Join(args, " "), out)
		}
		return string(out), err
	}

	if out, err := run(t, "8s", "env", "check"); err != nil {
		t.Fatalf("baseline: app env check with an 8 s lease and a 5 s timeout: want exit 0, got %v:\n%s", err, out)
	}
	for _, args := range [][]string{{"env", "check"}, {"worker"}} {
		t.Run("app "+strings.Join(args, " "), func(t *testing.T) {
			out, err := run(t, "3s", args...)
			if err == nil {
				t.Errorf("want a non-zero exit, got 0:\n%s", out)
			}
			if n := strings.Count(out, want); n != 1 {
				t.Errorf("want one line naming %q, found %d:\n%s", want, n, out)
			}
		})
	}
}

// R12: Delivery & Retry Logic - attempts are accounted exactly (decision 0005:
// one delivery_attempts row per provider call; gap "nothing compares provider
// requests with delivery_attempts rows").
// Dials: 503 on 30 % of requests; APP_MAX_ATTEMPTS=10 so none exhausts
// (0.3^10 per notification); APP_BREAKER_FAILURES=1000 so a random run of
// 503s cannot declare a provider outage and change the pace.
func TestGapDeliveryR12AttemptsAccountedExactly(t *testing.T) {
	const n = 210
	chans := []string{"sms", "email", "push"}
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(map[string]string{
		"APP_MAX_ATTEMPTS":     "10",
		"APP_BREAKER_FAILURES": "1000",
	})})
	ids := s.dlCreateMany(t, n, func(i int) apiclient.NotificationCreate {
		return msg(chans[i%3], fmt.Sprintf("gap R12 accounting %d", i))
	})
	s.dlFaults(t, fakeprovider.Fault{Mode: fakeprovider.ModeStatus, Status: 503, Ratio: 0.3})
	for range 3 {
		s.StartWorker()
	}
	s.dlAllSent(t, 60*time.Second, n, "1=1")
	rows, reqs := s.dgAccounted(t, ids, 15*time.Second)

	totalReqs := len(s.Provider.Requests(fakeprovider.Filter{}))
	t.Logf("%d delivery_attempts rows, %d provider requests", dgSum(rows), totalReqs)
	if dgSum(rows) != totalReqs {
		t.Errorf("delivery_attempts rows %d != provider requests %d", dgSum(rows), totalReqs)
	}

	for _, id := range ids {
		got := s.get(t, id)
		if got.Attempts != reqs[id.String()] || len(history(got)) != reqs[id.String()] {
			t.Errorf("%s: GET attempts=%d, deliveryAttempts=%d, provider requests=%d; want all equal",
				id, got.Attempts, len(history(got)), reqs[id.String()])
		}
	}

	responses := map[int]int{}
	for _, c := range s.Provider.Stats().Channels {
		for st, k := range c.Responses {
			responses[st] += k
		}
	}
	outcomes := s.dgGroupCount(t, "SELECT outcome, COUNT(*) FROM delivery_attempts GROUP BY outcome")
	// Rows by what the worker heard: the HTTP status it recorded, or
	// "timeout" for a transient row with no status (the call ended without a
	// response in hand).
	heard := s.dgGroupCount(t, "SELECT CASE WHEN http_status IS NULL THEN 'timeout' ELSE CAST(http_status AS CHAR) END, COUNT(*) "+
		"FROM delivery_attempts WHERE outcome = 'transient' GROUP BY 1")
	t.Logf("double responses %v; stored outcomes %v; transient rows by status %v", responses, outcomes, heard)
	// Decision 0005: a call that may have been sent is one attempt. A call
	// whose timer fires after its request was written (a descheduled worker
	// under load) reaches the double and is answered 202 or 503, while the
	// worker records a timeout: transient with no HTTP status. Responses and
	// rows can then differ by such timeouts, and only by them. The exact
	// per-notification rows == requests check above is unchanged. Here no
	// row records an answer the double did not give, and every answer the
	// worker did not record is a timeout row, or an unknown row (a lease
	// that ran out mid-call, which 0005 also counts as an attempt).
	if outcomes["accepted"] > responses[202] {
		t.Errorf("accepted rows %d > 202 responses %d", outcomes["accepted"], responses[202])
	}
	if heard["503"] > responses[503] {
		t.Errorf("transient rows with HTTP 503 %d > 503 responses %d", heard["503"], responses[503])
	}
	unheard := responses[202] - outcomes["accepted"] + responses[503] - heard["503"]
	if heard["timeout"]+outcomes["unknown"] != unheard {
		t.Errorf("timeout rows %d + unknown rows %d != responses the worker did not record %d (202: %d - %d accepted; 503: %d - %d transient with HTTP 503)",
			heard["timeout"], outcomes["unknown"], unheard, responses[202], outcomes["accepted"], responses[503], heard["503"])
	}
	for st, k := range heard {
		if st != "503" && st != "timeout" {
			t.Errorf("%d transient rows with HTTP status %s; only 503 or none was expected", k, st)
		}
	}
	if responses[503] == 0 {
		t.Errorf("no 503 was injected; the fault did not fire")
	}
	for st, k := range responses {
		if st != 202 && st != 503 && k > 0 {
			t.Errorf("double answered %d with status %d; only 202 and 503 were expected", k, st)
		}
	}
	if outcomes["permanent"] != 0 {
		t.Errorf("%d permanent rows; no permanent answer was injected", outcomes["permanent"])
	}
}

// dgB1Env is the provider-health dial set of the outage tests (as
// TestDeliveryB1): an outage is declared after APP_BREAKER_FAILURES=5
// consecutive provider faults fleet-wide (the default), one probe per 1 s
// cool-down (5 s by default; 1 s fits several cool-downs into a few seconds);
// APP_PROVIDER_TIMEOUT=500ms, APP_RETRY_BASE=100ms, APP_RETRY_CAP=1s.
func dgB1Env(maxAttempts int) map[string]string {
	return dlFastEnv(map[string]string{
		"APP_BREAKER_FAILURES": "5",
		"APP_BREAKER_COOLDOWN": "1s",
		"APP_MAX_ATTEMPTS":     fmt.Sprint(maxAttempts),
	})
}

// dgFirstArrival waits until the double recorded a request on channel and
// returns the earliest arrival time recorded.
func (s *stack) dgFirstArrival(t *testing.T, channel string) time.Time {
	t.Helper()
	var first time.Time
	dlEventually(t, 15*time.Second, "the first "+channel+" request reaches the double", func() (bool, string) {
		rs := s.dlRequests(fakeprovider.Filter{Channel: channel})
		if len(rs) == 0 {
			return false, "no " + channel + " request recorded"
		}
		first = rs[0].ReceivedAt
		return true, ""
	})
	return first
}

// B1: Bonus: Failure Handling - an outage that HANGS (decision 0008; gap
// "B1's outage is a fast 503, never a hang"): the breaker opens on timeouts
// as it does on 503s, so no sms notification fails during the hang, sms
// requests drop to probe level, and everything is sent once the hang ends.
// Isolation of the other channels (verifier finding 5) is proved by
// TestGapDeliveryB1HungChannelDoesNotStallOthers; here the breaker cuts the
// hang short, so the fast channels' rates are logged, not asserted (an
// absolute rate floor here measured the runner, not isolation).
// Dials: dgB1Env with APP_MAX_ATTEMPTS=5; sms requests are held 2 s against a
// 500 ms timeout for 6 s while email and push drain a 900 backlog each
// (about 9.5 s at 95/s, so both are still busy when the outage ends).
func TestGapDeliveryB1HangingOutage(t *testing.T) {
	const perFast, perSMS = 900, 300
	const outage = 6 * time.Second
	s := startStack(t, stackOptions{Workers: 0, Env: dgB1Env(5)})
	s.dlCreateMany(t, perSMS, func(i int) apiclient.NotificationCreate { return msg("sms", fmt.Sprintf("gap B1 hang sms %d", i)) })
	fast := []string{"email", "push"}
	s.dlCreateMany(t, 2*perFast, func(i int) apiclient.NotificationCreate {
		return msg(fast[i%2], fmt.Sprintf("gap B1 hang fast %d", i))
	})
	s.dlFaults(t, fakeprovider.Fault{Channel: "sms", Mode: fakeprovider.ModeDelay, DelayMS: 2000})
	for range 3 {
		s.StartWorker()
	}

	start := s.dgFirstArrival(t, "sms")
	consistently(t, time.Until(start.Add(outage)), func() bool {
		return s.count(t, "SELECT COUNT(*) FROM notifications WHERE channel = 'sms' AND status = 'failed'") == 0
	}, "no sms notification is failed during the hanging outage")
	dlWaitUntil(start.Add(outage))
	s.dlFaults(t)
	cleared := time.Now()

	for _, ch := range fast {
		s.dlAllSent(t, 30*time.Second, perFast, "channel = ?", ch)
		got := dlReceivedIn(s.dlRequests(fakeprovider.Filter{Channel: ch}), start, start.Add(outage))
		rate := float64(got) / outage.Seconds()
		t.Logf("%s during the %s sms hang: %d requests = %.2f/s (logged only)", ch, outage, got, rate)
	}

	// Probe-level bound. Before an outage can be declared, the first 5 calls
	// must time out (500 ms); every call admitted meanwhile is in flight: add
	// the 200 ms reservation horizon and 100 ms slack: at most 95/s * 0.8 s ~
	// 76 requests. Once declared, one probe per 1 s cool-down fleet-wide:
	// 6 + 1 = 7 (it was 3 * 7 = 21 with one breaker per worker process).
	// Bound 100, against ~570 the limiter alone would allow in 6 s.
	const smsBound = 100
	smsDuring := dlReceivedIn(s.dlRequests(fakeprovider.Filter{Channel: "sms"}), start, cleared)
	t.Logf("sms requests during the %s hang: %d (bound %d; limiter alone ~%.0f)", cleared.Sub(start).Round(time.Millisecond), smsDuring, smsBound, dlPacedRate*cleared.Sub(start).Seconds())
	if smsDuring > smsBound {
		t.Errorf("sms requests during the hang: %d, want at most %d (probe level): the channel did not pause", smsDuring, smsBound)
	}
	s.dlAllSent(t, 40*time.Second, perSMS, "channel = 'sms'")
}

// dgInWindow keeps the requests of rs that arrived in [from, to).
func dgInWindow(rs []fakeprovider.Request, from, to time.Time) []fakeprovider.Request {
	var out []fakeprovider.Request
	for _, r := range rs {
		if !r.ReceivedAt.Before(from) && r.ReceivedAt.Before(to) {
			out = append(out, r)
		}
	}
	return out
}

// B1: Bonus: Failure Handling - one hung channel does not stall the others
// (decision 0008; verifier finding 5 "one hung channel stalls the others: all
// channels share one pool of slots"; gap "B1's outage is a fast 503, never a
// hang"). The breaker is kept out of it so the hang lasts the whole window.
// Dials: dlFastEnv with APP_WORKER_CONCURRENCY=12, APP_PROVIDER_TIMEOUT=2s,
// APP_LEASE_DURATION=5s, APP_BREAKER_FAILURES=10000 (its upper bound: no
// provider outage is ever declared); 300 sms + 900 email + 900 push queued, 3 workers; every sms request
// is held 5 s; the window is 6 s from the first sms arrival.
// Why 40/s: with the defect reintroduced (one shared pool of provider-call
// places) email and push measured 18.5 and 17.0/s; the correct build (places
// per channel) measured 81-86/s on a loaded dev machine, and 56-60/s is the
// worst seen for a correct build on a starved 2-core machine. 40 sits between
// the two with room on both sides, so it is not a measure of the machine.
// After the window the hang is cleared and everything must be sent: the hung
// channel recovers on its own, with no breaker.
func TestGapDeliveryB1HungChannelDoesNotStallOthers(t *testing.T) {
	const perFast, perSMS = 900, 300
	const window = 6 * time.Second
	const minFastRate = 40.0
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(map[string]string{
		"APP_WORKER_CONCURRENCY": "12",
		"APP_PROVIDER_TIMEOUT":   "2s",
		"APP_LEASE_DURATION":     "5s",
		"APP_BREAKER_FAILURES":   "10000",
	})})
	s.dlCreateMany(t, perSMS, func(i int) apiclient.NotificationCreate { return msg("sms", fmt.Sprintf("gap B1 stall sms %d", i)) })
	fast := []string{"email", "push"}
	s.dlCreateMany(t, 2*perFast, func(i int) apiclient.NotificationCreate {
		return msg(fast[i%2], fmt.Sprintf("gap B1 stall fast %d", i))
	})
	s.dlFaults(t, fakeprovider.Fault{Channel: "sms", Mode: fakeprovider.ModeDelay, DelayMS: 5000})
	for range 3 {
		s.StartWorker()
	}

	start := s.dgFirstArrival(t, "sms")
	dlWaitUntil(start.Add(window))
	s.dlFaults(t)
	cleared := time.Now()
	t.Logf("sms hang cleared %s after the first sms arrival; stored counts %v", cleared.Sub(start).Round(time.Millisecond), s.dlCounts(t, "1=1"))

	for _, ch := range fast {
		s.dlAllSent(t, 60*time.Second, perFast, "channel = ?", ch)
		in := dgInWindow(s.dlRequests(fakeprovider.Filter{Channel: ch}), start, start.Add(window))
		rate := float64(len(in)) / window.Seconds()
		t.Logf("%s during the %s sms hang: %d requests = %.2f/s; per 1 s bucket: %v", ch, window, len(in), rate, dlR8Histogram(in))
		if rate < minFastRate {
			t.Errorf("%s: %.2f requests/s during the sms hang, want at least %.2f (a hung channel must not stall the others)", ch, rate, minFastRate)
		}
	}

	smsIn := dgInWindow(s.dlRequests(fakeprovider.Filter{Channel: "sms"}), start, start.Add(window))
	t.Logf("sms during the hang: %d requests; per 1 s bucket: %v", len(smsIn), dlR8Histogram(smsIn))
	s.dlAllSent(t, 60*time.Second, perSMS, "channel = 'sms'")
	t.Logf("all %d sms sent %s after the hang was cleared", perSMS, time.Since(cleared).Round(time.Millisecond))
}

// B1: Bonus: Failure Handling - "no attempts are burned" while the channel is
// paused (decision 0008: while open the worker does not claim that channel and
// nothing is counted as an attempt).
// Dials: dgB1Env with APP_MAX_ATTEMPTS=3; an sms 503 outage of 8 s.
// Why this fails without the shared health: at 95/s the limiter would cycle
// the 200 sms through 3 failed calls each within ~6.3 s (claims go by due_at,
// so each round takes the whole backlog before a retry comes up), and with
// backoff of at most 1 s per retry every one would be failed/exhausted before
// the 8 s outage ends. With it only the calls before the outage is declared
// (and those in flight then) and one probe per cool-down fleet-wide reach the
// provider.
// Rule changed with the shared provider health (decision 0026): APP_MAX_ATTEMPTS
// bounds notifications.budget_spent (failures counted against the message),
// not notifications.attempts (calls made). Excused calls (in flight when the
// outage was declared, probes) are still calls, so attempts may exceed
// APP_MAX_ATTEMPTS during an outage; the budget may not.
func TestGapDeliveryB1BreakerBurnsNoAttempts(t *testing.T) {
	const n, maxAttempts = 200, 3
	const outage = 8 * time.Second
	s := startStack(t, stackOptions{Workers: 0, Env: dgB1Env(maxAttempts)})
	ids := s.dlCreateMany(t, n, func(i int) apiclient.NotificationCreate { return msg("sms", fmt.Sprintf("gap B1 burn %d", i)) })
	s.dlFaults(t, fakeprovider.Fault{Channel: "sms", Mode: fakeprovider.ModeStatus, Status: 503})
	for range 3 {
		s.StartWorker()
	}
	start := s.dgFirstArrival(t, "sms")
	consistently(t, time.Until(start.Add(outage)), func() bool {
		return s.count(t, "SELECT COUNT(*) FROM notifications WHERE channel = 'sms' AND status = 'failed'") == 0
	}, "no sms notification is failed during the outage")
	dlWaitUntil(start.Add(outage))
	s.dlFaults(t)
	t.Logf("sms requests during the %s outage: %d", outage, dlReceivedIn(s.dlRequests(fakeprovider.Filter{Channel: "sms"}), start, start.Add(outage)))

	s.dlAllSent(t, 30*time.Second, n, "1=1")
	if k := s.count(t, "SELECT COUNT(*) FROM notifications WHERE budget_spent > ?", maxAttempts); k != 0 {
		t.Errorf("%d notifications spent more than APP_MAX_ATTEMPTS=%d of their failure budget", k, maxAttempts)
	}
	rows, _ := s.dgAccounted(t, ids, 10*time.Second)
	t.Logf("%d attempts over %d notifications after recovery", dgSum(rows), n)
}

// R8: Implement rate limiting: maximum 100 messages per second per channel -
// under a disturbed worker set (verifier findings 1 and 3; gap "R8 covers a
// steady drain only"). One of 3 workers is killed mid-drain and a new one
// started; the killed worker's claims return after the 2 s lease and must not
// go out on top of the full rate. A 100 ms delay keeps sends in flight so the
// kill lands on claimed rows.
func TestGapDeliveryR8RateUnderDisturbedWorker(t *testing.T) {
	const perChannel = 500
	chans := []string{"sms", "email", "push"}
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(nil)})
	s.dlCreateMany(t, perChannel*3, func(i int) apiclient.NotificationCreate {
		return msg(chans[i%3], fmt.Sprintf("gap R8 disturbed %d", i))
	})
	s.dlFaults(t, fakeprovider.Fault{Mode: fakeprovider.ModeDelay, DelayMS: 100})
	for range 3 {
		s.StartWorker()
	}
	dlEventually(t, 20*time.Second, "the drain is under way (300 requests)", func() (bool, string) {
		st := s.Provider.Stats()
		r := st.Channels["sms"].Received + st.Channels["email"].Received + st.Channels["push"].Received
		return r >= 300, fmt.Sprintf("%d requests", r)
	})
	s.KillWorker(2)
	s.StartWorker()
	t.Logf("killed worker-2, started worker-4 with stored counts %v", s.dlCounts(t, "1=1"))

	s.dlAllSent(t, 45*time.Second, perChannel*3, "1=1")
	st := s.Provider.Stats()
	for _, ch := range chans {
		cs := st.Channels[ch]
		t.Logf("%s: %d requests, busiest 1 s window %d", ch, cs.Received, cs.MaxRequestsPerSecond)
		if cs.MaxRequestsPerSecond > 100 {
			t.Errorf("%s: %d requests in one second at the provider, limit 100", ch, cs.MaxRequestsPerSecond)
		}
	}
}

// R16: Provider: POST { "to", "channel", "content" } with headers
// Idempotency-Key, X-Correlation-ID, traceparent (contract "Provider call";
// gap "R16 does not assert the correlation and trace headers").
func TestGapDeliveryR16CorrelationAndTraceHeaders(t *testing.T) {
	s := startStack(t, stackOptions{Workers: 1, Env: dlFastEnv(nil)})
	n := s.create(t, msg("push", "gap R16 headers"), corrID("gap-r16-"+randHex(16)))
	rs := s.providerRequests(t, n.Id, 15*time.Second)

	t.Run("X-Correlation-ID equals the notification's", func(t *testing.T) {
		for _, r := range rs {
			if r.CorrelationID != n.CorrelationId {
				t.Errorf("provider request X-Correlation-ID: want %q, got %q", n.CorrelationId, r.CorrelationID)
			}
		}
	})
	t.Run("traceparent is a valid W3C header", func(t *testing.T) {
		for _, r := range rs {
			if _, _, ok := parseTraceparent(r.Traceparent); !ok {
				t.Errorf("provider request traceparent %q is not a valid version-00 traceparent", r.Traceparent)
			}
		}
	})
}
