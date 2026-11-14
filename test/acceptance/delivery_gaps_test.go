//go:build integration

package acceptance

// Gap tests the delivery verifier named (.agents/reviews/2026-11-14-delivery-verifier.md,
// "Test gaps" and findings 1-6). Helpers here start with "dg".

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
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
func (s *stack) dgAccounted(t *testing.T, ids []string, timeout time.Duration) (rows, reqs map[string]int) {
	t.Helper()
	dlEventually(t, timeout, "delivery_attempts rows == provider requests for every notification", func() (bool, string) {
		if n := s.dgInFlight(); n != 0 {
			return false, fmt.Sprintf("%d requests still in flight at the double", n)
		}
		reqs = s.dgRequestsPerKey()
		rows = s.dgAttemptRows(t)
		var bad []string
		for _, id := range ids {
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
// call: the call's timeout is capped at the time left on the lease minus a
// margin" (verifier finding 2, gap "no test pauses a worker past its lease").
// Dials: APP_LEASE_DURATION=2s, APP_PROVIDER_TIMEOUT=5s, APP_REAP_INTERVAL=500ms,
// APP_MAX_ATTEMPTS=5, 2 workers; the first 6 sms requests are held 4 s, longer
// than the lease. APP_BREAKER_MIN_REQUESTS=1000 keeps the breaker out of it.
func TestGapDeliveryR11CallOutlivesLease(t *testing.T) {
	const n, held, maxAttempts = 20, 6, 5
	env := dlFastEnv(map[string]string{
		"APP_LEASE_DURATION":       "2s",
		"APP_PROVIDER_TIMEOUT":     "5s",
		"APP_MAX_ATTEMPTS":         fmt.Sprint(maxAttempts),
		"APP_BREAKER_MIN_REQUESTS": "1000",
	})
	for _, dedupe := range []bool{true, false} {
		t.Run(fmt.Sprintf("provider dedupe %v", dedupe), func(t *testing.T) {
			s := startStack(t, stackOptions{Workers: 0, Env: env, Provider: []fakeprovider.Option{fakeprovider.WithDedupe(dedupe)}})
			ids := s.dlCreateMany(t, n, func(i int) map[string]any {
				return msg("sms", fmt.Sprintf("gap R11 lease %d", i))
			})
			s.dlFaults(t, fakeprovider.Fault{Channel: "sms", Mode: fakeprovider.ModeDelay, DelayMS: 4000, Count: held})
			s.StartWorker()
			s.StartWorker()

			s.dlAllSent(t, 45*time.Second, n, "1=1")
			rows, reqs := s.dgAccounted(t, ids, 15*time.Second)
			if p := s.count(t, "SELECT COUNT(*) FROM notifications WHERE status = 'processing'"); p != 0 {
				t.Errorf("%d notifications still processing", p)
			}
			st := s.Provider.Stats()
			t.Logf("dedupe %v: %d delivery_attempts rows, %d provider requests; maxRequestsPerKey %d, maxDeliveriesPerKey %d",
				dedupe, dgSum(rows), dgSum(reqs), st.MaxRequestsPerKey, st.MaxDeliveriesPerKey)
			if dedupe {
				// With dedupe on this holds by construction at the double; the
				// rows == requests check above is the one that bites.
				if st.MaxDeliveriesPerKey != 1 {
					t.Errorf("maxDeliveriesPerKey: want 1, got %d", st.MaxDeliveriesPerKey)
				}
			} else if st.MaxDeliveriesPerKey > maxAttempts {
				t.Errorf("maxDeliveriesPerKey: %d, want at most APP_MAX_ATTEMPTS=%d", st.MaxDeliveriesPerKey, maxAttempts)
			}
			if st.MaxRequestsPerKey > maxAttempts {
				t.Errorf("maxRequestsPerKey: %d, want at most APP_MAX_ATTEMPTS=%d", st.MaxRequestsPerKey, maxAttempts)
			}
		})
	}
}

// R12: Delivery & Retry Logic - attempts are accounted exactly (decision 0005:
// one delivery_attempts row per provider call; gap "nothing compares provider
// requests with delivery_attempts rows").
// Dials: 503 on 30 % of requests; APP_MAX_ATTEMPTS=10 so none exhausts
// (0.3^10 per notification); APP_BREAKER_MIN_REQUESTS=1000 so a random run of
// 503s cannot open the breaker and change the pace.
func TestGapDeliveryR12AttemptsAccountedExactly(t *testing.T) {
	const n = 210
	chans := []string{"sms", "email", "push"}
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(map[string]string{
		"APP_MAX_ATTEMPTS":         "10",
		"APP_BREAKER_MIN_REQUESTS": "1000",
	})})
	ids := s.dlCreateMany(t, n, func(i int) map[string]any {
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
		got := s.dlGet(t, id)
		if got.Attempts != reqs[id] || len(got.History) != reqs[id] {
			t.Errorf("%s: GET attempts=%d, deliveryAttempts=%d, provider requests=%d; want all equal",
				id, got.Attempts, len(got.History), reqs[id])
		}
	}

	responses := map[int]int{}
	for _, c := range s.Provider.Stats().Channels {
		for st, k := range c.Responses {
			responses[st] += k
		}
	}
	outcomes := s.dgGroupCount(t, "SELECT outcome, COUNT(*) FROM delivery_attempts GROUP BY outcome")
	t.Logf("double responses %v; stored outcomes %v", responses, outcomes)
	if responses[202] != outcomes["accepted"] {
		t.Errorf("202 responses %d != accepted rows %d", responses[202], outcomes["accepted"])
	}
	if responses[503] != outcomes["transient"] {
		t.Errorf("503 responses %d != transient rows %d", responses[503], outcomes["transient"])
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

// dgB1Env is the breaker dial set of the outage tests (as TestDeliveryB1):
// window 2 s, opens after 5 calls at >= 50 % failures, 1 s cool-down;
// APP_PROVIDER_TIMEOUT=500ms, APP_RETRY_BASE=100ms, APP_RETRY_CAP=1s.
func dgB1Env(maxAttempts int) map[string]string {
	return dlFastEnv(map[string]string{
		"APP_BREAKER_WINDOW":       "2s",
		"APP_BREAKER_MIN_REQUESTS": "5",
		"APP_BREAKER_COOLDOWN":     "1s",
		"APP_MAX_ATTEMPTS":         fmt.Sprint(maxAttempts),
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
// (9 s at 97/s, so both are still busy when the outage ends).
func TestGapDeliveryB1HangingOutage(t *testing.T) {
	const perFast, perSMS = 900, 300
	const outage = 6 * time.Second
	s := startStack(t, stackOptions{Workers: 0, Env: dgB1Env(5)})
	s.dlCreateMany(t, perSMS, func(i int) map[string]any { return msg("sms", fmt.Sprintf("gap B1 hang sms %d", i)) })
	fast := []string{"email", "push"}
	s.dlCreateMany(t, 2*perFast, func(i int) map[string]any {
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

	// Probe-level bound. Before the breaker can open, the first calls must
	// time out (500 ms); add the 200 ms reservation horizon and 100 ms slack:
	// at most 97/s * 0.8 s ~ 78 requests. Once open, each of the 3 worker
	// processes sends one probe per 1 s cool-down at most: 3 * (6 + 1) = 21.
	// Bound 100, against ~580 the limiter alone would allow in 6 s.
	const smsBound = 100
	smsDuring := dlReceivedIn(s.dlRequests(fakeprovider.Filter{Channel: "sms"}), start, cleared)
	t.Logf("sms requests during the %s hang: %d (bound %d; limiter alone ~%.0f)", cleared.Sub(start).Round(time.Millisecond), smsDuring, smsBound, 97*cleared.Sub(start).Seconds())
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
// APP_LEASE_DURATION=5s, APP_BREAKER_MIN_REQUESTS=10000 (the breaker never
// opens); 300 sms + 900 email + 900 push queued, 3 workers; every sms request
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
		"APP_WORKER_CONCURRENCY":   "12",
		"APP_PROVIDER_TIMEOUT":     "2s",
		"APP_LEASE_DURATION":       "5s",
		"APP_BREAKER_MIN_REQUESTS": "10000",
	})})
	s.dlCreateMany(t, perSMS, func(i int) map[string]any { return msg("sms", fmt.Sprintf("gap B1 stall sms %d", i)) })
	fast := []string{"email", "push"}
	s.dlCreateMany(t, 2*perFast, func(i int) map[string]any {
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
// Why this fails without the breaker: at 97/s the limiter would cycle the 200
// sms through 3 failed calls each within ~6.2 s (claims go by due_at, so each
// round takes the whole backlog before a retry comes up), and with backoff of
// at most 1 s per retry every one would be failed/exhausted before the 8 s
// outage ends. With the breaker only the calls before it opens and one probe
// per cool-down per worker reach the provider (~40), at most one per notification.
func TestGapDeliveryB1BreakerBurnsNoAttempts(t *testing.T) {
	const n, maxAttempts = 200, 3
	const outage = 8 * time.Second
	s := startStack(t, stackOptions{Workers: 0, Env: dgB1Env(maxAttempts)})
	ids := s.dlCreateMany(t, n, func(i int) map[string]any { return msg("sms", fmt.Sprintf("gap B1 burn %d", i)) })
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
	if k := s.count(t, "SELECT COUNT(*) FROM notifications WHERE attempts > ?", maxAttempts); k != 0 {
		t.Errorf("%d notifications have more than APP_MAX_ATTEMPTS=%d attempts", k, maxAttempts)
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
	s.dlCreateMany(t, perChannel*3, func(i int) map[string]any {
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
	n := s.create(t, msg("push", "gap R16 headers"), "X-Correlation-ID", "gap-r16-"+randHex(16))
	rs := s.providerRequests(t, n.ID, 15*time.Second)

	t.Run("X-Correlation-ID equals the notification's", func(t *testing.T) {
		for _, r := range rs {
			if r.CorrelationID != n.CorrelationID {
				t.Errorf("provider request X-Correlation-ID: want %q, got %q", n.CorrelationID, r.CorrelationID)
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
