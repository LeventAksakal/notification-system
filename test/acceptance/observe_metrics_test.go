//go:build integration

package acceptance

import (
	"math"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// R13: Real-time metrics endpoint (queue depth, success/failure rates, latency)
// - "The endpoint's numbers match what a test just produced": queue depth per
// channel with nothing delivering (decision 0014).
func TestR13MetricsQueueDepth(t *testing.T) {
	start := time.Now()
	s := startStack(t, stackOptions{})

	type mix struct{ due, scheduled int }
	want := map[string]mix{"sms": {3, 2}, "email": {2, 1}, "push": {1, 3}}
	future := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	for ch, m := range want {
		for range m.due {
			s.create(t, msg(ch, "r13 due "+ch))
		}
		for range m.scheduled {
			b := msg(ch, "r13 later "+ch)
			b.ScheduledAt = &future
			s.create(t, b)
		}
		// One cancelled per channel: counted in totals, not in the queue.
		n := s.create(t, msg(ch, "r13 cancelled "+ch))
		s.cancel(t, n.Id)
	}

	m := s.metrics(t, 0)
	q := queueByChannel(t, m)
	depth, dueAll, schedAll := 0, 0, 0
	for ch, w := range want {
		got := q[ch]
		if got.Pending != w.due || got.Scheduled != w.scheduled || got.Processing != 0 {
			t.Errorf("queue[%s]: want pending=%d scheduled=%d processing=0, got %+v", ch, w.due, w.scheduled, got)
		}
		maxAge := int(time.Since(start).Milliseconds()) + 5000
		if got.OldestPendingAgeMs < 0 || got.OldestPendingAgeMs > maxAge {
			t.Errorf("queue[%s].oldestPendingAgeMs: want 0..%d, got %d", ch, maxAge, got.OldestPendingAgeMs)
		}
		depth += w.due + w.scheduled
		dueAll += w.due
		schedAll += w.scheduled
	}
	if m.QueueDepth != depth {
		t.Errorf("queueDepth: want %d (every unfinished notification), got %d", depth, m.QueueDepth)
	}
	wantTotals := apiclient.StatusCounts{Scheduled: schedAll, Pending: dueAll, Cancelled: 3}
	if m.Totals != wantTotals {
		t.Errorf("totals: want %+v, got %+v", wantTotals, m.Totals)
	}
	if m.Delivery.Sent != 0 || m.Delivery.Failed != 0 || m.Delivery.Attempts != 0 ||
		m.Delivery.SuccessRate != 0 || m.Delivery.FailureRate != 0 {
		t.Errorf("delivery with nothing delivered: want all zero, got %+v", m.Delivery)
	}
	if m.WindowSeconds != 60 {
		t.Errorf("windowSeconds default: want 60, got %d", m.WindowSeconds)
	}
}

// R13: Real-time metrics endpoint (queue depth, success/failure rates, latency)
// - "The endpoint's numbers match what a test just produced": a provider double
// that refuses a known number permanently; then "read through any API
// instance" (decision 0014).
func TestR13MetricsDeliveryOutcomes(t *testing.T) {
	start := time.Now()
	s := startStack(t, stackOptions{
		Workers: 1,
		// Keep the breaker out of this test: it would only slow the email lane.
		Env: map[string]string{"APP_BREAKER_MIN_REQUESTS": "1000"},
	})
	const k = 3 // email notifications the provider refuses (400 = permanent, one call each)
	if err := s.Provider.SetFaults([]fakeprovider.Fault{
		{Channel: "email", Mode: fakeprovider.ModeStatus, Status: http.StatusBadRequest, Count: k},
	}); err != nil {
		t.Fatal(err)
	}
	perChannel := map[string]int{"sms": 6, "email": 8, "push": 4}
	total := 0
	for ch, n := range perChannel {
		for range n {
			s.create(t, msg(ch, "r13 delivery "+ch))
		}
		total += n
	}
	wantFailed := map[string]int{"sms": 0, "email": k, "push": 0}

	var m apiclient.Metrics
	last := "none"
	if !poll(120*time.Second, func() bool {
		r, err := s.API.GetMetricsWithResponse(t.Context(), &apiclient.GetMetricsParams{WindowSeconds: ptr(3600)})
		if err != nil {
			return false
		}
		last = show(r)
		m = apiclient.Metrics{}
		if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
			return false
		}
		m = *r.JSON200
		return m.QueueDepth == 0 && m.Delivery.Sent+m.Delivery.Failed == total
	}) {
		t.Fatalf("not within 120s: metrics never showed all %d notifications finished; last response %v; finished in db: %d",
			total, last, s.count(t, "SELECT COUNT(*) FROM notifications WHERE status IN ('sent','failed')"))
	}
	elapsed := time.Since(start)

	reqs := s.Provider.Requests(fakeprovider.Filter{})
	non2xx := 0
	for _, r := range reqs {
		if r.Status < 200 || r.Status > 299 {
			non2xx++
		}
	}

	d := m.Delivery
	if d.Sent != total-k || d.Failed != k {
		t.Errorf("delivery: want sent=%d failed=%d, got sent=%d failed=%d", total-k, k, d.Sent, d.Failed)
	}
	wantRate := float64(d.Sent) / float64(d.Sent+d.Failed)
	if math.Abs(d.SuccessRate-wantRate) > 1e-9 || math.Abs(d.FailureRate-(1-wantRate)) > 1e-9 {
		t.Errorf("rates: want success=%v failure=%v, got %v / %v", wantRate, 1-wantRate, d.SuccessRate, d.FailureRate)
	}
	if d.Attempts != len(reqs) || d.FailedAttempts != non2xx {
		t.Errorf("attempts: want %d (provider requests) failed %d (non-2xx), got %d / %d",
			len(reqs), non2xx, d.Attempts, d.FailedAttempts)
	}
	l := d.LatencyMs
	if l.P50 > l.P95 || l.P95 > l.P99 || l.P99 <= 0 || int64(l.P99) >= elapsed.Milliseconds() {
		t.Errorf("latencyMs: want 0 < p50 <= p95 <= p99 < %d ms (test elapsed), got %+v", elapsed.Milliseconds(), l)
	}
	if m.QueueDepth != 0 {
		t.Errorf("queueDepth: want 0, got %d", m.QueueDepth)
	}
	if m.Totals.Sent != total-k || m.Totals.Failed != k || m.Totals.Pending+m.Totals.Processing+m.Totals.Scheduled != 0 {
		t.Errorf("totals: want sent=%d failed=%d and nothing unfinished, got %+v", total-k, k, m.Totals)
	}

	byCh := deliveryByChannel(t, m)
	var sum apiclient.DeliveryTotals
	for ch, n := range perChannel {
		c := byCh[ch]
		if c.Sent != n-wantFailed[ch] || c.Failed != wantFailed[ch] {
			t.Errorf("deliveryByChannel[%s]: want sent=%d failed=%d, got %+v", ch, n-wantFailed[ch], wantFailed[ch], c)
		}
		r := float64(c.Sent) / float64(c.Sent+c.Failed)
		if math.Abs(c.SuccessRate-r) > 1e-9 || math.Abs(c.FailureRate-(1-r)) > 1e-9 {
			t.Errorf("deliveryByChannel[%s] rates: want %v/%v, got %v/%v", ch, r, 1-r, c.SuccessRate, c.FailureRate)
		}
		if cl := c.LatencyMs; cl.P50 > cl.P95 || cl.P95 > cl.P99 {
			t.Errorf("deliveryByChannel[%s].latencyMs out of order: %+v", ch, cl)
		}
		sum.Sent += c.Sent
		sum.Failed += c.Failed
		sum.Attempts += c.Attempts
		sum.FailedAttempts += c.FailedAttempts
	}
	if sum.Sent != d.Sent || sum.Failed != d.Failed || sum.Attempts != d.Attempts || sum.FailedAttempts != d.FailedAttempts {
		t.Errorf("deliveryByChannel does not sum to delivery: sum %+v, delivery %+v", sum, d)
	}
	for _, ch := range []string{"sms", "email", "push"} {
		if q := queueByChannel(t, m)[ch]; q.Pending+q.Scheduled+q.Processing != 0 {
			t.Errorf("queue[%s]: want empty, got %+v", ch, q)
		}
	}

	t.Run("same numbers from a second API instance", func(t *testing.T) {
		peer := s.startServe(t, nil)
		var a, b apiclient.Metrics
		if !poll(10*time.Second, func() bool {
			a = s.metrics(t, 3600)
			b = peer.metrics(t, 3600)
			return reflect.DeepEqual(sortedQueue(a.Queue), sortedQueue(b.Queue)) &&
				a.Totals == b.Totals && a.Delivery == b.Delivery
		}) {
			t.Fatalf("instances disagree:\nfirst  queue=%+v totals=%+v delivery=%+v\nsecond queue=%+v totals=%+v delivery=%+v",
				a.Queue, a.Totals, a.Delivery, b.Queue, b.Totals, b.Delivery)
		}
		if b.Delivery.Sent != total-k || b.Delivery.Failed != k {
			t.Errorf("second instance delivery: want sent=%d failed=%d, got %+v", total-k, k, b.Delivery)
		}
	})
}

// R13: Real-time metrics endpoint - the window parameter is bounded 1..3600
// (api/openapi.yaml, /v1/metrics).
func TestR13MetricsWindowSeconds(t *testing.T) {
	s := startStack(t, stackOptions{})
	for _, w := range []int{0, 3601} {
		t.Run("windowSeconds="+strconv.Itoa(w)+" refused", func(t *testing.T) {
			r := answered(t)(s.API.GetMetricsWithResponse(t.Context(), &apiclient.GetMetricsParams{WindowSeconds: ptr(w)}))
			wantError(t, r, http.StatusBadRequest, "invalid_request", "windowSeconds")
		})
	}
	for _, w := range []int{1, 3600} {
		t.Run("windowSeconds="+strconv.Itoa(w)+" echoed", func(t *testing.T) {
			m := s.metrics(t, w)
			if m.WindowSeconds != w {
				t.Errorf("windowSeconds: want %d echoed, got %d", w, m.WindowSeconds)
			}
		})
	}
}
