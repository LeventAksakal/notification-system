//go:build integration

package acceptance

// Helpers of the delivery acceptance tests (R5, R7, R8, R9, R11, R12, R16,
// B1, B2). Every name here starts with "dl" so it cannot collide with the
// helpers of other test files in this package.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

// The limiter dials every stack starts with (harness_test.go) and the rate
// they pace at. Every throughput floor is written as dlPacedRate minus its
// margin, so a change of headroom is this one place. The cap (at most
// dlRateLimit per channel in any second) does not depend on the headroom.
const (
	dlRateLimit       = 100 // APP_RATE_LIMIT_PER_SECOND
	dlRateHeadroomPct = 5   // APP_RATE_LIMIT_HEADROOM_PERCENT (default since 14 Nov; 3 before)
	// dlPacedRate is the limiter's rate per channel: one slot per 10.53 ms.
	dlPacedRate = dlRateLimit * (100 - dlRateHeadroomPct) / 100.0 // 95/s
)

// dlRateFloorEnv names the test-run knob that relaxes throughput floors on a
// constrained runner (CI's 2-core runner sets it to 40). It only ever lowers
// "at least N requests per second" floors (each becomes min(strict, value));
// it never touches a cap (at most 100 per second), a count or a correctness
// assertion.
const dlRateFloorEnv = "ACCEPTANCE_RATE_FLOOR"

// dlRateFloorOverride is ACCEPTANCE_RATE_FLOOR as parsed by TestMain; 0 when
// the variable is not set, so every floor stays strict.
var dlRateFloorOverride float64

// dlReadRateFloor reads ACCEPTANCE_RATE_FLOOR. Unset returns 0; set to
// anything but a finite positive number is an error, and TestMain fails the
// whole run on it rather than silently keeping or dropping the floors.
func dlReadRateFloor() (float64, error) {
	// A knob of the test run itself (how starved the runner is), not a
	// setting of the service, so it is not read through internal/config.
	raw, ok := os.LookupEnv(dlRateFloorEnv) //nolint:forbidigo // test-run knob, not a service setting
	if !ok {
		return 0, nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsInf(f, 0) || !(f > 0) {
		return 0, fmt.Errorf("%s=%q: want a positive number of requests per second", dlRateFloorEnv, raw)
	}
	return f, nil
}

// dlRateFloor returns the throughput floor (requests per second) to assert
// for what: strict by default; when ACCEPTANCE_RATE_FLOOR is set,
// min(strict, override), so the knob can only ever lower a floor, never
// raise it. It logs which one applies. Use it only for "at least N/s"
// floors, never for a cap.
func dlRateFloor(t *testing.T, what string, strict float64) float64 {
	t.Helper()
	if dlRateFloorOverride > 0 {
		floor := min(strict, dlRateFloorOverride)
		t.Logf("throughput floor %s: %.2f/s = min(strict default %.2f/s, %s=%.2f/s); the override only lowers a floor", what, floor, strict, dlRateFloorEnv, dlRateFloorOverride)
		return floor
	}
	t.Logf("throughput floor %s: %.2f/s (strict default; %s not set)", what, strict, dlRateFloorEnv)
	return strict
}

// dlFastEnv is the dial set the delivery tests share: short backoff, timeout,
// lease and breaker timings so retries, crashes and outages resolve in
// seconds. extra overrides any of them.
func dlFastEnv(extra map[string]string) map[string]string {
	env := map[string]string{
		"APP_RETRY_BASE":       "100ms",
		"APP_RETRY_CAP":        "1s",
		"APP_PROVIDER_TIMEOUT": "500ms",
		"APP_LEASE_DURATION":   "2s",
		"APP_REAP_INTERVAL":    "500ms",
	}
	maps.Copy(env, extra)
	return env
}

// dlAttempt is the API's DeliveryAttempt schema.
type dlAttempt struct {
	AttemptNo         int       `json:"attemptNo"`
	StartedAt         time.Time `json:"startedAt"`
	DurationMs        int       `json:"durationMs"`
	Outcome           string    `json:"outcome"`
	HTTPStatus        *int      `json:"httpStatus"`
	Error             string    `json:"error"`
	ProviderMessageID string    `json:"providerMessageId"`
}

func (a dlAttempt) String() string {
	st := "none"
	if a.HTTPStatus != nil {
		st = fmt.Sprint(*a.HTTPStatus)
	}
	return fmt.Sprintf("#%d %s http=%s err=%q", a.AttemptNo, a.Outcome, st, a.Error)
}

// dlNotification is the Notification schema with the delivery fields the
// intake helpers do not decode.
type dlNotification struct {
	notification
	ProviderMessageID string      `json:"providerMessageId"`
	FinishedAt        *time.Time  `json:"finishedAt"`
	History           []dlAttempt `json:"deliveryAttempts"` // shadows notification.DeliveryAttempts
}

// dlGet fetches one notification with its attempt history; requires 200.
func (s *stack) dlGet(t *testing.T, id string) dlNotification {
	t.Helper()
	r := s.getJSON(t, "/v1/notifications/"+id)
	if r.Status != http.StatusOK {
		t.Fatalf("GET /v1/notifications/%s: want 200, got %s", id, r)
	}
	var n dlNotification
	r.decode(t, &n)
	return n
}

// dlEventually polls cond until it reports true or timeout passes, then fails
// with what cond said last (so the message shows the final state, not the
// state when the wait began).
func dlEventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		ok, detail := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not within %s: %s; last seen: %s", timeout, what, detail)
		}
		<-tick.C
	}
}

// dlWaitUntil blocks until the wall clock passes at; at is always a few
// seconds away (a scheduled time, the end of an outage), never unbounded.
func dlWaitUntil(at time.Time) {
	if d := time.Until(at); d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		<-timer.C
	}
}

// dlCreateMany creates n notifications through batches of at most 1000 and
// returns their ids in creation order.
func (s *stack) dlCreateMany(t *testing.T, n int, item func(i int) map[string]any) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for start := 0; start < n; start += 1000 {
		end := min(start+1000, n)
		items := make([]map[string]any, 0, end-start)
		for i := start; i < end; i++ {
			items = append(items, item(i))
		}
		b := s.createBatch(t, items)
		if len(b.NotificationIDs) != len(items) {
			t.Fatalf("batch of %d returned %d ids", len(items), len(b.NotificationIDs))
		}
		ids = append(ids, b.NotificationIDs...)
	}
	return ids
}

// dlCounts returns stored status -> count over the rows matching where
// (a SQL condition on notifications; "1=1" for all).
func (s *stack) dlCounts(t *testing.T, where string, args ...any) map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// where is always a constant condition written in the test source; values go through args.
	rows, err := s.DB.QueryContext(ctx, "SELECT status, COUNT(*) FROM notifications WHERE "+where+" GROUP BY status", args...) //nolint:gosec // G202: where is a test-source constant, values are bound via args
	if err != nil {
		t.Fatalf("status counts: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			t.Fatalf("status counts: %v", err)
		}
		out[st] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("status counts: %v", err)
	}
	return out
}

// dlAllSent waits until every row matching where is stored as sent. It fails
// at once if any is failed or cancelled, and on timeout with the counts.
func (s *stack) dlAllSent(t *testing.T, timeout time.Duration, want int, where string, args ...any) {
	t.Helper()
	dlEventually(t, timeout, fmt.Sprintf("all %d notifications (%s) stored as sent", want, where), func() (bool, string) {
		c := s.dlCounts(t, where, args...)
		if c["failed"] > 0 || c["cancelled"] > 0 {
			t.Fatalf("notifications (%s) ended failed or cancelled: %v", where, c)
		}
		return c["sent"] == want, fmt.Sprintf("%v", c)
	})
}

// dlFaults replaces the double's fault rules; it fails the test on a bad rule.
func (s *stack) dlFaults(t *testing.T, rules ...fakeprovider.Fault) {
	t.Helper()
	if err := s.Provider.SetFaults(rules); err != nil {
		t.Fatalf("set faults %+v: %v", rules, err)
	}
}

// dlRequests returns the double's recorded requests for f, sorted by the time
// they arrived (history is recorded when a request ends, not when it arrives).
func (s *stack) dlRequests(f fakeprovider.Filter) []fakeprovider.Request {
	rs := s.Provider.Requests(f)
	sort.SliceStable(rs, func(i, j int) bool { return rs[i].ReceivedAt.Before(rs[j].ReceivedAt) })
	return rs
}

// dlDeliveredKeys is the set of Idempotency-Keys the double counted as delivered.
func (s *stack) dlDeliveredKeys() map[string]bool {
	out := map[string]bool{}
	for _, r := range s.Provider.Requests(fakeprovider.Filter{}) {
		if r.Delivery {
			out[r.IdempotencyKey] = true
		}
	}
	return out
}

// dlRate is a drain's average request rate: (count-1)/(last-first) over
// requests sorted by arrival.
func dlRate(rs []fakeprovider.Request) (n int, span time.Duration, perSecond float64) {
	n = len(rs)
	if n < 2 {
		return n, 0, 0
	}
	span = rs[n-1].ReceivedAt.Sub(rs[0].ReceivedAt)
	if span <= 0 {
		return n, span, 0
	}
	return n, span, float64(n-1) / span.Seconds()
}

// dlReceivedIn counts requests in rs that arrived in [from, to).
func dlReceivedIn(rs []fakeprovider.Request, from, to time.Time) int {
	n := 0
	for _, r := range rs {
		if !r.ReceivedAt.Before(from) && r.ReceivedAt.Before(to) {
			n++
		}
	}
	return n
}

// dlCapture is one provider call seen by dlRecorder: the raw request body and
// headers as the worker sent them, and the body the double answered with.
type dlCapture struct {
	Path         string
	Header       http.Header
	Body         []byte
	Status       int
	ResponseBody []byte
}

// dlRecorder is a transparent proxy in front of the provider double. Point a
// channel's provider URL at it (through stackOptions.Env) to see the exact
// bytes a worker sends and the exact answer it gets.
type dlRecorder struct {
	URL      string
	upstream atomic.Pointer[string]
	mu       sync.Mutex
	calls    []dlCapture
}

func dlNewRecorder(t *testing.T) *dlRecorder {
	t.Helper()
	rec := &dlRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(rec.serve))
	t.Cleanup(srv.Close)
	rec.URL = srv.URL
	return rec
}

// SetUpstream sets the double's base URL; calls before it get 503.
func (r *dlRecorder) SetUpstream(base string) { r.upstream.Store(&base) }

func (r *dlRecorder) Calls() []dlCapture {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func (r *dlRecorder) serve(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	c := dlCapture{Path: req.URL.Path, Header: req.Header.Clone(), Body: body}
	up := r.upstream.Load()
	if up == nil {
		c.Status = http.StatusServiceUnavailable
		r.record(c)
		http.Error(w, "recorder has no upstream yet", http.StatusServiceUnavailable)
		return
	}
	// The recorder forwards by design: the target is the test's own provider
	// double on loopback (set by SetUpstream), only the path comes from the worker.
	out, err := http.NewRequestWithContext(req.Context(), req.Method, strings.TrimRight(*up, "/")+req.URL.Path, bytes.NewReader(body)) //nolint:gosec // G704: test proxy to the test's own loopback double
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out.Header = req.Header.Clone()
	res, err := http.DefaultClient.Do(out) //nolint:gosec // G704: test proxy to the test's own loopback double
	if err != nil {
		r.record(c)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = res.Body.Close() }()
	rb, _ := io.ReadAll(res.Body)
	c.Status, c.ResponseBody = res.StatusCode, rb
	r.record(c)
	for k, vs := range res.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(res.StatusCode)
	_, _ = w.Write(rb)
}

func (r *dlRecorder) record(c dlCapture) {
	r.mu.Lock()
	r.calls = append(r.calls, c)
	r.mu.Unlock()
}

// dlJSONKeys returns the sorted top-level keys of a JSON object.
func dlJSONKeys(t *testing.T, b []byte) (map[string]any, []string) {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("provider request body is not a JSON object: %v\n%s", err, b)
	}
	return m, slices.Sorted(maps.Keys(m))
}
