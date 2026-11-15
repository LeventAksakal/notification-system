package fakeprovider_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// clock is a settable time source; tests move it instead of sleeping.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) set(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC).Add(d)
}

func start(t *testing.T, opts ...fakeprovider.Option) (*fakeprovider.Server, string, *clock) {
	t.Helper()
	c := &clock{}
	c.set(0)
	s := fakeprovider.New(append([]fakeprovider.Option{fakeprovider.WithClock(c.now)}, opts...)...)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts.URL, c
}

type sent struct {
	status int
	header http.Header
	body   map[string]any
	err    error
}

func send(t *testing.T, client *http.Client, method, url, body string, headers ...string) sent {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return sent{err: err}
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	out := sent{status: res.StatusCode, header: res.Header}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out.body); err != nil {
			t.Fatalf("response is not a JSON object: %s", raw)
		}
	}
	return out
}

const sms = `{"to":"+905551234567","channel":"sms","content":"Your message"}`

func msg(channel string) string {
	return fmt.Sprintf(`{"to":"+905551234567","channel":%q,"content":"Your message"}`, channel)
}

// R16: the provider request/response contract.
func TestProviderAcceptsTheContractAndRejectsEverythingElse(t *testing.T) {
	cases := []struct {
		name, body string
		want       int
	}{
		{"valid sms on any path", sms, http.StatusAccepted},
		{"valid email", msg("email"), http.StatusAccepted},
		{"valid push", msg("push"), http.StatusAccepted},
		{"unknown channel", msg("fax"), http.StatusBadRequest},
		{"missing to", `{"channel":"sms","content":"x"}`, http.StatusBadRequest},
		{"empty content", `{"to":"+90","channel":"sms","content":""}`, http.StatusBadRequest},
		{"missing channel", `{"to":"+90","content":"x"}`, http.StatusBadRequest},
		{"invalid JSON", `{"to":`, http.StatusBadRequest},
		{"trailing bytes after the object", sms + `x`, http.StatusBadRequest},
		{"a second object after the first", sms + sms, http.StatusBadRequest},
		{"trailing whitespace is fine", sms + "\n", http.StatusAccepted},
		{"unknown fields are ignored", `{"to":"+90","channel":"sms","content":"x","extra":1}`, http.StatusAccepted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, url, _ := start(t)
			got := send(t, nil, http.MethodPost, url+"/anything/here", tc.body)
			if got.status != tc.want {
				t.Fatalf("status %d, want %d (%v)", got.status, tc.want, got.body)
			}
			if tc.want == http.StatusBadRequest {
				if got.body["code"] == "" || got.body["message"] == "" {
					t.Fatalf("400 body lacks code/message: %v", got.body)
				}
				return
			}
			if !uuidV4.MatchString(fmt.Sprint(got.body["messageId"])) {
				t.Errorf("messageId %v is not a UUID v4", got.body["messageId"])
			}
			if got.body["status"] != "accepted" || got.body["timestamp"] != "2026-11-13T09:00:00Z" {
				t.Errorf("body %v, want status accepted at the clock's time", got.body)
			}
			if len(got.body) != 3 {
				t.Errorf("body has %d fields, want exactly 3: %v", len(got.body), got.body)
			}
		})
	}
}

// Every request is recorded with its headers and outcome; control calls are not deliveries.
func TestEveryRequestIsRecordedAndControlCallsAreNotDeliveries(t *testing.T) {
	s, url, _ := start(t)
	send(t, nil, http.MethodPost, url+"/send", sms,
		"Idempotency-Key", "n-1", "X-Correlation-ID", "corr-1", "traceparent", "00-abc-def-01")
	send(t, nil, http.MethodPost, url+"/send", `nope`)
	send(t, nil, http.MethodGet, url+"/_control/stats", "")
	send(t, nil, http.MethodPost, url+"/_control/typo", sms)

	reqs := s.Requests(fakeprovider.Filter{})
	if len(reqs) != 2 {
		t.Fatalf("recorded %d requests, want 2: %+v", len(reqs), reqs)
	}
	want := fakeprovider.Request{
		ReceivedAt: time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC), Path: "/send",
		To: "+905551234567", Channel: "sms", Content: "Your message",
		IdempotencyKey: "n-1", CorrelationID: "corr-1", Traceparent: "00-abc-def-01",
		Status: http.StatusAccepted, Delivery: true,
	}
	if reqs[0] != want {
		t.Errorf("first record\n got %+v\nwant %+v", reqs[0], want)
	}
	if reqs[1].Status != http.StatusBadRequest || reqs[1].Delivery {
		t.Errorf("invalid request recorded as %+v", reqs[1])
	}
	st := s.Stats()
	if st.Channels["sms"].Deliveries != 1 || st.Channels[fakeprovider.InvalidChannel].Received != 1 {
		t.Errorf("stats %+v", st)
	}
}

// R11, R12: dedupe on remembers keys; dedupe off counts every attempt.
func TestDedupeDecidesWhetherARepeatedKeyIsANewDelivery(t *testing.T) {
	cases := []struct {
		name            string
		dedupe          bool
		keys            []string
		wantDeliveries  int
		wantMaxPerKey   int
		wantKeysRepeat  int
		wantSameMessage bool
	}{
		{"dedupe on, same key thrice", true, []string{"k", "k", "k"}, 1, 1, 0, true},
		{"dedupe off, same key thrice", false, []string{"k", "k", "k"}, 3, 3, 1, false},
		{"dedupe on, no key is always a delivery", true, []string{"", ""}, 2, 0, 0, false},
		{"dedupe on, distinct keys", true, []string{"a", "b"}, 2, 1, 0, false},
		{"dedupe off, two keys repeated", false, []string{"a", "a", "b", "b", "b"}, 5, 3, 2, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, url, _ := start(t, fakeprovider.WithDedupe(!tc.dedupe))
			// Flip through the control API so it is exercised too.
			if got := send(t, nil, http.MethodPut, url+"/_control/dedupe", fmt.Sprintf(`{"enabled":%t}`, tc.dedupe)); got.status != http.StatusNoContent {
				t.Fatalf("PUT dedupe: %d", got.status)
			}
			ids := map[any]bool{}
			for _, k := range tc.keys {
				got := send(t, nil, http.MethodPost, url+"/", sms, "Idempotency-Key", k)
				if got.status != http.StatusAccepted {
					t.Fatalf("status %d", got.status)
				}
				ids[got.body["messageId"]] = true
			}
			if (len(ids) == 1) != tc.wantSameMessage {
				t.Errorf("%d distinct messageIds across %d sends", len(ids), len(tc.keys))
			}
			var st fakeprovider.Stats
			getJSON(t, url+"/_control/stats", &st)
			if st.Channels["sms"].Deliveries != tc.wantDeliveries || st.MaxDeliveriesPerKey != tc.wantMaxPerKey ||
				st.KeysDeliveredMoreThanOnce != tc.wantKeysRepeat || st.Channels["sms"].Received != len(tc.keys) {
				t.Errorf("stats %+v", st)
			}
		})
	}
}

// R8: the busiest 1-second span is computed exactly from delivery times.
func TestMaxDeliveriesPerSecondIsTheBusiestSlidingWindow(t *testing.T) {
	ms := time.Millisecond
	cases := []struct {
		name string
		at   []time.Duration
		want int
	}{
		{"none", nil, 0},
		{"one", []time.Duration{0}, 1},
		{"exactly 1s apart are separate windows", []time.Duration{0, time.Second, 2 * time.Second}, 1},
		{"window straddles a second boundary", []time.Duration{0, 600 * ms, 900 * ms, 1100 * ms, 1500 * ms}, 4},
		{"burst after a quiet spell", []time.Duration{0, 5 * time.Second, 5*time.Second + ms, 5*time.Second + 999*ms, 6 * time.Second}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, url, c := start(t)
			for _, d := range tc.at {
				c.set(d)
				send(t, nil, http.MethodPost, url, sms)
			}
			if got := s.Stats().Channels["sms"].MaxDeliveriesPerSecond; got != tc.want {
				t.Errorf("max per second %d, want %d", got, tc.want)
			}
		})
	}
}

// B1: each fault mode, its scope and its lifetime.
func TestFaultRulesScriptProviderFailures(t *testing.T) {
	quick := &http.Client{Timeout: 300 * time.Millisecond}
	cases := []struct {
		name           string
		rules          string
		rand           float64
		sends          []string // channels, in order
		client         *http.Client
		wantStatus     []int // 0 = transport error
		wantDeliveries int
		wantRetryAfter string
	}{
		{"status 503 for the next two, then normal",
			`[{"mode":"status","status":503,"retryAfterSeconds":7,"count":2}]`, 0,
			[]string{"sms", "sms", "sms"}, nil, []int{503, 503, 202}, 1, "7"},
		{"rule scoped to one channel",
			`[{"channel":"email","mode":"status","status":500}]`, 0,
			[]string{"sms", "email", "push"}, nil, []int{202, 500, 202}, 2, ""},
		{"first matching rule wins",
			`[{"mode":"status","status":429,"count":1},{"mode":"status","status":500}]`, 0,
			[]string{"sms", "sms"}, nil, []int{429, 500}, 0, ""},
		{"ratio fires when the draw is under it",
			`[{"mode":"status","status":502,"ratio":0.5}]`, 0.2,
			[]string{"sms"}, nil, []int{502}, 0, ""},
		{"ratio falls through when the draw is over it",
			`[{"mode":"status","status":502,"ratio":0.5}]`, 0.9,
			[]string{"sms"}, nil, []int{202}, 1, ""},
		{"delay then respond normally",
			`[{"mode":"delay","delayMs":20,"count":1}]`, 0,
			[]string{"sms"}, nil, []int{202}, 1, ""},
		{"accept then hang past the caller's timeout",
			`[{"mode":"accept_then_hang","delayMs":10000,"count":1}]`, 0,
			[]string{"sms"}, quick, []int{0}, 1, ""},
		{"drop the connection",
			`[{"mode":"drop","count":1}]`, 0,
			[]string{"sms", "sms"}, nil, []int{0, 202}, 1, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, url, _ := start(t, fakeprovider.WithRand(func() float64 { return tc.rand }))
			if got := send(t, nil, http.MethodPut, url+"/_control/faults", `{"rules":`+tc.rules+`}`); got.status != http.StatusNoContent {
				t.Fatalf("PUT faults: %d %v", got.status, got.body)
			}
			for i, ch := range tc.sends {
				got := send(t, tc.client, http.MethodPost, url, msg(ch))
				if (got.err != nil) != (tc.wantStatus[i] == 0) || (got.err == nil && got.status != tc.wantStatus[i]) {
					t.Fatalf("send %d: status %d err %v, want %d", i, got.status, got.err, tc.wantStatus[i])
				}
				if i == 0 && got.header.Get("Retry-After") != tc.wantRetryAfter {
					t.Errorf("Retry-After %q, want %q", got.header.Get("Retry-After"), tc.wantRetryAfter)
				}
			}
			total := 0
			for _, c := range s.Stats().Channels {
				total += c.Deliveries
			}
			if total != tc.wantDeliveries {
				t.Errorf("deliveries %d, want %d", total, tc.wantDeliveries)
			}
		})
	}
}

func TestControlAPIRejectsBadInputAndResets(t *testing.T) {
	cases := []struct {
		name, method, path, body string
		want                     int
	}{
		{"unknown mode", http.MethodPut, "/_control/faults", `{"rules":[{"mode":"explode"}]}`, 400},
		{"status out of range", http.MethodPut, "/_control/faults", `{"rules":[{"mode":"status","status":99}]}`, 400},
		{"count and ratio together", http.MethodPut, "/_control/faults", `{"rules":[{"mode":"drop","count":1,"ratio":0.5}]}`, 400},
		{"ratio above one", http.MethodPut, "/_control/faults", `{"rules":[{"mode":"drop","ratio":2}]}`, 400},
		{"unknown field", http.MethodPut, "/_control/faults", `{"rulez":[]}`, 400},
		{"dedupe without enabled", http.MethodPut, "/_control/dedupe", `{}`, 400},
		{"negative limit", http.MethodGet, "/_control/requests?limit=-1", "", 400},
		{"clear faults", http.MethodDelete, "/_control/faults", "", 204},
		{"health", http.MethodGet, "/healthz", "", 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, url, _ := start(t)
			if got := send(t, nil, tc.method, url+tc.path, tc.body); got.status != tc.want {
				t.Errorf("status %d, want %d (%v)", got.status, tc.want, got.body)
			}
		})
	}

	t.Run("reset clears history, counters, keys and faults", func(t *testing.T) {
		s, url, _ := start(t)
		send(t, nil, http.MethodPost, url, sms, "Idempotency-Key", "k")
		if err := s.SetFaults([]fakeprovider.Fault{{Mode: fakeprovider.ModeStatus, Status: 500}}); err != nil {
			t.Fatal(err)
		}
		send(t, nil, http.MethodPost, url+"/_control/reset", "")
		if got := send(t, nil, http.MethodPost, url, sms, "Idempotency-Key", "k"); got.status != http.StatusAccepted {
			t.Fatalf("after reset: %d", got.status)
		}
		st := s.Stats()
		if st.Channels["sms"].Received != 1 || st.Channels["sms"].Deliveries != 1 || st.DistinctKeys != 1 {
			t.Errorf("stats after reset %+v", st)
		}
	})

	t.Run("requests filter by channel and key, newest kept by limit", func(t *testing.T) {
		_, url, _ := start(t)
		for i, ch := range []string{"sms", "email", "sms", "sms"} {
			send(t, nil, http.MethodPost, url, msg(ch), "Idempotency-Key", fmt.Sprint("k", i))
		}
		var got struct{ Requests []fakeprovider.Request }
		getJSON(t, url+"/_control/requests?channel=sms&limit=2", &got)
		if len(got.Requests) != 2 || got.Requests[0].IdempotencyKey != "k2" || got.Requests[1].IdempotencyKey != "k3" {
			t.Errorf("limit/channel filter: %+v", got.Requests)
		}
		getJSON(t, url+"/_control/requests?key=k1", &got)
		if len(got.Requests) != 1 || got.Requests[0].Channel != "email" {
			t.Errorf("key filter: %+v", got.Requests)
		}
	})
}

func TestHistoryIsCappedWhileCountersStayExact(t *testing.T) {
	s, url, _ := start(t, fakeprovider.WithHistoryLimit(3))
	for i := range 5 {
		send(t, nil, http.MethodPost, url, sms, "Idempotency-Key", fmt.Sprint("k", i))
	}
	reqs := s.Requests(fakeprovider.Filter{})
	if len(reqs) != 3 || reqs[0].IdempotencyKey != "k2" || reqs[2].IdempotencyKey != "k4" {
		t.Errorf("history %+v, want k2..k4", reqs)
	}
	if st := s.Stats(); st.Channels["sms"].Deliveries != 5 || st.DistinctKeys != 5 {
		t.Errorf("counters %+v", st)
	}
}

// R11 under concurrency: many simultaneous retries of one key are one delivery.
func TestConcurrentRetriesOfOneKeyAreOneDelivery(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	s := fakeprovider.New()
	ts := httptest.NewServer(s.Handler())
	client := &http.Client{Transport: &http.Transport{}}
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for range 50 {
		wg.Go(func() {
			req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL, strings.NewReader(sms))
			req.Header.Set("Idempotency-Key", "same")
			res, err := client.Do(req)
			if err != nil {
				errs <- err
				return
			}
			_ = res.Body.Close()
			if res.StatusCode != http.StatusAccepted {
				errs <- errors.New(res.Status)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	client.CloseIdleConnections()
	ts.Close()
	if st := s.Stats(); st.Channels["sms"].Deliveries != 1 || st.Channels["sms"].Received != 50 || st.MaxDeliveriesPerKey != 1 {
		t.Errorf("stats %+v", st)
	}
}

func TestSetFaultsRejectsInvalidRulesWithSentinel(t *testing.T) {
	err := fakeprovider.New().SetFaults([]fakeprovider.Fault{{Mode: "nope"}})
	if !errors.Is(err, fakeprovider.ErrInvalidFault) {
		t.Errorf("err %v, want ErrInvalidFault", err)
	}
}

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if err := json.NewDecoder(res.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

// R8: the sender's rate is judged on requests; deliveries show what got through.
func TestRequestWindowCountsEveryValidRequestWhateverItsOutcome(t *testing.T) {
	cases := []struct {
		name                   string
		faults                 []fakeprovider.Fault
		sends                  []string // "channel:key"
		wantRequestsPerSecond  map[string]int
		wantDeliveriesPerSec   map[string]int
		wantMaxRequestsPerKey  int
		wantMaxDeliveriesByKey int
	}{
		{"dedupe hit raises requests, not deliveries", nil,
			[]string{"sms:k", "sms:k", "sms:k"}, map[string]int{"sms": 3}, map[string]int{"sms": 1}, 3, 1},
		{"injected 503 raises requests, not deliveries",
			[]fakeprovider.Fault{{Mode: fakeprovider.ModeStatus, Status: 503, Count: 2}},
			[]string{"sms:a", "sms:a", "sms:a"}, map[string]int{"sms": 3}, map[string]int{"sms": 1}, 3, 1},
		{"channels are counted separately", nil,
			[]string{"sms:", "email:", "sms:", "push:"}, map[string]int{"sms": 2, "email": 1, "push": 1}, map[string]int{"sms": 2, "email": 1, "push": 1}, 0, 0},
		{"invalid requests are not in the window", nil,
			[]string{"fax:x", "sms:x"}, map[string]int{"sms": 1}, map[string]int{"sms": 1}, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, url, _ := start(t)
			if err := s.SetFaults(tc.faults); err != nil {
				t.Fatal(err)
			}
			for _, sd := range tc.sends {
				ch, key, _ := strings.Cut(sd, ":")
				send(t, nil, http.MethodPost, url, msg(ch), "Idempotency-Key", key)
			}
			st := s.Stats()
			for ch, want := range tc.wantRequestsPerSecond {
				if got := st.Channels[ch]; got.MaxRequestsPerSecond != want || got.MaxDeliveriesPerSecond != tc.wantDeliveriesPerSec[ch] {
					t.Errorf("%s: requests/s %d deliveries/s %d, want %d and %d", ch, got.MaxRequestsPerSecond, got.MaxDeliveriesPerSecond, want, tc.wantDeliveriesPerSec[ch])
				}
			}
			if st.MaxRequestsPerKey != tc.wantMaxRequestsPerKey || st.MaxDeliveriesPerKey != tc.wantMaxDeliveriesByKey {
				t.Errorf("per key: requests %d deliveries %d", st.MaxRequestsPerKey, st.MaxDeliveriesPerKey)
			}
		})
	}
}

// B1: a request that ends without a response is recorded with status 0, and
// only accept_then_hang counts it as delivered (once).
func TestRequestsWithoutAResponseAreRecordedAsStatusZero(t *testing.T) {
	cases := []struct {
		name         string
		fault        fakeprovider.Fault
		wantDelivery bool
	}{
		{"drop", fakeprovider.Fault{Mode: fakeprovider.ModeDrop}, false},
		{"delay cancelled by the caller", fakeprovider.Fault{Mode: fakeprovider.ModeDelay, DelayMS: 60_000}, false},
		{"accept then hang, caller left", fakeprovider.Fault{Mode: fakeprovider.ModeAcceptThenHang, DelayMS: 60_000}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, url, _ := start(t)
			if err := s.SetFaults([]fakeprovider.Fault{tc.fault}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ended := make(chan error, 1)
			go func() { ended <- post(ctx, url) }()
			waitFor(t, func() bool { return s.Stats().Channels["sms"].Received == 1 })
			cancel() // no-op for drop, which has already ended
			select {
			case err := <-ended:
				if err == nil {
					t.Fatal("caller got a response, want none")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("request did not end after the caller left")
			}
			// The handler finishes after the caller sees the error; wait for its record.
			waitFor(t, func() bool { return len(s.Requests(fakeprovider.Filter{})) == 1 })
			rec := s.Requests(fakeprovider.Filter{})[0]
			st := s.Stats().Channels["sms"]
			if rec.Status != 0 || rec.Delivery != tc.wantDelivery || st.Responses[0] != 1 || len(st.Responses) != 1 {
				t.Errorf("record %+v, responses %v", rec, st.Responses)
			}
			if want := map[bool]int{true: 1, false: 0}[tc.wantDelivery]; st.Deliveries != want {
				t.Errorf("deliveries %d, want %d", st.Deliveries, want)
			}
		})
	}
}

func TestResponsesAreCountedByStatus(t *testing.T) {
	s, url, _ := start(t)
	if err := s.SetFaults([]fakeprovider.Fault{
		{Mode: fakeprovider.ModeStatus, Status: 503, Count: 1},
		{Mode: fakeprovider.ModeDrop, Count: 1},
	}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		_ = post(t.Context(), url)
	}
	want := map[int]int{503: 1, 0: 1, 202: 1}
	got := s.Stats().Channels["sms"].Responses
	if len(got) != len(want) || got[503] != 1 || got[0] != 1 || got[202] != 1 {
		t.Errorf("responses %v, want %v", got, want)
	}
}

// A request in flight across a Reset is answered but lands in no counter.
func TestResetDuringADelayDoesNotCountTheOldRequest(t *testing.T) {
	s, url, _ := start(t)
	if err := s.SetFaults([]fakeprovider.Fault{{Mode: fakeprovider.ModeDelay, DelayMS: 300, Count: 1}}); err != nil {
		t.Fatal(err)
	}
	done := make(chan sent, 1)
	go func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(sms))
		req.Header.Set("Idempotency-Key", "old")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- sent{err: err}
			return
		}
		_ = res.Body.Close()
		done <- sent{status: res.StatusCode}
	}()
	waitFor(t, func() bool { return s.Stats().Channels["sms"].Received == 1 })
	s.Reset()
	select {
	case got := <-done:
		if got.err != nil || got.status != http.StatusAccepted {
			t.Fatalf("delayed request: %d %v, want 202", got.status, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delayed request never answered")
	}
	if st := s.Stats(); len(st.Channels) != 0 || st.DistinctKeys != 0 || len(s.Requests(fakeprovider.Filter{})) != 0 {
		t.Errorf("post-reset state holds the old request: %+v", st)
	}
}

func post(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(sms))
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	return res.Body.Close()
}

// waitFor polls cond for up to 2 s: a bounded wait on an observable outcome,
// not a fixed sleep before an assertion.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A rule with To fails one recipient while the others on the same channel
// succeed: the message-specific failure of the provider health tests.
func TestFaultForOneRecipient(t *testing.T) {
	s, url, _ := start(t)
	if err := s.SetFaults([]fakeprovider.Fault{{Mode: fakeprovider.ModeStatus, Status: 503, To: "+15550000001"}}); err != nil {
		t.Fatal(err)
	}
	bad := `{"to":"+15550000001","channel":"sms","content":"x"}`
	good := `{"to":"+15550000002","channel":"sms","content":"x"}`
	for range 2 {
		if got := send(t, nil, http.MethodPost, url, bad).status; got != http.StatusServiceUnavailable {
			t.Errorf("matching recipient: status %d, want 503", got)
		}
		if got := send(t, nil, http.MethodPost, url, good).status; got != http.StatusAccepted {
			t.Errorf("other recipient: status %d, want 202", got)
		}
	}
}
