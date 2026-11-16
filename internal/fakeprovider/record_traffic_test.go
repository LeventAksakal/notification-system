package fakeprovider_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

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
