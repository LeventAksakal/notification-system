package fakeprovider_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

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
