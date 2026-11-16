package fakeprovider_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

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

func TestSetFaultsRejectsInvalidRulesWithSentinel(t *testing.T) {
	err := fakeprovider.New().SetFaults([]fakeprovider.Fault{{Mode: "nope"}})
	if !errors.Is(err, fakeprovider.ErrInvalidFault) {
		t.Errorf("err %v, want ErrInvalidFault", err)
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
