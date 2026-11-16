package fakeprovider_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

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
