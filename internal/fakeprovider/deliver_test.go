package fakeprovider_test

import (
	"fmt"
	"net/http"
	"testing"
)

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
