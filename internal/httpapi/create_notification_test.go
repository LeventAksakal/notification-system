package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAMalformedIdempotencyKeyIs400NamingTheHeader(t *testing.T) {
	h, err := NewServer(nil, nil).Handler()
	if err != nil {
		t.Fatal(err)
	}
	for name, key := range map[string]string{"non-ASCII": "clé", "space": "a key"} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/notifications",
				strings.NewReader(`{"to":"+905551234567","channel":"sms","content":"hi"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", key)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			e := decodeError(t, rec)
			if rec.Code != http.StatusBadRequest || e.Code != "invalid_request" || e.Details == nil ||
				e.Details[0].Field != "Idempotency-Key" {
				t.Fatalf("got %d %.300s", rec.Code, rec.Body)
			}
		})
	}
}
