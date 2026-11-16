package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func TestEveryResponseCarriesTheCorrelationID(t *testing.T) {
	h, err := NewServer(nil, nil).Handler()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, header string
		want         func(string) bool
	}{
		{"given id is echoed", "abc-123:x.y_z", func(got string) bool { return got == "abc-123:x.y_z" }},
		{"absent id is generated", "", func(got string) bool { return len(got) == 36 }},
		{"malformed id is replaced", "has space", func(got string) bool { return got != "has space" && len(got) == 36 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// An unknown route: the header is set even on an error response.
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/nope", nil)
			if tc.header != "" {
				req.Header.Set("X-Correlation-ID", tc.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if got := rec.Header().Get("X-Correlation-ID"); !tc.want(got) {
				t.Fatalf("X-Correlation-ID = %q (status %d)", got, rec.Code)
			}
		})
	}
}

// The create operations declare X-Correlation-ID in the spec, so a value past
// MaxCorrelationIDLen (a W3C traceparent) is refused there naming the header;
// other routes replace it (TestEveryResponseCarriesTheCorrelationID).
func TestACorrelationIDPastTheLimitIsRefusedOnCreate(t *testing.T) {
	h, err := NewServer(nil, nil).Handler()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/notifications", "/v1/notifications/batch"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(`{}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Correlation-ID", strings.Repeat("a", notification.MaxCorrelationIDLen+1))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			e := decodeError(t, rec)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("got %d %.300s", rec.Code, rec.Body)
			}
			for _, d := range e.Details {
				if d.Field == "X-Correlation-ID" {
					return
				}
			}
			t.Fatalf("no detail names X-Correlation-ID: %.300s", rec.Body)
		})
	}
}
