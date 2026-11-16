package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
)

func TestRefusedRequestsNameTheOffendingField(t *testing.T) {
	item := func(content string) string {
		return `{"to":"+905551234567","channel":"sms","content":"` + content + `"}`
	}
	batch := func(items ...string) string { return `{"notifications":[` + strings.Join(items, ",") + `]}` }
	over := strings.Repeat("x", notification.MaxContentLen+1)
	many := make([]string, notification.MaxBatchSize+1)
	for i := range many {
		many[i] = item("hi")
	}
	three := 3

	for _, tc := range []struct {
		name, method, path, body string
		want                     ErrorDetail
	}{
		{"missing field", http.MethodPost, "/v1/notifications", `{"channel":"sms","content":"hi"}`,
			ErrorDetail{Field: "to"}},
		{"field over its limit", http.MethodPost, "/v1/notifications", item(over),
			ErrorDetail{Field: "content"}},
		{"content whitespace only", http.MethodPost, "/v1/notifications", item(` \t\u3000`),
			ErrorDetail{Field: "content"}},
		{"to whitespace only", http.MethodPost, "/v1/notifications", `{"to":" \u00a0","channel":"push","content":"hi"}`,
			ErrorDetail{Field: "to"}},
		{"template body whitespace only", http.MethodPut, "/v1/templates/welcome", `{"body":"\n\u2028 "}`,
			ErrorDetail{Field: "body"}},
		{"batch item over its limit", http.MethodPost, "/v1/notifications/batch", batch(item("a"), item("b"), item("c"), item(over)),
			ErrorDetail{Field: "content", Index: &three}},
		{"batch over 1000 items", http.MethodPost, "/v1/notifications/batch", batch(many...),
			ErrorDetail{Field: "notifications"}},
		{"bad query parameter", http.MethodGet, "/v1/notifications?limit=0", "",
			ErrorDetail{Field: "limit"}},
		{"reschedule without scheduledAt", http.MethodPatch, "/v1/notifications/01a1e115-35f4-713d-8d33-fbf2f9af0c3c", `{}`,
			ErrorDetail{Field: "scheduledAt"}},
		{"reschedule to a non-time", http.MethodPatch, "/v1/notifications/01a1e115-35f4-713d-8d33-fbf2f9af0c3c", `{"scheduledAt":"tomorrow"}`,
			ErrorDetail{Field: "scheduledAt"}},
		{"reschedule with an unknown field", http.MethodPatch, "/v1/notifications/01a1e115-35f4-713d-8d33-fbf2f9af0c3c", `{"scheduledAt":null,"dueAt":null}`,
			ErrorDetail{Field: "body"}},
		{"reschedule of a malformed id", http.MethodPatch, "/v1/notifications/not-a-uuid", `{"scheduledAt":null}`,
			ErrorDetail{Field: "id"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := serve(t, nil, tc.method, tc.path, tc.body)
			e := decodeError(t, rec)
			if rec.Code != http.StatusBadRequest || e.Code != "invalid_request" || e.Message == "" || e.Details == nil {
				t.Fatalf("got %d %.300s", rec.Code, rec.Body)
			}
			for _, d := range e.Details {
				if d.Field == tc.want.Field && d.Reason != "" &&
					(tc.want.Index == nil) == (d.Index == nil) &&
					(tc.want.Index == nil || *tc.want.Index == *d.Index) {
					return
				}
			}
			t.Fatalf("no detail like %+v in %.300s", tc.want, rec.Body)
		})
	}
}

type failingMetrics struct{ err error }

func (f failingMetrics) Read(context.Context, time.Duration) (store.Metrics, error) {
	return store.Metrics{}, f.err
}

func TestAnUnreachableDatabaseIs503WithRetryAfter(t *testing.T) {
	refused := fmt.Errorf("metrics: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")})
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"a refused connection is unavailable", refused, http.StatusServiceUnavailable, "unavailable"},
		{"any other failure stays internal", errors.New("boom"), http.StatusInternalServerError, "internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := NewServer(nil, nil, Options{Metrics: failingMetrics{tc.err}}).Handler()
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/metrics", nil))
			if e := decodeError(t, rec); rec.Code != tc.status || e.Code != tc.code {
				t.Fatalf("got %d %s", rec.Code, rec.Body)
			}
			ra := rec.Header().Get("Retry-After")
			if tc.status == http.StatusServiceUnavailable && ra != "1" {
				t.Errorf("Retry-After %q, want 1", ra)
			}
			if tc.status != http.StatusServiceUnavailable && ra != "" {
				t.Errorf("Retry-After %q on a %d", ra, tc.status)
			}
		})
	}
}
