package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/core"
)

func serve(t *testing.T, checks map[string]Check, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	h, err := NewServer(nil, checks).Handler()
	if err != nil {
		t.Fatal(err)
	}
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequestWithContext(t.Context(), method, path, r)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) Error {
	t.Helper()
	var e Error
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("body is not an Error: %v: %s", err, rec.Body)
	}
	return e
}

func TestReadinessIs503WhenADependencyIsDown(t *testing.T) {
	down := map[string]Check{"mysql": func(context.Context) error { return errors.New("refused") }}
	if rec := serve(t, down, http.MethodGet, "/readyz", ""); rec.Code != 503 || !strings.Contains(rec.Body.String(), `"mysql":"down"`) {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}

func TestOperationsNotBuiltYetAnswer501(t *testing.T) {
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/metrics", ""},
		{http.MethodGet, "/v1/ws", ""},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := serve(t, nil, tc.method, tc.path, tc.body)
			if rec.Code != http.StatusNotImplemented || decodeError(t, rec).Code != "not_implemented" {
				t.Fatalf("got %d %s", rec.Code, rec.Body)
			}
		})
	}
}

func TestSpecAndDocsAreServedOutsideTheContract(t *testing.T) {
	rec := serve(t, nil, http.MethodGet, "/openapi.json", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"openapi":"3.0.3"`) {
		t.Fatalf("/openapi.json: %d %.200s", rec.Code, rec.Body)
	}
	rec = serve(t, nil, http.MethodGet, "/docs", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "/openapi.json") {
		t.Fatalf("/docs: %d %.200s", rec.Code, rec.Body)
	}
}

func TestRefusedRequestsNameTheOffendingField(t *testing.T) {
	item := func(content string) string {
		return `{"to":"+905551234567","channel":"sms","content":"` + content + `"}`
	}
	batch := func(items ...string) string { return `{"notifications":[` + strings.Join(items, ",") + `]}` }
	over := strings.Repeat("x", core.MaxContentLen+1)
	many := make([]string, core.MaxBatchSize+1)
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
		{"batch item over its limit", http.MethodPost, "/v1/notifications/batch", batch(item("a"), item("b"), item("c"), item(over)),
			ErrorDetail{Field: "content", Index: &three}},
		{"batch over 1000 items", http.MethodPost, "/v1/notifications/batch", batch(many...),
			ErrorDetail{Field: "notifications"}},
		{"bad query parameter", http.MethodGet, "/v1/notifications?limit=0", "",
			ErrorDetail{Field: "limit"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := serve(t, nil, tc.method, tc.path, tc.body)
			e := decodeError(t, rec)
			if rec.Code != http.StatusBadRequest || e.Code != "invalid_request" || e.Message == "" || e.Details == nil {
				t.Fatalf("got %d %.300s", rec.Code, rec.Body)
			}
			for _, d := range *e.Details {
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

func TestUnknownRouteIs404(t *testing.T) {
	rec := serve(t, nil, http.MethodGet, "/v1/nope", "")
	if rec.Code != http.StatusNotFound || decodeError(t, rec).Code != "not_found" {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}

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

func TestABodyOverTheLimitIs413InTheErrorShape(t *testing.T) {
	for _, path := range []string{"/v1/notifications", "/v1/notifications/batch"} {
		t.Run(path, func(t *testing.T) {
			body := `{"to":"a@example.com","channel":"email","content":"` +
				strings.Repeat("a", core.MaxRequestBodyBytes) + `"}`
			rec := serve(t, nil, http.MethodPost, path, body)
			if rec.Code != http.StatusRequestEntityTooLarge || decodeError(t, rec).Code != "request_too_large" {
				t.Fatalf("got %d %.300s", rec.Code, rec.Body)
			}
		})
	}
}

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
				(*e.Details)[0].Field != "Idempotency-Key" {
				t.Fatalf("got %d %.300s", rec.Code, rec.Body)
			}
		})
	}
}
