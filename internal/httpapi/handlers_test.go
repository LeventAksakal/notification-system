package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
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

func TestStreamAndMetricsRefuseBrokenParametersByName(t *testing.T) {
	for _, tc := range []struct{ path, field string }{
		{"/v1/ws?notificationId=nope", "notificationId"},
		{"/v1/ws?batchId=123", "batchId"},
		{"/v1/metrics?windowSeconds=0", "windowSeconds"},
		{"/v1/metrics?windowSeconds=3601", "windowSeconds"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			rec := serve(t, nil, http.MethodGet, tc.path, "")
			e := decodeError(t, rec)
			if rec.Code != http.StatusBadRequest || e.Code != "invalid_request" || e.Details == nil || e.Details[0].Field != tc.field {
				t.Fatalf("got %d %s", rec.Code, rec.Body)
			}
		})
	}
}

func TestStreamWithoutAHubIsUnavailable(t *testing.T) {
	if rec := serve(t, nil, http.MethodGet, "/v1/ws", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
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
				strings.Repeat("a", notification.MaxRequestBodyBytes) + `"}`
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
				e.Details[0].Field != "Idempotency-Key" {
				t.Fatalf("got %d %.300s", rec.Code, rec.Body)
			}
		})
	}
}

func TestMalformedParametersAre400WithTheErrorBody(t *testing.T) {
	for _, tc := range []struct{ method, path, field string }{
		{http.MethodGet, "/v1/notifications/not-a-uuid", "id"},
		{http.MethodPost, "/v1/notifications/not-a-uuid/cancel", "id"},
		{http.MethodGet, "/v1/batches/not-a-uuid", "id"},
		{http.MethodGet, "/v1/notifications?status=sent&status=failed", "status"},
		{http.MethodGet, "/v1/notifications?channel=sms&channel=push", "channel"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := serve(t, nil, tc.method, tc.path, "")
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type %q, want application/json", ct)
			}
			e := decodeError(t, rec)
			if rec.Code != http.StatusBadRequest || e.Code != "invalid_request" || e.Details == nil || e.Details[0].Field != tc.field {
				t.Fatalf("got %d %s", rec.Code, rec.Body)
			}
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

func TestServerSpansAreNamedByRoutePattern(t *testing.T) {
	mux, generated := http.NewServeMux(), http.NewServeMux()
	nop := func(http.ResponseWriter, *http.Request) {}
	mux.HandleFunc("GET /openapi.json", nop)
	mux.HandleFunc("/", nop)
	generated.HandleFunc("GET /v1/notifications/{id}", nop)
	name := spanName(mux, generated)
	for path, want := range map[string]string{
		"/v1/notifications/0190d6c6-1f43-7bf6-a7a4-6f3c2b1e9d10": "GET /v1/notifications/{id}",
		"/openapi.json": "GET /openapi.json",
		"/nope":         "GET unmatched",
	} {
		if got := name("http", httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)); got != want {
			t.Errorf("%s: span %q, want %q", path, got, want)
		}
	}
}

// Rules the spec-first validator (kin-openapi) enforced that Huma does not by
// default; each is set up in api.go and must keep holding.
func TestRequestsHumaWouldAcceptAreRefusedNamingTheField(t *testing.T) {
	const ok = `"to":"+905551234567","channel":"sms","content":"hi"`
	json := map[string]string{"Content-Type": "application/json"}
	for _, tc := range []struct {
		name, path, body string
		headers          map[string]string
		repeat           string // header sent twice
		field, reason    string // reason: substring, optional
	}{
		{"field name in another case", "/v1/notifications", `{"TO":"+905551234567","channel":"sms","content":"hi"}`, json, "", "to", ""},
		{"unknown field named by its object", "/v1/notifications", `{` + ok + `,"x":1}`, json, "", "body", `"x"`},
		{"unknown field whose name holds a dot", "/v1/notifications", `{` + ok + `,"a.b":1}`, json, "", "body", `"a.b"`},
		{"no Content-Type", "/v1/notifications", `{` + ok + `}`, nil, "", "body", ""},
		{"text/plain", "/v1/notifications", `{` + ok + `}`, map[string]string{"Content-Type": "text/plain"}, "", "body", ""},
		{"data after the JSON value", "/v1/notifications", `{` + ok + `} x`, json, "", "body", ""},
		{"null for an optional field", "/v1/notifications", `{` + ok + `,"priority":null}`, json, "", "priority", "null"},
		{"null inside the template", "/v1/notifications", `{"to":"+905551234567","channel":"sms","template":{"name":"t","variables":null}}`, json, "", "template.variables", "null"},
		{"null in a batch item", "/v1/notifications/batch", `{"notifications":[{` + ok + `,"scheduledAt":null}]}`, json, "", "scheduledAt", "null"},
		{"Idempotency-Key sent twice", "/v1/notifications", `{` + ok + `}`, json, "Idempotency-Key", "Idempotency-Key", "one value"},
		{"X-Correlation-ID sent twice", "/v1/notifications/batch", `{"notifications":[{` + ok + `}]}`, json, "X-Correlation-ID", "X-Correlation-ID", "one value"},
		{"null reported beside a missing field: the null", "/v1/notifications", `{"channel":"sms","content":"hi","priority":null}`, json, "", "priority", "null"},
		{"null reported beside a missing field: the missing one", "/v1/notifications", `{"channel":"sms","content":"hi","priority":null}`, json, "", "to", "missing"},
		{"empty cursor", "GET /v1/notifications?cursor=", "", nil, "", "cursor", ""},
		{"empty Idempotency-Key", "/v1/notifications", `{` + ok + `}`, map[string]string{"Content-Type": "application/json", "Idempotency-Key": ""}, "", "Idempotency-Key", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := NewServer(nil, nil).Handler()
			if err != nil {
				t.Fatal(err)
			}
			method, path, ok := strings.Cut(tc.path, " ")
			if !ok {
				method, path = http.MethodPost, tc.path
			}
			req := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(tc.body))
			for k, v := range tc.headers {
				req.Header[http.CanonicalHeaderKey(k)] = []string{v}
			}
			if tc.repeat != "" {
				req.Header.Add(tc.repeat, "key-1")
				req.Header.Add(tc.repeat, "key-2")
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			e := decodeError(t, rec)
			if rec.Code != http.StatusBadRequest || e.Code != "invalid_request" || len(e.Details) == 0 {
				t.Fatalf("got %d %.300s", rec.Code, rec.Body)
			}
			for _, d := range e.Details {
				if d.Field == tc.field && strings.Contains(d.Reason, tc.reason) {
					return
				}
			}
			t.Fatalf("no detail names %q (reason with %q): %.300s", tc.field, tc.reason, rec.Body)
		})
	}
}

// Huma's uuid format accepts a "urn:uuid:" prefix in any case; the standard
// library parses it only in lower case, so UUID parameters are read by
// parseUUID.
func TestEveryUUIDHumaAcceptsIsParsed(t *testing.T) {
	for _, s := range []string{
		"3f2504e0-4f89-11d3-9a0c-0305e82c3301",
		"3F2504E0-4F89-11D3-9A0C-0305E82C3301",
		"urn:uuid:3f2504e0-4f89-11d3-9a0c-0305e82c3301",
		"URN:UUID:3f2504e0-4f89-11d3-9a0c-0305e82c3301",
	} {
		var p uuidParam
		if err := p.UnmarshalText([]byte(s)); err != nil || p.String() != "3f2504e0-4f89-11d3-9a0c-0305e82c3301" {
			t.Errorf("%s: %v %v", s, p, err)
		}
	}
}

// api/openapi.yaml is generated from this package (decision 0024); a hand
// edit or a contract change without regenerating fails here before CI.
func TestCommittedSpecIsGenerated(t *testing.T) {
	want, err := SpecYAML()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, body, ok := strings.Cut(strings.ReplaceAll(string(got), "\r\n", "\n"), "\ncomponents:"); !ok ||
		"components:"+body != string(want) {
		t.Fatal("api/openapi.yaml differs from the server's code: run `go generate ./internal/httpapi` (never edit the file)")
	}
}
