package httpapi

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

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
