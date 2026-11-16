package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

func TestUnknownRouteIs404(t *testing.T) {
	rec := serve(t, nil, http.MethodGet, "/v1/nope", "")
	if rec.Code != http.StatusNotFound || decodeError(t, rec).Code != "not_found" {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
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
