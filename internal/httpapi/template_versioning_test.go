package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAnIfMatchThatIsNotOneStrongVersionIs400NamingTheHeader(t *testing.T) {
	h, err := NewServer(nil, nil).Handler()
	if err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]string{
		"any (*)":             "*",
		"weak":                `W/"3"`,
		"unquoted":            "3",
		"a list":              `"3", "4"`,
		"version zero":        `"0"`,
		"leading zero":        `"03"`,
		"more than 10 digits": `"12345678901"`,
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/v1/templates/welcome",
				strings.NewReader(`{"body":"Hello {{name}}"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("If-Match", v)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			e := decodeError(t, rec)
			if rec.Code != http.StatusBadRequest || e.Code != "invalid_request" || e.Details == nil ||
				e.Details[0].Field != "If-Match" {
				t.Fatalf("got %d %.300s", rec.Code, rec.Body)
			}
		})
	}
}

func TestETagRoundTripsAVersion(t *testing.T) {
	for _, v := range []uint32{1, 42, 4294967295} {
		got, ok := parseETag(etagOf(v))
		if !ok || got != uint64(v) {
			t.Fatalf("version %d: parseETag(%s) = %d, %v", v, etagOf(v), got, ok)
		}
	}
	for _, bad := range []string{"", `""`, `"`, `"0"`, `"01"`, `"x"`, `W/"1"`, "1"} {
		if _, ok := parseETag(bad); ok {
			t.Errorf("parseETag(%q) accepted", bad)
		}
	}
}
