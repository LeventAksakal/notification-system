package httpapi

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
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
