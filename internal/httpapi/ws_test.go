package httpapi

import (
	"net/http"
	"testing"
)

func TestStreamWithoutAHubIsUnavailable(t *testing.T) {
	if rec := serve(t, nil, http.MethodGet, "/v1/ws", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}
