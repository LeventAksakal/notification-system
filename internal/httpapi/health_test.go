package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestReadinessIs503WhenADependencyIsDown(t *testing.T) {
	down := map[string]Check{"mysql": func(context.Context) error { return errors.New("refused") }}
	if rec := serve(t, down, http.MethodGet, "/readyz", ""); rec.Code != 503 || !strings.Contains(rec.Body.String(), `"mysql":"down"`) {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}
