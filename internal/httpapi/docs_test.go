package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"mime"
	"net/http"
	"strings"
	"testing"
)

func TestDocsRenderWithTheEmbeddedScalarBundle(t *testing.T) {
	rec := serve(t, nil, http.MethodGet, "/docs", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `src="/docs/scalar.js`) {
		t.Fatalf("/docs does not load the local bundle: %d %.300s", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), "://") {
		t.Fatalf("/docs references another host: %s", rec.Body)
	}

	rec = serve(t, nil, http.MethodGet, "/docs/scalar.js", "")
	if rec.Code != http.StatusOK || rec.Body.Len() == 0 {
		t.Fatalf("/docs/scalar.js: %d, %d bytes", rec.Code, rec.Body.Len())
	}
	if mt, _, err := mime.ParseMediaType(rec.Header().Get("Content-Type")); err != nil || mt != "text/javascript" {
		t.Fatalf("/docs/scalar.js Content-Type %q", rec.Header().Get("Content-Type"))
	}
}

func TestServedScalarBundleIsThePinnedRelease(t *testing.T) {
	rec := serve(t, nil, http.MethodGet, "/docs/scalar.js", "")
	sum := sha256.Sum256(rec.Body.Bytes())
	if got := hex.EncodeToString(sum[:]); got != scalarSHA256 {
		t.Fatalf("served bundle sha256 %s, recorded %s: update the version and hash in docs.go", got, scalarSHA256)
	}
}
