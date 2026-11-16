package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"html"
	"mime"
	"net/http"
	"regexp"
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

var (
	scalarConfigAttr = regexp.MustCompile(`<script id="api-reference" data-configuration="([^"]*)"`)
	scriptSrc        = regexp.MustCompile(`<script src="([^"]+)"`)
)

// TestBothReferencesRenderBothDocuments follows what each reference page
// points at: the documents in its Scalar configuration and the bundle it
// loads must all be served here, the one it opens on must be its own, and
// each page must offer the other document, so the two link to each other.
func TestBothReferencesRenderBothDocuments(t *testing.T) {
	for page, first := range map[string]string{
		"/docs":           "/openapi.json",
		"/docs/websocket": "/asyncapi.yaml",
	} {
		t.Run(page, func(t *testing.T) {
			rec := serve(t, nil, http.MethodGet, page, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: %d", page, rec.Code)
			}
			if mt, _, err := mime.ParseMediaType(rec.Header().Get("Content-Type")); err != nil || mt != "text/html" {
				t.Fatalf("%s Content-Type %q", page, rec.Header().Get("Content-Type"))
			}
			body := rec.Body.String()
			if strings.Contains(body, "://") {
				t.Fatalf("%s references another host: %s", page, body)
			}

			m := scalarConfigAttr.FindStringSubmatch(body)
			if m == nil {
				t.Fatalf("%s has no Scalar configuration: %s", page, body)
			}
			var config struct {
				Sources []struct {
					URL     string `json:"url"`
					Default bool   `json:"default"`
				} `json:"sources"`
			}
			if err := json.Unmarshal([]byte(html.UnescapeString(m[1])), &config); err != nil {
				t.Fatalf("%s configuration: %v", page, err)
			}
			var defaults []string
			urls := map[string]bool{}
			for _, s := range config.Sources {
				urls[s.URL] = true
				if s.Default {
					defaults = append(defaults, s.URL)
				}
			}
			if !urls["/openapi.json"] || !urls["/asyncapi.yaml"] {
				t.Fatalf("%s does not offer both documents: %+v", page, config.Sources)
			}
			if len(defaults) != 1 || defaults[0] != first {
				t.Fatalf("%s opens on %v, want %s", page, defaults, first)
			}
			for u := range urls {
				if rec := serve(t, nil, http.MethodGet, u, ""); rec.Code != http.StatusOK || rec.Body.Len() == 0 {
					t.Fatalf("%s source %s: %d, %d bytes", page, u, rec.Code, rec.Body.Len())
				}
			}

			src := scriptSrc.FindStringSubmatch(body)
			if src == nil {
				t.Fatalf("%s loads no script: %s", page, body)
			}
			rec = serve(t, nil, http.MethodGet, src[1], "")
			if mt, _, err := mime.ParseMediaType(rec.Header().Get("Content-Type")); err != nil || mt != "text/javascript" {
				t.Fatalf("%s Content-Type %q", src[1], rec.Header().Get("Content-Type"))
			}
			if sum := sha256.Sum256(rec.Body.Bytes()); rec.Code != http.StatusOK || hex.EncodeToString(sum[:]) != scalarSHA256 {
				t.Fatalf("%s: %d, not the pinned bundle", src[1], rec.Code)
			}
		})
	}
}
