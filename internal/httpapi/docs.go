package httpapi

import (
	_ "embed"
	"encoding/json"
	"html"
	"net/http"
)

// specHandler serves the embedded OpenAPI spec as JSON.
func specHandler(specJSON []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(specJSON)
	})
}

// scalarJS is the standalone browser bundle of @scalar/api-reference,
// vendored so /docs and /docs/websocket render with no third party at
// runtime. Since 1.75 it renders AsyncAPI 3.0 documents too.
//
//	version: 1.78.1 (npm dist/browser/standalone.js, 5000073 bytes)
//	sha256:  db9d00b7ee9259ef90554cbf8b6b0f671c9e0fe8540a33d845630fee68cbb966
//
// To upgrade: replace the file with another exact release, then update the
// version above, scalarVersion and scalarSHA256 (a test checks the hash).
//
//go:embed assets/scalar-api-reference.standalone.js
var scalarJS []byte

const (
	scalarVersion = "1.78.1"
	scalarSHA256  = "db9d00b7ee9259ef90554cbf8b6b0f671c9e0fe8540a33d845630fee68cbb966"
)

// scalarPath is where the bundle is served. renderDocsPage adds ?v=<version> so a
// browser holding the long-cached copy fetches the new file after an upgrade.
const scalarPath = "/docs/scalar.js"

// The reference renders two documents with the one embedded Scalar bundle,
// which reads AsyncAPI 3.0 as well as OpenAPI: the HTTP contract
// (/openapi.json) and the WebSocket stream (/asyncapi.yaml, decision 0027).
// Both pages list both documents, so Scalar's document selector links each
// reference to the other; they differ only in the one shown first. Scalar
// also accepts ?api=<slug> on either page.
const (
	httpDocsSlug      = "http"
	websocketDocsSlug = "websocket"
	websocketDocsPath = "/docs/websocket"
)

var (
	docsPage          = renderDocsPage(httpDocsSlug)
	websocketDocsPage = renderDocsPage(websocketDocsSlug)
)

// docsSource is one entry of Scalar's sources configuration.
type docsSource struct {
	Title   string `json:"title"`
	Slug    string `json:"slug"`
	URL     string `json:"url"`
	Default bool   `json:"default,omitempty"`
}

// renderDocsPage returns the reference page that opens on the document with
// slug first. The configuration turns off the fonts Scalar would load from
// fonts.scalar.com, and its telemetry, AI agent and MCP features, so the page
// needs no host other than this server; each source inherits it.
func renderDocsPage(first string) string {
	sources := []docsSource{
		{Title: "HTTP API", Slug: httpDocsSlug, URL: "/openapi.json"},
		{Title: "WebSocket stream", Slug: websocketDocsSlug, URL: "/asyncapi.yaml"},
	}
	for i := range sources {
		sources[i].Default = sources[i].Slug == first
	}
	config, err := json.Marshal(map[string]any{
		"sources":          sources,
		"withDefaultFonts": false,
		"telemetry":        false,
		"agent":            map[string]bool{"disabled": true},
		"mcp":              map[string]bool{"disabled": true},
		// Scalar's own toolbar (Configure, Share, Deploy API) is for editing
		// a spec in Scalar's cloud; on a page served by this server it only
		// distracts, so it is off everywhere, localhost included.
		"showDeveloperTools": "never",
	})
	if err != nil {
		panic(err) // fixed values of plain types always encode
	}
	return `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Notification system API</title>
</head>
<body>
<script id="api-reference" data-configuration="` + html.EscapeString(string(config)) + `"></script>
<script src="` + scalarPath + `?v=` + scalarVersion + `"></script>
</body>
</html>
`
}

// docsHandler serves a reference page built by renderDocsPage.
func docsHandler(page string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(page))
	})
}

// scalarHandler serves the vendored bundle. The file is pinned to one
// release, so clients may cache it for a long time.
func scalarHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		_, _ = w.Write(scalarJS)
	})
}
