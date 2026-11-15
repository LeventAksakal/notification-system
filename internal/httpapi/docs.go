package httpapi

import (
	_ "embed"
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
// vendored so /docs renders with no third party at runtime.
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

// scalarPath is where the bundle is served. docsPage adds ?v=<version> so a
// browser holding the long-cached copy fetches the new file after an upgrade.
const scalarPath = "/docs/scalar.js"

// docsPage renders /openapi.json with the embedded Scalar bundle. The
// configuration turns off the fonts Scalar would load from fonts.scalar.com,
// and its telemetry, AI agent and MCP features, so the page needs no host
// other than this server.
const docsPage = `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Notification system API</title>
</head>
<body>
<script id="api-reference" data-url="/openapi.json" data-configuration='{"withDefaultFonts":false,"telemetry":false,"agent":{"disabled":true},"mcp":{"disabled":true}}'></script>
<script src="` + scalarPath + `?v=` + scalarVersion + `"></script>
</body>
</html>
`

func docsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(docsPage))
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
