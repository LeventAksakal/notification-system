package httpapi

import (
	"fmt"
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Handler returns the full HTTP stack: tracing, the correlation id, the body
// size limit, then the routes that live outside the contract (/openapi.json,
// /docs, the WebSocket), then, under the request deadline, the contract's
// operations, which Huma validates before a handler runs.
func (s *Server) Handler() (http.Handler, error) {
	contract := http.NewServeMux()
	api := s.newAPI(contract)
	contract.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, newError(http.StatusNotFound, "not_found", "no such route"))
	})
	spec, err := api.OpenAPI().Downgrade()
	if err != nil {
		return nil, fmt.Errorf("encode the OpenAPI document: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /openapi.json", specHandler(spec))
	mux.Handle("GET /docs", docsHandler())
	mux.Handle("GET "+scalarPath, scalarHandler())
	mux.Handle("GET /asyncapi.yaml", asyncAPIHandler())
	mux.HandleFunc("GET /v1/ws", s.streamEvents)
	mux.Handle("/", withTimeout(s.requestTimeout, contract))
	return otelhttp.NewHandler(withCorrelation(limitBody(mux)), "http",
		otelhttp.WithSpanNameFormatter(spanName(mux, contract))), nil
}

// spanName names a server span by method and matched route pattern, never
// the raw path, so span names stay low-cardinality. Routes of the contract
// are looked up in contract; the others in mux.
func spanName(mux, contract *http.ServeMux) func(string, *http.Request) string {
	return func(_ string, r *http.Request) string {
		_, pattern := mux.Handler(r)
		if pattern == "/" {
			_, pattern = contract.Handler(r)
		}
		if pattern == "" || pattern == "/" {
			return r.Method + " unmatched"
		}
		if _, path, ok := strings.Cut(pattern, " "); ok {
			pattern = path
		}
		return r.Method + " " + pattern
	}
}
