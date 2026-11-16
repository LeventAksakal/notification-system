package httpapi

import (
	"log/slog"
	"net/http"
	"sync"
)

// asyncAPIYAML builds the AsyncAPI document once, on the first request for
// it; the document depends only on the code, so it never changes after.
var asyncAPIYAML = sync.OnceValues(AsyncAPIYAML)

// asyncAPIHandler serves the AsyncAPI document of the WebSocket stream (GET
// /asyncapi.yaml), beside /openapi.json and outside the contract. A document
// that cannot be built is a bug a test catches; were it to happen, the answer
// is 500 in the Error shape.
func asyncAPIHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		doc, err := asyncAPIYAML()
		if err != nil {
			slog.ErrorContext(r.Context(), "build the AsyncAPI document", "err", err)
			writeError(w, errInternal())
			return
		}
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(doc)
	})
}
