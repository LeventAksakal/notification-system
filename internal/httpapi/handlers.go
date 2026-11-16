package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/realtime"
)

// Check reports whether one dependency answers.
type Check func(ctx context.Context) error

// Server holds the contract's handlers (api.go registers them with Huma) and
// the WebSocket stream mounted beside them.
type Server struct {
	intake   *intake.Service
	checks   map[string]Check
	metrics  MetricsReader
	hub      *realtime.Hub
	wsBuffer int
	streams  *streams
	// requestTimeout is every contract request's deadline but the health
	// endpoints' (withTimeout).
	requestTimeout time.Duration
}

// Options are the parts of the server beyond intake; a nil part answers 503
// (stream) or 500 (metrics).
type Options struct {
	Metrics MetricsReader
	// Hub fans status changes out to WebSocket subscribers.
	Hub *realtime.Hub
	// WSSendBuffer is how many events a subscriber may fall behind before it
	// is disconnected (APP_WS_SEND_BUFFER).
	WSSendBuffer int
	// RequestTimeout is the longest a request may take before it is
	// answered 503 (APP_REQUEST_TIMEOUT); 0 means DefaultRequestTimeout.
	RequestTimeout time.Duration
}

// NewServer takes the intake service, the readiness checks by dependency
// name, and the optional parts.
func NewServer(svc *intake.Service, checks map[string]Check, opts ...Options) *Server {
	s := &Server{intake: svc, checks: checks, wsBuffer: 1024 /* APP_WS_SEND_BUFFER's default */, streams: newStreams(), requestTimeout: DefaultRequestTimeout}
	for _, o := range opts {
		s.metrics, s.hub = o.Metrics, o.Hub
		if o.WSSendBuffer > 0 {
			s.wsBuffer = o.WSSendBuffer
		}
		if o.RequestTimeout > 0 {
			s.requestTimeout = o.RequestTimeout
		}
	}
	return s
}

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
	mux.HandleFunc("GET /v1/ws", s.streamEvents)
	mux.Handle("/", withTimeout(s.requestTimeout, contract))
	return otelhttp.NewHandler(withCorrelation(limitBody(mux)), "http",
		otelhttp.WithSpanNameFormatter(spanName(mux, contract))), nil
}

type healthOutput struct {
	Status int
	Body   Health
}

func (s *Server) Health(context.Context, *struct{}) (*healthOutput, error) {
	return &healthOutput{Status: http.StatusOK, Body: Health{Status: healthOK, Checks: map[string]string{}}}, nil
}

func (s *Server) Ready(ctx context.Context, _ *struct{}) (*healthOutput, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	out := &healthOutput{Status: http.StatusOK, Body: Health{Status: healthOK, Checks: make(map[string]string, len(s.checks))}}
	for name, check := range s.checks {
		if err := check(ctx); err != nil {
			slog.WarnContext(ctx, "readiness check failed", "dependency", name, "err", err)
			out.Status, out.Body.Status, out.Body.Checks[name] = http.StatusServiceUnavailable, healthUnavailable, "down"
			continue
		}
		out.Body.Checks[name] = "up"
	}
	return out, nil
}

// writeError writes the contract's Error body outside Huma (unknown route,
// body limit, request deadline, WebSocket refusals), with its length, so a
// client has the whole answer even while the server still holds the
// connection.
func writeError(w http.ResponseWriter, e *Error) {
	b, err := json.Marshal(e)
	if err != nil {
		b = []byte(`{"code":"internal","message":"internal error"}`)
	}
	b = append(b, '\n')
	for k, v := range e.headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(e.status)
	_, _ = w.Write(b)
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
