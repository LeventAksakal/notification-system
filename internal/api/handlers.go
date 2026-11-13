package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	middleware "github.com/oapi-codegen/nethttp-middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/realtime"
)

// Check reports whether one dependency answers.
type Check func(ctx context.Context) error

// Server implements the generated StrictServerInterface and the WebSocket
// stream mounted beside it.
type Server struct {
	intake   *intake.Service
	checks   map[string]Check
	metrics  MetricsReader
	hub      *realtime.Hub
	wsBuffer int
	streams  *streams
}

var _ StrictServerInterface = (*Server)(nil)

// Options are the parts of the server beyond intake; a nil part answers 503
// (stream) or 500 (metrics).
type Options struct {
	Metrics MetricsReader
	// Hub fans status changes out to WebSocket subscribers.
	Hub *realtime.Hub
	// WSSendBuffer is how many events a subscriber may fall behind before it
	// is disconnected (APP_WS_SEND_BUFFER).
	WSSendBuffer int
}

// NewServer takes the intake service, the readiness checks by dependency
// name, and the optional parts.
func NewServer(svc *intake.Service, checks map[string]Check, opts ...Options) *Server {
	s := &Server{intake: svc, checks: checks, wsBuffer: 256, streams: newStreams()}
	for _, o := range opts {
		s.metrics, s.hub = o.Metrics, o.Hub
		if o.WSSendBuffer > 0 {
			s.wsBuffer = o.WSSendBuffer
		}
	}
	return s
}

// Handler returns the full HTTP stack: tracing, the correlation id, the body
// size limit, then the routes that live
// outside the contract (/openapi.json, /docs, the WebSocket), then spec
// validation (so a request the contract forbids never reaches a handler),
// then the generated handlers.
func (s *Server) Handler() (http.Handler, error) {
	spec, err := GetSwagger()
	if err != nil {
		return nil, fmt.Errorf("load embedded spec: %w", err)
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("encode embedded spec: %w", err)
	}
	spec.Servers = nil // validate paths without matching a host

	generated := http.NewServeMux()
	HandlerFromMux(NewStrictHandlerWithOptions(s, nil, StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  writeRequestError,
		ResponseErrorHandlerFunc: writeResponseError,
	}), generated)
	validated := middleware.OapiRequestValidatorWithOptions(spec, &middleware.Options{
		Options:              openapi3filter.Options{MultiError: true},
		ErrorHandlerWithOpts: writeValidationError,
	})(generated)

	mux := http.NewServeMux()
	mux.Handle("GET /openapi.json", specHandler(specJSON))
	mux.Handle("GET /docs", docsHandler())
	mux.HandleFunc("GET /v1/ws", s.streamEvents)
	mux.Handle("/", validated)
	return otelhttp.NewHandler(withCorrelation(limitBody(mux)), "http"), nil
}

func (s *Server) Health(context.Context, HealthRequestObject) (HealthResponseObject, error) {
	return Health200JSONResponse{Status: Ok, Checks: map[string]string{}}, nil
}

func (s *Server) Ready(ctx context.Context, _ ReadyRequestObject) (ReadyResponseObject, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	out := Health{Status: Ok, Checks: make(map[string]string, len(s.checks))}
	for name, check := range s.checks {
		if err := check(ctx); err != nil {
			slog.WarnContext(ctx, "readiness check failed", "dependency", name, "err", err)
			out.Status, out.Checks[name] = Unavailable, "down"
			continue
		}
		out.Checks[name] = "up"
	}
	if out.Status != Ok {
		return Ready503JSONResponse(out), nil
	}
	return Ready200JSONResponse(out), nil
}

// writeError writes the contract's Error body.
func writeError(w http.ResponseWriter, status int, body Error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeRequestError handles a body or parameter the generated binder could
// not decode (the validator normally refuses those first).
func writeRequestError(w http.ResponseWriter, _ *http.Request, err error) {
	if tooLarge(err) {
		writeTooLarge(w)
		return
	}
	writeError(w, http.StatusBadRequest, Error{Code: "invalid_request", Message: firstLine(err.Error())})
}

// writeResponseError turns a handler error into a 500 that does not leak
// the cause.
func writeResponseError(w http.ResponseWriter, r *http.Request, err error) {
	slog.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, Error{Code: "internal", Message: "internal error"})
}
