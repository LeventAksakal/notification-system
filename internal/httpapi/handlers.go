package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	middleware "github.com/oapi-codegen/nethttp-middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/realtime"
	"github.com/LeventAksakal/notification-system/internal/store"
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
	// requestTimeout is every contract request's deadline but the health
	// endpoints' (withTimeout).
	requestTimeout time.Duration
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
	// RequestTimeout is the longest a request may take before it is
	// answered 503 (APP_REQUEST_TIMEOUT); 0 means DefaultRequestTimeout.
	RequestTimeout time.Duration
}

// NewServer takes the intake service, the readiness checks by dependency
// name, and the optional parts.
func NewServer(svc *intake.Service, checks map[string]Check, opts ...Options) *Server {
	s := &Server{intake: svc, checks: checks, wsBuffer: 256, streams: newStreams(), requestTimeout: DefaultRequestTimeout}
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
	HandlerWithOptions(NewStrictHandlerWithOptions(s, nil, StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  writeRequestError,
		ResponseErrorHandlerFunc: writeResponseError,
	}), StdHTTPServerOptions{BaseRouter: generated, ErrorHandlerFunc: writeParamError})
	validated := middleware.OapiRequestValidatorWithOptions(spec, &middleware.Options{
		Options:              openapi3filter.Options{MultiError: true},
		ErrorHandlerWithOpts: writeValidationError,
	})(generated)

	mux := http.NewServeMux()
	mux.Handle("GET /openapi.json", specHandler(specJSON))
	mux.Handle("GET /docs", docsHandler())
	mux.Handle("GET "+scalarPath, scalarHandler())
	mux.HandleFunc("GET /v1/ws", s.streamEvents)
	mux.Handle("/", withTimeout(s.requestTimeout, validated))
	return otelhttp.NewHandler(withCorrelation(limitBody(mux)), "http",
		otelhttp.WithSpanNameFormatter(spanName(mux, generated))), nil
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

// writeError writes the contract's Error body, with its length, so a client
// has the whole answer even while the server still holds the connection.
func writeError(w http.ResponseWriter, status int, body Error) {
	b, err := json.Marshal(body)
	if err != nil {
		b = []byte(`{"code":"internal","message":"internal error"}`)
	}
	b = append(b, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	_, _ = w.Write(b)
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

// writeResponseError turns a handler error into 503 unavailable when the
// database cannot be reached, else into a 500 that does not leak the cause.
func writeResponseError(w http.ResponseWriter, r *http.Request, err error) {
	if store.IsUnavailable(err) {
		writeUnavailable(w, r, err)
		return
	}
	slog.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, Error{Code: "internal", Message: "internal error"})
}

// retryAfterSeconds is the Retry-After of a 503 unavailable.
const retryAfterSeconds = 1

// writeUnavailable answers 503 unavailable: the database could not be
// reached, or did not answer in time (api/openapi.yaml, ServiceUnavailable).
// The request may or may not have been stored: a connection lost during a
// commit the server did apply looks the same. The client repeats it with
// the same Idempotency-Key. The cause is logged once here, at WARN: the
// outage is an operating condition, and the readiness probe reports it too.
// Driver errors carry the address, never the DSN's credentials.
func writeUnavailable(w http.ResponseWriter, r *http.Request, err error) {
	slog.WarnContext(r.Context(), "database unreachable; answered 503", "method", r.Method, "path", r.URL.Path, "err", err)
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	writeError(w, http.StatusServiceUnavailable, Error{Code: "unavailable",
		Message: "the database cannot be reached; the request may or may not have been stored; repeat it with the same Idempotency-Key"})
}

// writeParamError handles a path, query or header parameter the generated
// wrapper could not bind (a malformed id, a single-value parameter given
// twice): 400 invalid_request naming the parameter, like every refusal.
func writeParamError(w http.ResponseWriter, _ *http.Request, err error) {
	name, reason := paramOf(err)
	body := Error{Code: "invalid_request", Message: firstLine(err.Error())}
	if name != "" {
		body.Message = name + ": " + reason
		body.Details = &[]ErrorDetail{{Field: name, Reason: reason}}
	}
	writeError(w, http.StatusBadRequest, body)
}

// paramOf names the parameter of a generated binding error and says what is
// wrong with it.
func paramOf(err error) (name, reason string) {
	var (
		format   *InvalidParamFormatError
		tooMany  *TooManyValuesForParamError
		required *RequiredParamError
		header   *RequiredHeaderError
		unmarsh  *UnmarshalingParamError
		cookie   *UnescapedCookieParamError
	)
	switch {
	case errors.As(err, &format):
		return format.ParamName, "malformed value"
	case errors.As(err, &tooMany):
		return tooMany.ParamName, fmt.Sprintf("expected one value, got %d", tooMany.Count)
	case errors.As(err, &required):
		return required.ParamName, "required"
	case errors.As(err, &header):
		return header.ParamName, "required"
	case errors.As(err, &unmarsh):
		return unmarsh.ParamName, "malformed value"
	case errors.As(err, &cookie):
		return cookie.ParamName, "malformed value"
	}
	return "", ""
}

// spanName names a server span by method and matched route pattern, never
// the raw path, so span names stay low-cardinality. Routes of the contract
// are looked up in generated; the others in mux.
func spanName(mux, generated *http.ServeMux) func(string, *http.Request) string {
	return func(_ string, r *http.Request) string {
		_, pattern := mux.Handler(r)
		if pattern == "/" {
			_, pattern = generated.Handler(r)
		}
		if pattern == "" {
			return r.Method + " unmatched"
		}
		if _, path, ok := strings.Cut(pattern, " "); ok {
			pattern = path
		}
		return r.Method + " " + pattern
	}
}
