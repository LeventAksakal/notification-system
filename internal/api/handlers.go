package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	middleware "github.com/oapi-codegen/nethttp-middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// Check reports whether one dependency answers.
type Check func(ctx context.Context) error

// Server implements the generated StrictServerInterface.
type Server struct {
	q      sqlc.Querier
	checks map[string]Check
}

var _ StrictServerInterface = (*Server)(nil)

// NewServer takes the queries and the readiness checks by dependency name.
func NewServer(q sqlc.Querier, checks map[string]Check) *Server {
	return &Server{q: q, checks: checks}
}

// Handler returns the full HTTP stack: tracing, then spec validation (so a
// request the contract forbids never reaches a handler), then the handlers.
func (s *Server) Handler() (http.Handler, error) {
	spec, err := GetSwagger()
	if err != nil {
		return nil, fmt.Errorf("load embedded spec: %w", err)
	}
	spec.Servers = nil // validate paths without matching a host
	mux := http.NewServeMux()
	HandlerFromMux(NewStrictHandler(s, nil), mux)
	validated := middleware.OapiRequestValidatorWithOptions(spec, &middleware.Options{
		ErrorHandlerWithOpts: writeValidationError,
	})(mux)
	return otelhttp.NewHandler(validated, "http"), nil
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

func (s *Server) GetMeta(ctx context.Context, req GetMetaRequestObject) (GetMetaResponseObject, error) {
	m, err := s.q.GetMeta(ctx, req.Key)
	if errors.Is(err, sql.ErrNoRows) {
		return GetMeta404JSONResponse{Code: "not_found", Message: "no such key"}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get meta: %w", err)
	}
	return GetMeta200JSONResponse{Key: m.K, Value: m.V, UpdatedAt: m.UpdatedAt}, nil
}

func writeValidationError(_ context.Context, err error, w http.ResponseWriter, _ *http.Request, opts middleware.ErrorHandlerOpts) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(opts.StatusCode)
	msg, _, _ := strings.Cut(err.Error(), "\n") // the rest echoes the schema and the value
	_, _ = fmt.Fprintf(w, `{"code":"invalid_request","message":%q}`, msg)
}
