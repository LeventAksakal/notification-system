package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// ErrNotImplemented marks an operation whose slice has not landed; the strict
// handler's error func turns it into 501 not_implemented. A slice that builds
// an operation deletes its method here and implements it in its own file.
var ErrNotImplemented = errors.New("not implemented")

func notImplemented(op string) error { return fmt.Errorf("%s: %w", op, ErrNotImplemented) }

func (s *Server) GetMetrics(context.Context, GetMetricsRequestObject) (GetMetricsResponseObject, error) {
	return nil, notImplemented("getMetrics")
}

// streamEvents is GET /v1/ws (operation streamEvents). It is excluded from the
// generated server (a WebSocket upgrade does not fit the strict interface) and
// mounted outside the validated routes in Handler.
func (s *Server) streamEvents(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, Error{Code: "not_implemented", Message: notImplemented("streamEvents").Error()})
}
