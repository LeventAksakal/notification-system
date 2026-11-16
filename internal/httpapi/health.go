package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

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
