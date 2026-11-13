package api

import (
	"bufio"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/obs"
)

// correlationHeader is accepted on every request and echoed on every response.
const correlationHeader = "X-Correlation-ID"

// correlationShape repeats the spec's pattern for the header.
var correlationShape = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

// withCorrelation takes the request's X-Correlation-ID, or generates one when
// it is absent or malformed, puts it in the context (so every log line of the
// request carries it), echoes it on the response before any handler writes,
// and logs one line per request.
func withCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationHeader)
		if len(id) > core.MaxCorrelationIDLen || !correlationShape.MatchString(id) {
			id = uuid.NewString()
		}
		ctx := obs.WithCorrelationID(r.Context(), id)
		w.Header().Set(correlationHeader, id)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(ctx))
		slog.InfoContext(ctx, "request", "method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration_ms", time.Since(start).Milliseconds())
	})
}

// statusRecorder remembers the status code for the request log line.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Hijack passes a WebSocket upgrade through to the underlying writer.
func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	s.status = http.StatusSwitchingProtocols
	return http.NewResponseController(s.ResponseWriter).Hijack()
}

// Flush passes flushes through to the underlying writer.
func (s *statusRecorder) Flush() { _ = http.NewResponseController(s.ResponseWriter).Flush() }
