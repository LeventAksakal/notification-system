package httpapi

import (
	"context"
	"time"

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
