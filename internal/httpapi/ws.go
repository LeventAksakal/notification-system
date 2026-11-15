package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
	"uuid"

	"github.com/coder/websocket"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/realtime"
	"github.com/LeventAksakal/notification-system/internal/store"
)

const (
	// wsPingEvery keeps idle sockets (and the proxies in front of them) alive
	// and finds dead peers.
	wsPingEvery = 20 * time.Second
	// wsWriteTimeout bounds one message or ping to a subscriber.
	wsWriteTimeout = 10 * time.Second
)

// toStatusEvent maps a status change to the contract's StatusEvent
// (contract.go; documented in the spec beside GET /v1/ws). An id that is not a UUID
// fails; the stream skips that event rather than sending a zero id.
func toStatusEvent(e notification.Event) (StatusEvent, error) {
	id, err := uuid.Parse(e.NotificationID)
	if err != nil {
		return StatusEvent{}, fmt.Errorf("notification id %q: %w", e.NotificationID, err)
	}
	v := StatusEvent{NotificationID: id, Status: Status(e.Status), Attempts: e.Attempts,
		At: e.At.UTC(), CorrelationID: e.CorrelationID}
	if e.BatchID != "" {
		bid, err := uuid.Parse(e.BatchID)
		if err != nil {
			return StatusEvent{}, fmt.Errorf("batch id %q: %w", e.BatchID, err)
		}
		v.BatchID = &bid
	}
	if e.Error != "" {
		v.Error = &e.Error
	}
	return v, nil
}

// streams tracks open WebSocket handlers so shutdown can end them and wait.
// Hijacked connections are invisible to http.Server.Shutdown.
type streams struct {
	mu      sync.Mutex
	closing bool
	done    chan struct{}
	wg      sync.WaitGroup
}

func newStreams() *streams { return &streams{done: make(chan struct{})} }

func (st *streams) begin() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closing {
		return false
	}
	st.wg.Add(1)
	return true
}

// CloseStreams tells every open WebSocket to close (1001 going away) and
// waits until each handler has returned. New upgrades are refused.
func (s *Server) CloseStreams() {
	st := s.streams
	st.mu.Lock()
	if !st.closing {
		st.closing = true
		close(st.done)
	}
	st.mu.Unlock()
	st.wg.Wait()
}

// streamEvents is GET /v1/ws (operation streamEvents). A WebSocket upgrade
// does not fit a Huma operation, so it is mounted beside them in Handler and
// documented in the spec by documentStream (api.go).
//
// No change committed after the snapshot is lost: the hub subscription is
// taken before the snapshot is read, and the events buffered meanwhile that
// the snapshot already shows are dropped (realtime.Covered).
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f realtime.Filter
	var details []ErrorDetail
	for _, p := range []struct {
		name string
		dst  *string
	}{{"notificationId", &f.NotificationID}, {"batchId", &f.BatchID}} {
		name, dst := p.name, p.dst
		v := q.Get(name)
		if v == "" {
			continue
		}
		id, err := parseUUID(v)
		if err != nil {
			details = append(details, ErrorDetail{Field: name, Reason: "must be a UUID"})
			continue
		}
		*dst = id.String()
	}
	if len(details) > 0 {
		e := invalid(details)
		e.Message = details[0].Field + ": " + details[0].Reason
		writeError(w, e)
		return
	}
	if s.hub == nil || !s.streams.begin() {
		writeError(w, newError(http.StatusServiceUnavailable, "unavailable", "status stream unavailable"))
		return
	}
	defer s.streams.wg.Done()

	ctx := r.Context()
	if f.BatchID != "" {
		if _, err := s.intake.GetBatch(ctx, f.BatchID, 1, ""); err != nil {
			s.refuseLookup(w, r, err, "batch")
			return
		}
	}
	sub, err := s.hub.Subscribe(f, s.wsBuffer)
	if err != nil {
		msg := "status stream unavailable"
		if errors.Is(err, realtime.ErrFeedLost) {
			msg = "status stream unavailable: event feed lost; retry shortly"
		}
		writeError(w, newError(http.StatusServiceUnavailable, "unavailable", msg))
		return
	}
	defer sub.Close()
	var snap *intake.Notification
	if f.NotificationID != "" {
		n, err := s.intake.Get(ctx, f.NotificationID)
		if err != nil {
			s.refuseLookup(w, r, err, "notification")
			return
		}
		snap = &n
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		slog.InfoContext(ctx, "websocket handshake failed", "err", err)
		return
	}
	s.pump(ctx, conn, sub, snap)
}

func (s *Server) refuseLookup(w http.ResponseWriter, r *http.Request, err error, what string) {
	if errors.Is(err, intake.ErrNotFound) {
		writeError(w, notFound(what))
		return
	}
	if store.IsUnavailable(err) {
		slog.WarnContext(r.Context(), "database unreachable; answered 503", "method", r.Method, "path", r.URL.Path, "err", err)
		writeError(w, errUnavailable())
		return
	}
	slog.ErrorContext(r.Context(), "websocket lookup failed", "what", what, "err", err)
	writeError(w, errInternal())
}

// pump writes the snapshot, then every event of sub, until the peer leaves,
// the subscription ends or the server shuts down.
func (s *Server) pump(ctx context.Context, conn *websocket.Conn, sub *realtime.Subscription, snap *intake.Notification) {
	// Nothing is read from the client; CloseRead answers its pings and close
	// frame and cancels peerCtx when the peer goes away.
	peerCtx := conn.CloseRead(ctx)
	write := func(v StatusEvent) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		wctx, cancel := context.WithTimeout(peerCtx, wsWriteTimeout)
		defer cancel()
		return conn.Write(wctx, websocket.MessageText, b)
	}

	var covers *notification.Event // the snapshot, until an event goes past it
	if snap != nil {
		e := snapshotEvent(*snap)
		v, err := toStatusEvent(e)
		snapshot := true
		v.Status, v.Snapshot = Status(snap.Status()), &snapshot
		if err == nil {
			err = write(v)
		}
		if err != nil {
			_ = conn.CloseNow()
			return
		}
		covers = &e
	}

	ping := time.NewTicker(wsPingEvery)
	defer ping.Stop()
	for {
		select {
		case <-peerCtx.Done():
			_ = conn.CloseNow()
			return
		case <-s.streams.done:
			_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
			return
		case e, ok := <-sub.C:
			if !ok {
				switch err := sub.Err(); {
				case errors.Is(err, realtime.ErrSlow):
					_ = conn.Close(websocket.StatusPolicyViolation, "too slow: events were dropped; resubscribe")
				case errors.Is(err, realtime.ErrFeedLost):
					// 1012: the server lost its event feed; changes may be
					// missing, so the stream ends instead of going on with a hole.
					_ = conn.Close(websocket.StatusServiceRestart, "event feed lost; reconnect and read the snapshot")
				default:
					_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
				}
				return
			}
			if covers != nil {
				if realtime.Covered(*covers, e) {
					continue
				}
				covers = nil
			}
			v, err := toStatusEvent(e)
			if err != nil {
				continue
			}
			if err := write(v); err != nil {
				_ = conn.CloseNow()
				return
			}
		case <-ping.C:
			pctx, cancel := context.WithTimeout(peerCtx, wsWriteTimeout)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				_ = conn.CloseNow()
				return
			}
		}
	}
}

// snapshotEvent is the stored state as an event (stored status; the API view
// is applied when it is written).
func snapshotEvent(n intake.Notification) notification.Event {
	return notification.Event{
		NotificationID: n.ID, BatchID: n.BatchID.String, Status: notification.Status(n.Notification.Status),
		Attempts: int(n.Attempts), Error: n.LastError.String, At: n.UpdatedAt, CorrelationID: n.CorrelationID,
	}
}
