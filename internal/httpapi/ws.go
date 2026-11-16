package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/realtime"
	"github.com/LeventAksakal/notification-system/internal/store"
)

const (
	// wsPingEvery keeps idle sockets (and the proxies in front of them) alive
	// and finds dead peers. It stays under the 60 s idle timeout common to
	// proxies and load balancers (nginx proxy_read_timeout, AWS ALB idle
	// timeout), with room for one lost ping: 2 × 20 s < 60 s.
	wsPingEvery = 20 * time.Second
	// wsWriteTimeout bounds one message or ping to a subscriber; a peer that
	// cannot take one small frame in that time is dropped. Derived from the
	// ping: half of wsPingEvery, so a stuck write is noticed before the next
	// ping is due. Arbitrary within that; to tune it, look at the write
	// durations of slow mobile clients and keep it above their p99.
	wsWriteTimeout = wsPingEvery / 2
)

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
// the snapshot already shows (status_seq at or below it) are dropped. A batch
// subscription reads every item's status_seq the same way, so its events are
// ordered exactly too (realtime.Reorderer).
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f realtime.Filter
	var details []ErrorDetail
	for _, p := range []struct {
		name string
		dst  *string
	}{{queryNotificationID, &f.NotificationID}, {queryBatchID, &f.BatchID}} {
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
		if errors.Is(err, realtime.ErrFeedLost) {
			writeError(w, errLiveUpdatesUnavailable())
			return
		}
		writeError(w, newError(http.StatusServiceUnavailable, "unavailable", "status stream unavailable"))
		return
	}
	defer sub.Close()
	order := realtime.NewReorderer()
	var snap *intake.Notification
	if f.NotificationID != "" {
		n, err := s.intake.Get(ctx, f.NotificationID)
		if err != nil {
			s.refuseLookup(w, r, err, "notification")
			return
		}
		snap = &n
		order.Seed(n.ID, n.StatusSeq)
	} else if f.BatchID != "" {
		seqs, err := s.intake.BatchStatusSeqs(ctx, f.BatchID)
		if err != nil {
			s.refuseLookup(w, r, err, "batch")
			return
		}
		for id, seq := range seqs {
			order.Seed(id, seq)
		}
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		slog.InfoContext(ctx, "websocket handshake failed", "err", err)
		return
	}
	s.pump(ctx, conn, sub, order, snap)
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

// pump writes the snapshot, then every event of sub in order, until the peer
// leaves, the subscription ends or the server shuts down.
func (s *Server) pump(ctx context.Context, conn *websocket.Conn, sub *realtime.Subscription, order *realtime.Reorderer, snap *intake.Notification) {
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
	}
	// writeAll writes events in order; false when the peer is gone.
	writeAll := func(es []notification.Event) bool {
		for _, e := range es {
			v, err := toStatusEvent(e)
			if err != nil {
				continue
			}
			if err := write(v); err != nil {
				_ = conn.CloseNow()
				return false
			}
		}
		return true
	}

	ping := time.NewTicker(wsPingEvery)
	defer ping.Stop()
	// hold fires when the reorderer has an event to give up waiting for.
	hold := time.NewTimer(time.Hour)
	hold.Stop()
	defer hold.Stop()
	rearm := func() {
		hold.Stop()
		if at, ok := order.Deadline(); ok {
			hold.Reset(max(time.Until(at), 0))
		}
	}
	for {
		select {
		case <-peerCtx.Done():
			_ = conn.CloseNow()
			return
		case <-s.streams.done:
			_ = conn.Close(closeShutdown.code, closeShutdown.reason)
			return
		case e, ok := <-sub.C:
			if !ok {
				switch err := sub.Err(); {
				case errors.Is(err, realtime.ErrSlow):
					_ = conn.Close(closeTooSlow.code, closeTooSlow.reason)
				case errors.Is(err, realtime.ErrFeedLost):
					// 1012: the server lost its event feed; changes may be
					// missing, so the stream ends instead of going on with a hole.
					_ = conn.Close(closeFeedLost.code, closeFeedLost.reason)
				default:
					_ = conn.Close(closeShutdown.code, closeShutdown.reason)
				}
				return
			}
			if !writeAll(order.Push(e, time.Now())) {
				return
			}
			rearm()
		case <-hold.C:
			if !writeAll(order.Expire(time.Now())) {
				return
			}
			rearm()
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
