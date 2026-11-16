package fakeprovider

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Channels the provider accepts.
var validChannels = map[string]bool{"sms": true, "email": true, "push": true}

type payload struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type acceptedBody struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

var errTrailing = errors.New("trailing data after the JSON object")

// decodePayload reads exactly one JSON object. Unknown fields are accepted,
// as a real provider would ignore them; bytes after the object are not.
func decodePayload(w http.ResponseWriter, r *http.Request) (payload, error) {
	var in payload
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&in); err != nil {
		return in, err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return in, errTrailing
	}
	return in, nil
}

// deliver handles a provider request on any path. The request is counted on
// arrival; its response status and history entry are recorded when it ends.
func (s *Server) deliver(w http.ResponseWriter, r *http.Request) {
	rec := Request{
		Path:           r.URL.Path,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		CorrelationID:  r.Header.Get("X-Correlation-ID"),
		Traceparent:    r.Header.Get("traceparent"),
	}
	in, decodeErr := decodePayload(w, r)
	rec.To, rec.Channel, rec.Content = in.To, in.Channel, in.Content

	s.mu.Lock()
	epoch := s.epoch
	rec.ReceivedAt = s.now()
	msg := invalid(in, decodeErr)
	s.arriveLocked(rec, msg == "")
	if msg != "" {
		rec.Status = http.StatusBadRequest
		s.finishLocked(epoch, rec)
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, "invalid_request", msg)
		return
	}
	fault, hit := s.pickFaultLocked(in.Channel, in.To)
	switch {
	case !hit:
		body := s.acceptLocked(epoch, &rec)
		rec.Status = http.StatusAccepted
		s.finishLocked(epoch, rec)
		s.mu.Unlock()
		_ = writeJSON(w, http.StatusAccepted, body)
	case fault.Mode == ModeDelay:
		s.mu.Unlock()
		if !s.wait(r.Context(), fault.DelayMS) {
			s.finish(epoch, rec) // status 0: the caller left before we answered
			return
		}
		s.mu.Lock()
		body := s.acceptLocked(epoch, &rec)
		rec.Status = http.StatusAccepted
		s.finishLocked(epoch, rec)
		s.mu.Unlock()
		_ = writeJSON(w, http.StatusAccepted, body)
	case fault.Mode == ModeStatus:
		rec.Status = fault.Status
		s.finishLocked(epoch, rec)
		s.mu.Unlock()
		if fault.RetryAfterSeconds > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(fault.RetryAfterSeconds))
		}
		writeError(w, fault.Status, "injected_fault", "fault injected by the provider double")
	case fault.Mode == ModeAcceptThenHang:
		body := s.acceptLocked(epoch, &rec) // the delivery counts now, answered or not
		s.mu.Unlock()
		if s.wait(r.Context(), fault.DelayMS) && writeJSON(w, http.StatusAccepted, body) == nil {
			rec.Status = http.StatusAccepted
		}
		s.finish(epoch, rec) // status stays 0 if the caller left first
	default: // ModeDrop
		s.finishLocked(epoch, rec)
		s.mu.Unlock()
		// The server closes the connection without writing a response and
		// does not log this sentinel panic; it is net/http's "abort" signal.
		panic(http.ErrAbortHandler)
	}
}

func invalid(in payload, decodeErr error) string {
	switch {
	case errors.Is(decodeErr, errTrailing):
		return "body must be one JSON object with nothing after it"
	case decodeErr != nil:
		return "body is not valid JSON"
	case in.To == "" || in.Channel == "" || in.Content == "":
		return "to, channel and content are required"
	case !validChannels[in.Channel]:
		return "channel must be sms, email or push"
	}
	return ""
}

// wait blocks for ms milliseconds or until ctx is cancelled (the caller hung
// up or the server is shutting down); it reports whether the full time passed.
func (s *Server) wait(ctx context.Context, ms int) bool {
	t := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// newUUID returns a random (version 4) UUID.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails since Go 1.24
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
