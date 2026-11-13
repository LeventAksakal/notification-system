// Package fakeprovider is a local stand-in for the external notification
// provider. It accepts the provider's request shape, records every request,
// can deduplicate by Idempotency-Key, injects scripted faults, and counts
// exactly what was delivered so tests can assert on it.
package fakeprovider

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// DefaultHistoryLimit caps the stored request history; counters stay exact.
const DefaultHistoryLimit = 100_000

// Channels the provider accepts.
var validChannels = map[string]bool{"sms": true, "email": true, "push": true}

// InvalidChannel is the stats bucket for requests whose channel is missing or unknown.
const InvalidChannel = "_invalid"

// Mode is what a fault rule does to a matching request.
type Mode string

// Fault modes.
const (
	ModeStatus         Mode = "status"           // respond with Status (and Retry-After if set)
	ModeDelay          Mode = "delay"            // wait DelayMS, then respond normally
	ModeAcceptThenHang Mode = "accept_then_hang" // count the delivery, then hold the response DelayMS
	ModeDrop           Mode = "drop"             // close the connection without a response
)

// ErrInvalidFault is returned by SetFaults for a rule that cannot apply.
var ErrInvalidFault = errors.New("invalid fault rule")

// Fault is one scripted failure. Count > 0 applies it to the next Count
// matching requests; Ratio in (0,1] applies it to that share of them; neither
// applies it until cleared. A ratio rule that does not fire falls through to
// the next rule.
type Fault struct {
	Channel           string  `json:"channel,omitempty"`
	Mode              Mode    `json:"mode"`
	Status            int     `json:"status,omitempty"`
	RetryAfterSeconds int     `json:"retryAfterSeconds,omitempty"`
	DelayMS           int     `json:"delayMs,omitempty"`
	Count             int     `json:"count,omitempty"`
	Ratio             float64 `json:"ratio,omitempty"`
}

func (f Fault) validate() error {
	if f.Channel != "" && !validChannels[f.Channel] {
		return fmt.Errorf("%w: unknown channel %q", ErrInvalidFault, f.Channel)
	}
	switch f.Mode {
	case ModeStatus:
		if f.Status < 200 || f.Status > 599 {
			return fmt.Errorf("%w: status %d outside 200..599", ErrInvalidFault, f.Status)
		}
	case ModeDelay, ModeAcceptThenHang, ModeDrop:
	default:
		return fmt.Errorf("%w: unknown mode %q", ErrInvalidFault, f.Mode)
	}
	switch {
	case f.Count < 0 || f.DelayMS < 0 || f.RetryAfterSeconds < 0:
		return fmt.Errorf("%w: count, delayMs and retryAfterSeconds must be >= 0", ErrInvalidFault)
	case f.Ratio < 0 || f.Ratio > 1:
		return fmt.Errorf("%w: ratio %v outside 0..1", ErrInvalidFault, f.Ratio)
	case f.Count > 0 && f.Ratio > 0:
		return fmt.Errorf("%w: set count or ratio, not both", ErrInvalidFault)
	}
	return nil
}

// Request is one recorded request. Status 0 means no response was sent
// (dropped, or the caller went away during a delay).
type Request struct {
	ReceivedAt     time.Time `json:"receivedAt"`
	Path           string    `json:"path"`
	To             string    `json:"to"`
	Channel        string    `json:"channel"`
	Content        string    `json:"content"`
	IdempotencyKey string    `json:"idempotencyKey,omitempty"`
	CorrelationID  string    `json:"correlationId,omitempty"`
	Traceparent    string    `json:"traceparent,omitempty"`
	Status         int       `json:"status"`
	Delivery       bool      `json:"delivery"`
}

// Filter selects recorded requests. Limit > 0 keeps the most recent Limit.
type Filter struct {
	Channel string
	Key     string
	Limit   int
}

// ChannelStats counts one channel's traffic.
//
// The sender's rate limit is judged on requests (MaxRequestsPerSecond: every
// valid request by receive time, whatever its outcome); deliveries show what
// got through. Both maxima are the busiest sliding window [t, t+1s).
type ChannelStats struct {
	Received               int         `json:"received"`
	Deliveries             int         `json:"deliveries"`
	MaxRequestsPerSecond   int         `json:"maxRequestsPerSecond"`
	MaxDeliveriesPerSecond int         `json:"maxDeliveriesPerSecond"`
	Responses              map[int]int `json:"responses"` // by status; 0 = no response sent
}

// Stats is a snapshot of everything counted since start or the last Reset.
//
// With dedupe on, MaxDeliveriesPerKey is at most 1 by construction, so "sent
// once" is asserted on MaxRequestsPerKey, or with dedupe off.
type Stats struct {
	Dedupe                    bool                    `json:"dedupe"`
	Channels                  map[string]ChannelStats `json:"channels"`
	DistinctKeys              int                     `json:"distinctKeys"`
	MaxRequestsPerKey         int                     `json:"maxRequestsPerKey"`
	MaxDeliveriesPerKey       int                     `json:"maxDeliveriesPerKey"`
	KeysDeliveredMoreThanOnce int                     `json:"keysDeliveredMoreThanOnce"`
}

// Option configures a Server.
type Option func(*Server)

// WithClock sets the time source; it must not go backwards.
func WithClock(now func() time.Time) Option { return func(s *Server) { s.now = now } }

// WithDedupe sets the initial (and post-Reset) dedupe switch. Default on.
func WithDedupe(on bool) Option { return func(s *Server) { s.dedupeDefault = on } }

// WithHistoryLimit caps the stored request history (oldest dropped).
func WithHistoryLimit(n int) Option { return func(s *Server) { s.historyLimit = max(n, 1) } }

// WithRand sets the [0,1) source used for ratio rules.
func WithRand(f func() float64) Option { return func(s *Server) { s.rand = f } }

// Server is the provider double. Safe for concurrent use.
type Server struct {
	now           func() time.Time
	rand          func() float64
	dedupeDefault bool
	historyLimit  int

	// One mutex guards everything below: Go maps are not safe for concurrent
	// writes, and handlers run on one goroutine per request.
	mu       sync.Mutex
	epoch    uint64 // bumped by Reset; requests begun in an older epoch are not counted
	dedupe   bool
	faults   []Fault
	history  ring
	channels map[string]*channelState
	keys     map[string]*keyState
}

type channelState struct {
	received, deliveries      int
	responses                 map[int]int
	requestWin, deliveriesWin window
}

type keyState struct {
	body                 []byte // first accepted response; nil until one is accepted
	requests, deliveries int
}

// New returns a Server with dedupe on, no faults and an empty history.
func New(opts ...Option) *Server {
	s := &Server{
		now:           time.Now,
		rand:          mrand.Float64, //nolint:gosec // fault sampling, not security
		dedupeDefault: true,
		historyLimit:  DefaultHistoryLimit,
	}
	for _, o := range opts {
		o(s)
	}
	s.Reset()
	return s
}

// Reset clears history, counters, remembered keys and faults, and restores
// the dedupe switch to its configured default.
func (s *Server) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	s.dedupe = s.dedupeDefault
	s.faults = nil
	s.history = ring{limit: s.historyLimit}
	s.channels = map[string]*channelState{}
	s.keys = map[string]*keyState{}
}

// SetFaults replaces every fault rule; nil or empty clears them.
func (s *Server) SetFaults(rules []Fault) error {
	for i, f := range rules {
		if err := f.validate(); err != nil {
			return fmt.Errorf("rule %d: %w", i, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = append([]Fault(nil), rules...) // copy: the caller keeps its slice
	return nil
}

// SetDedupe turns Idempotency-Key deduplication on or off.
func (s *Server) SetDedupe(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dedupe = on
}

// Requests returns recorded requests matching f, oldest first.
func (s *Server) Requests(f Filter) []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Request
	s.history.each(func(r Request) {
		if (f.Channel == "" || r.Channel == f.Channel) && (f.Key == "" || r.IdempotencyKey == f.Key) {
			out = append(out, r)
		}
	})
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[len(out)-f.Limit:]
	}
	return out
}

// Stats returns a snapshot of the counters.
func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{Dedupe: s.dedupe, Channels: map[string]ChannelStats{}, DistinctKeys: len(s.keys)}
	for name, c := range s.channels {
		resp := make(map[int]int, len(c.responses))
		for code, n := range c.responses {
			resp[code] = n
		}
		st.Channels[name] = ChannelStats{
			Received: c.received, Deliveries: c.deliveries,
			MaxRequestsPerSecond: c.requestWin.max, MaxDeliveriesPerSecond: c.deliveriesWin.max,
			Responses: resp,
		}
	}
	for _, k := range s.keys {
		st.MaxRequestsPerKey = max(st.MaxRequestsPerKey, k.requests)
		st.MaxDeliveriesPerKey = max(st.MaxDeliveriesPerKey, k.deliveries)
		if k.deliveries > 1 {
			st.KeysDeliveredMoreThanOnce++
		}
	}
	return st
}

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
	fault, hit := s.pickFaultLocked(in.Channel)
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

// pickFaultLocked returns the first rule that fires for channel, spending one
// of its Count if it has one. Caller holds s.mu.
func (s *Server) pickFaultLocked(channel string) (Fault, bool) {
	for i := range s.faults {
		f := &s.faults[i] // pointer into the slice so the Count decrement sticks
		if f.Channel != "" && f.Channel != channel {
			continue
		}
		switch {
		case f.Count > 0:
			f.Count--
			got := *f
			if f.Count == 0 {
				s.faults = append(s.faults[:i:i], s.faults[i+1:]...) // spent: remove
			}
			return got, true
		case f.Ratio > 0:
			if s.rand() < f.Ratio {
				return *f, true
			}
		default:
			return *f, true
		}
	}
	return Fault{}, false
}

// arriveLocked counts a request on arrival: received, and for a valid one the
// request-rate window and its key's request count. Caller holds s.mu.
func (s *Server) arriveLocked(rec Request, valid bool) {
	c := s.channelLocked(rec.Channel)
	c.received++
	if !valid {
		return
	}
	c.requestWin.add(rec.ReceivedAt)
	if rec.IdempotencyKey != "" {
		s.keyLocked(rec.IdempotencyKey).requests++
	}
}

// acceptLocked returns the 202 body, counting a delivery unless dedupe
// recognises the key or a Reset happened since the request arrived (epoch).
// Caller holds s.mu.
func (s *Server) acceptLocked(epoch uint64, rec *Request) []byte {
	key := rec.IdempotencyKey
	if k := s.keys[key]; key != "" && s.dedupe && k != nil && k.body != nil && epoch == s.epoch {
		return k.body
	}
	now := s.now()
	body, _ := json.Marshal(acceptedBody{newUUID(), "accepted", now.UTC().Format(time.RFC3339)}) // cannot fail: plain strings
	if epoch != s.epoch {
		return body // answered, but it belongs to counters that Reset discarded
	}
	if key != "" {
		k := s.keyLocked(key)
		if k.body == nil {
			k.body = body
		}
		k.deliveries++
	}
	rec.Delivery = true
	c := s.channelLocked(rec.Channel)
	c.deliveries++
	c.deliveriesWin.add(now)
	return body
}

func (s *Server) finish(epoch uint64, rec Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishLocked(epoch, rec)
}

// finishLocked records rec's outcome (history and responses by status) unless
// a Reset happened since it arrived. Caller holds s.mu.
func (s *Server) finishLocked(epoch uint64, rec Request) {
	if epoch != s.epoch {
		return
	}
	s.channelLocked(rec.Channel).responses[rec.Status]++
	s.history.push(rec)
}

func (s *Server) channelLocked(name string) *channelState {
	if !validChannels[name] {
		name = InvalidChannel
	}
	c := s.channels[name]
	if c == nil {
		c = &channelState{responses: map[int]int{}}
		s.channels[name] = c
	}
	return c
}

func (s *Server) keyLocked(key string) *keyState {
	k := s.keys[key]
	if k == nil {
		k = &keyState{}
		s.keys[key] = k
	}
	return k
}

// window tracks the most events (requests or deliveries) in any 1-second span [t, t+1s).
// It keeps only timestamps from the last second, so memory follows the peak
// rate, not the total.
type window struct {
	recent []time.Time
	max    int
}

func (w *window) add(t time.Time) {
	i := 0
	for i < len(w.recent) && t.Sub(w.recent[i]) >= time.Second {
		i++
	}
	w.recent = append(w.recent[i:], t)
	w.max = max(w.max, len(w.recent))
}

// ring is a bounded FIFO: once full, each push overwrites the oldest entry.
type ring struct {
	buf   []Request
	start int
	limit int
}

func (r *ring) push(x Request) {
	if len(r.buf) < r.limit {
		r.buf = append(r.buf, x)
		return
	}
	r.buf[r.start] = x
	r.start = (r.start + 1) % r.limit
}

func (r *ring) each(fn func(Request)) {
	for i := range len(r.buf) {
		fn(r.buf[(r.start+i)%len(r.buf)])
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
