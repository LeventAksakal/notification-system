package fakeprovider

import (
	"encoding/json"
	"time"
)

// InvalidChannel is the stats bucket for requests whose channel is missing or unknown.
const InvalidChannel = "_invalid"

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
