package realtime

import (
	"time"

	"github.com/LeventAksakal/notification-system/internal/core"
)

// sequencer restores commit order per notification where two processes race
// to publish (decision 0013).
//
// Publishes of one process for one notification already arrive in commit
// order: each is synchronous, after commit, in the goroutine that committed.
// The race is between processes: the API commits a create (pending) and
// publishes it, while a worker can claim the row within milliseconds and
// publish processing first. The same holds for a reschedule or release
// (published by one worker) and the next claim (by another).
//
// Every claim follows a pending with the same attempts count, so a
// processing(a) whose notification was last seen at anything but pending(a)
// has its predecessor still in flight. It is held, with whatever follows it,
// until that pending arrives (then the pending goes first) or for at most
// hold (then everything held goes out as it came; the predecessor was
// published before this hub subscribed, or was lost).
//
// A pending(a) arriving while processing(a) is held is its predecessor when
// it comes from the API (create, replay) or carries an error (a reschedule:
// a claim never follows it with the same attempts). A release or reap
// (worker, no error) can be either; it counts as the predecessor when its
// time is not after the held claim's.
type sequencer struct {
	hold time.Duration
	ttl  time.Duration
	last map[string]seen
	held map[string]*heldQueue
}

type seen struct {
	status   core.Status
	attempts int
	at       time.Time // local receive time, for expiry
}

type heldQueue struct {
	msgs  []Message
	until time.Time
}

func newSequencer(hold, ttl time.Duration) *sequencer {
	return &sequencer{hold: hold, ttl: ttl, last: map[string]seen{}, held: map[string]*heldQueue{}}
}

// push takes one message and returns what may be delivered now, in order.
func (s *sequencer) push(m Message, now time.Time) []Message {
	id := m.NotificationID
	if q, ok := s.held[id]; ok {
		if !predecessor(m, q.msgs[0]) {
			q.msgs = append(q.msgs, m)
			return nil
		}
		delete(s.held, id)
		out := s.emit(nil, m, now)
		for _, h := range q.msgs {
			out = s.emit(out, h, now)
		}
		return out
	}
	if m.Status == core.StatusProcessing {
		if l, ok := s.last[id]; !ok || l.status != core.StatusPending || l.attempts != m.Attempts {
			s.held[id] = &heldQueue{msgs: []Message{m}, until: now.Add(s.hold)}
			return nil
		}
	}
	return s.emit(nil, m, now)
}

// expire releases holds that ran out and forgets notifications not heard of
// for ttl.
func (s *sequencer) expire(now time.Time) []Message {
	var out []Message
	for id, q := range s.held {
		if now.Before(q.until) {
			continue
		}
		delete(s.held, id)
		for _, h := range q.msgs {
			out = s.emit(out, h, now)
		}
	}
	for id, l := range s.last {
		if now.Sub(l.at) > s.ttl {
			delete(s.last, id)
		}
	}
	return out
}

func (s *sequencer) emit(out []Message, m Message, now time.Time) []Message {
	if m.Status.Terminal() {
		delete(s.last, m.NotificationID)
	} else {
		s.last[m.NotificationID] = seen{status: m.Status, attempts: m.Attempts, at: now}
	}
	return append(out, m)
}

// predecessor reports whether p is the pending that the held claim c follows.
func predecessor(p, c Message) bool {
	if p.Status != core.StatusPending || p.Attempts != c.Attempts {
		return false
	}
	return p.Origin == OriginAPI || p.Error != "" || !p.At.After(c.At)
}

// Covered reports whether e, received on a subscription opened before
// snapshot was read, is already reflected in the snapshot (the stored state
// of the same notification). Within one attempt budget a notification moves
// forward in (attempts, phase) order: pending, processing, then a finished
// state or a pending with one more attempt. A replay starts over at
// pending with attempts 0 and is never covered by a failed snapshot.
//
// Only events up to the first uncovered one are checked; after that the
// stream is in commit order and everything is new.
func Covered(snapshot, e core.Event) bool {
	if e.Status == snapshot.Status && e.Attempts == snapshot.Attempts {
		return true
	}
	if snapshot.Status == core.StatusFailed && e.Status == core.StatusPending && e.Attempts == 0 {
		return false
	}
	if e.Attempts != snapshot.Attempts {
		return e.Attempts < snapshot.Attempts
	}
	return phase(e.Status) < phase(snapshot.Status)
}

func phase(s core.Status) int {
	switch s {
	case core.StatusPending:
		return 0
	case core.StatusProcessing:
		return 1
	default:
		return 2
	}
}
