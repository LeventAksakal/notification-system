package realtime

import (
	"container/list"
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
//
// Memory is capped (maxTracked): past the cap the oldest remembered
// notification is forgotten (its next claim is merely held), and a claim
// that would need a hold while the cap is reached goes out unheld.
type sequencer struct {
	hold time.Duration
	ttl  time.Duration
	max  int // cap on remembered and on held notifications
	// last is each remembered notification's last delivered state; lru
	// orders the same entries by receive time, oldest first, so expiry and
	// eviction look only at the front.
	last map[string]*list.Element // of *seen
	lru  *list.List
	// held are the claims waiting for their predecessor; holds lists them by
	// creation, which is deadline order (every hold is equally long).
	held  map[string]*heldQueue
	holds *list.List // of *heldQueue
}

type seen struct {
	id       string
	status   core.Status
	attempts int
	at       time.Time // local receive time, for expiry
}

type heldQueue struct {
	id    string
	msgs  []Message
	until time.Time
}

// maxTracked is the sequencer's default cap: notifications in flight at
// once on this hub is far below it at 100 sends per second per channel.
const maxTracked = 50_000

func newSequencer(hold, ttl time.Duration) *sequencer {
	return newCappedSequencer(hold, ttl, maxTracked)
}

func newCappedSequencer(hold, ttl time.Duration, maxEntries int) *sequencer {
	return &sequencer{hold: hold, ttl: ttl, max: max(maxEntries, 1),
		last: map[string]*list.Element{}, lru: list.New(), held: map[string]*heldQueue{}, holds: list.New()}
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
	// With as many holds as the cap allows, a claim goes out unheld: order is
	// best effort, memory is not.
	if m.Status == core.StatusProcessing && len(s.held) < s.max {
		if e, ok := s.last[id]; !ok || e.Value.(*seen).status != core.StatusPending || e.Value.(*seen).attempts != m.Attempts {
			q := &heldQueue{id: id, msgs: []Message{m}, until: now.Add(s.hold)}
			s.held[id] = q
			s.holds.PushBack(q)
			return nil
		}
	}
	return s.emit(nil, m, now)
}

// expire releases holds that ran out and forgets notifications not heard of
// for ttl. It looks only at the oldest entries: cost is what it removes.
func (s *sequencer) expire(now time.Time) []Message {
	var out []Message
	for f := s.holds.Front(); f != nil; f = s.holds.Front() {
		q := f.Value.(*heldQueue)
		if s.held[q.id] == q && now.Before(q.until) {
			break
		}
		s.holds.Remove(f)
		if s.held[q.id] != q {
			continue // released by its predecessor already
		}
		delete(s.held, q.id)
		for _, h := range q.msgs {
			out = s.emit(out, h, now)
		}
	}
	for f := s.lru.Front(); f != nil && now.Sub(f.Value.(*seen).at) > s.ttl; f = s.lru.Front() {
		s.forget(f)
	}
	return out
}

func (s *sequencer) emit(out []Message, m Message, now time.Time) []Message {
	id := m.NotificationID
	if e, ok := s.last[id]; ok {
		s.forget(e)
	}
	if !m.Status.Terminal() {
		s.last[id] = s.lru.PushBack(&seen{id: id, status: m.Status, attempts: m.Attempts, at: now})
		for len(s.last) > s.max {
			s.forget(s.lru.Front()) // evict the oldest; its next claim is merely held
		}
	}
	return append(out, m)
}

func (s *sequencer) forget(e *list.Element) {
	delete(s.last, e.Value.(*seen).id)
	s.lru.Remove(e)
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
