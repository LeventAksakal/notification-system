package realtime

import (
	"container/list"
	"slices"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// holdForPredecessor is how long an event waits for the events before it
// (lower Seq of the same notification) before it is passed on without them.
//
// Where it comes from: every change is published once, after its commit, by
// the process that made it. A worker publishes a result or a release only
// after the claim's own publish is over (it waits for that, up to
// publishTimeout), then spends up to publishTimeout on its own; the API
// reads the row back (milliseconds) and publishes within publishTimeout. A
// successor is committed after its predecessor, so a predecessor whose
// publish succeeded reaches Valkey within 2 × publishTimeout of the moment
// its successor can first arrive. A late predecessor never breaks the order:
// it arrives below the next expected Seq and is dropped. Holding longer only
// delays a stream whose predecessor was lost (Valkey trouble); holding
// shorter skips states whose publish was slow but succeeded.
const holdForPredecessor = 2 * publishTimeout

// forgetUnseededAfter is how long an unfiltered stream remembers a
// notification after its last event. A filtered stream remembers its
// notifications for its whole life (one, or the items of one batch), so
// this bounds only the unfiltered stream's memory: about 120 bytes per
// notification with an event in the window.
//
// Where it comes from: a late event is a publish whose bytes were still on
// their way when the publisher gave up waiting. TCP gives up retransmitting
// unacknowledged data after about 924.6 s with Linux's default
// tcp_retries2 = 15 (RFC 1122 asks for at least 100 s), and the connection
// is then dropped with those bytes; so no event lands later than that after
// it was written, unless Valkey itself stalls that long. Rounded up to 16
// minutes. To tune it, weigh memory per unfiltered socket (event rate ×
// window) against the rare very late event, and lower it together with the
// host's tcp_retries2.
const forgetUnseededAfter = 16 * time.Minute

// Reorderer puts one subscriber's events in Seq order per notification and
// drops stale and duplicate ones (decision 0013). It is owned by one
// goroutine (the subscriber's), which calls Expire at Deadline.
//
// A notification's base, the Seq it already has, comes from a snapshot read
// after the subscription was taken (Seed): then every later change reaches
// the subscriber in order, and a state is skipped only when its publish was
// lost or later than the hold. Seeded notifications are never forgotten, so a
// late event can never be shown after a newer one. A notification seen first
// in a live event (an unfiltered stream) has no base: that event is passed
// on at once and becomes it, so an earlier event still in flight is dropped
// when it arrives (the order holds; that state is not shown). It is
// forgotten forgetUnseededAfter its last event.
type Reorderer struct {
	hold   time.Duration
	forget time.Duration
	tracks map[string]*track
	// holding are the tracks with held events; few at a time (only while a
	// predecessor is in flight).
	holding map[string]*track
	// unseeded lists the unseeded tracks by their last event, oldest first.
	unseeded *list.List // of *track
}

type track struct {
	id     string
	next   uint64 // the Seq expected next; anything lower is stale
	held   []heldEvent
	last   time.Time     // last event, for forgetting an unseeded track
	lru    *list.Element // in Reorderer.unseeded; nil when seeded
	seeded bool
}

type heldEvent struct {
	notification.Event
	at time.Time // when it arrived
}

// NewReorderer returns an empty Reorderer that holds an event at most
// holdForPredecessor for its predecessors.
func NewReorderer() *Reorderer { return newReorderer(holdForPredecessor, forgetUnseededAfter) }

func newReorderer(hold, forget time.Duration) *Reorderer {
	return &Reorderer{hold: hold, forget: forget, tracks: map[string]*track{}, holding: map[string]*track{}, unseeded: list.New()}
}

// Seed records that the subscriber has notification id at seq (a snapshot):
// events at or below it are skipped, seq+1 is passed on, later ones wait for
// their predecessors. Seed before the first Push.
func (r *Reorderer) Seed(id string, seq uint64) {
	if t, ok := r.tracks[id]; ok && t.lru != nil {
		r.unseeded.Remove(t.lru)
	}
	r.tracks[id] = &track{id: id, next: seq + 1, seeded: true}
}

// Push takes one event and returns what may be passed on now, in order.
func (r *Reorderer) Push(e notification.Event, now time.Time) []notification.Event {
	if e.Seq == 0 {
		return []notification.Event{e} // unsequenced publisher: pass on as it came
	}
	t, ok := r.tracks[e.NotificationID]
	if !ok {
		t = &track{id: e.NotificationID, next: e.Seq}
		t.lru = r.unseeded.PushBack(t)
		r.tracks[e.NotificationID] = t
	}
	if t.lru != nil {
		t.last = now
		r.unseeded.MoveToBack(t.lru)
	}
	switch {
	case e.Seq < t.next:
		return nil // stale or duplicate: the subscriber has this state or a later one
	case e.Seq == t.next:
		t.next++
		return r.drain([]notification.Event{e}, t)
	}
	i, found := slices.BinarySearchFunc(t.held, e.Seq, func(h heldEvent, s uint64) int { return cmpSeq(h.Seq, s) })
	if found {
		return nil // duplicate of a held event
	}
	t.held = slices.Insert(t.held, i, heldEvent{Event: e, at: now})
	r.holding[t.id] = t
	return nil
}

// Expire passes on what has waited hold for a predecessor, skipping the gap
// before it, and forgets unseeded notifications quiet for the forget window.
// Call it at Deadline.
func (r *Reorderer) Expire(now time.Time) []notification.Event {
	var out []notification.Event
	for _, t := range r.holding {
		for len(t.held) > 0 && !now.Before(oldest(t.held).Add(r.hold)) {
			t.next = t.held[0].Seq // the gap before it is given up
			out = r.drain(out, t)
		}
	}
	for f := r.unseeded.Front(); f != nil; f = r.unseeded.Front() {
		t := f.Value.(*track)
		if now.Sub(t.last) < r.forget || len(t.held) > 0 {
			break
		}
		r.unseeded.Remove(f)
		delete(r.tracks, t.id)
	}
	return out
}

// Deadline is when Expire next has work: the earliest hold to run out or
// unseeded notification to forget. ok is false when there is none.
func (r *Reorderer) Deadline() (at time.Time, ok bool) {
	if f := r.unseeded.Front(); f != nil {
		at, ok = f.Value.(*track).last.Add(r.forget), true
	}
	for _, t := range r.holding {
		if d := oldest(t.held).Add(r.hold); !ok || d.Before(at) {
			at, ok = d, true
		}
	}
	return at, ok
}

// drain passes on the held events that now follow without a gap.
func (r *Reorderer) drain(out []notification.Event, t *track) []notification.Event {
	for len(t.held) > 0 && t.held[0].Seq <= t.next {
		h := t.held[0]
		t.held = t.held[1:]
		if h.Seq == t.next {
			out = append(out, h.Event)
			t.next++
		}
	}
	if len(t.held) == 0 {
		delete(r.holding, t.id)
	}
	return out
}

func oldest(hs []heldEvent) time.Time {
	at := hs[0].at
	for _, h := range hs[1:] {
		if h.at.Before(at) {
			at = h.at
		}
	}
	return at
}

func cmpSeq(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
