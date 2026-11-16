package realtime

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

const (
	testHold   = time.Second
	testForget = time.Minute
)

var t0 = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func ev(id string, seq uint64, st notification.Status) notification.Event {
	return notification.Event{NotificationID: id, Seq: seq, Status: st}
}

// seqs renders events as "id:seq" for comparison.
func seqs(es []notification.Event) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = fmt.Sprintf("%s:%d", e.NotificationID, e.Seq)
	}
	return out
}

// permutations returns every order of es.
func permutations(es []notification.Event) [][]notification.Event {
	if len(es) <= 1 {
		return [][]notification.Event{slices.Clone(es)}
	}
	var out [][]notification.Event
	for i := range es {
		rest := slices.Concat(es[:i], es[i+1:])
		for _, p := range permutations(rest) {
			out = append(out, append([]notification.Event{es[i]}, p...))
		}
	}
	return out
}

// lifecycle is one notification after a snapshot at seq 1 (pending): a claim,
// a release, a reclaim, a send.
var lifecycle = []notification.Event{
	ev("n", 2, notification.StatusProcessing),
	ev("n", 3, notification.StatusPending),
	ev("n", 4, notification.StatusProcessing),
	ev("n", 5, notification.StatusSent),
}

func TestReordererDeliversEveryArrivalOrderInSeqOrder(t *testing.T) {
	want := []string{"n:2", "n:3", "n:4", "n:5"}
	for _, order := range permutations(lifecycle) {
		t.Run(fmt.Sprint(seqs(order)), func(t *testing.T) {
			r := newReorderer(testHold, testForget)
			r.Seed("n", 1)
			var got []notification.Event
			for i, e := range order {
				got = append(got, r.Push(e, t0.Add(time.Duration(i)*time.Millisecond))...)
			}
			if !slices.Equal(seqs(got), want) {
				t.Fatalf("arrived %v: delivered %v, want %v", seqs(order), seqs(got), want)
			}
		})
	}
}

func TestReordererEveryArrivalOrderWithDuplicatesAndStaleEvents(t *testing.T) {
	// The snapshot already shows seq 3: the buffered 2 and 3 are stale, and
	// every event arrives twice.
	all := []notification.Event{
		ev("n", 2, notification.StatusProcessing), ev("n", 3, notification.StatusPending),
		ev("n", 4, notification.StatusProcessing), ev("n", 5, notification.StatusSent),
	}
	want := []string{"n:4", "n:5"}
	for _, order := range permutations(all) {
		r := newReorderer(testHold, testForget)
		r.Seed("n", 3)
		var got []notification.Event
		for _, e := range append(order, order...) {
			got = append(got, r.Push(e, t0)...)
		}
		if !slices.Equal(seqs(got), want) {
			t.Fatalf("arrived %v twice: delivered %v, want %v", seqs(order), seqs(got), want)
		}
	}
}

func TestReordererInterleavedNotificationsAreOrderedIndependently(t *testing.T) {
	arrivals := []notification.Event{
		ev("a", 3, notification.StatusSent), ev("b", 2, notification.StatusProcessing),
		ev("a", 2, notification.StatusProcessing), ev("b", 4, notification.StatusFailed),
		ev("b", 3, notification.StatusPending),
	}
	r := newReorderer(testHold, testForget)
	r.Seed("a", 1)
	r.Seed("b", 1)
	var got []notification.Event
	for _, e := range arrivals {
		got = append(got, r.Push(e, t0)...)
	}
	want := []string{"b:2", "a:2", "a:3", "b:3", "b:4"}
	if !slices.Equal(seqs(got), want) {
		t.Fatalf("delivered %v, want %v", seqs(got), want)
	}
}

func TestReordererGivesUpOnALostPredecessorAfterTheHold(t *testing.T) {
	r := newReorderer(testHold, testForget)
	r.Seed("n", 1)
	// 2 (the claim) was lost; 3 and 4 arrive.
	if out := r.Push(ev("n", 3, notification.StatusPending), t0); len(out) != 0 {
		t.Fatalf("3 without 2: want it held, delivered %v", seqs(out))
	}
	if out := r.Push(ev("n", 4, notification.StatusProcessing), t0.Add(10*time.Millisecond)); len(out) != 0 {
		t.Fatalf("4 without 2: want it held, delivered %v", seqs(out))
	}
	at, ok := r.Deadline()
	if !ok || !at.Equal(t0.Add(testHold)) {
		t.Fatalf("deadline: want %s, got %s (ok %v)", t0.Add(testHold), at, ok)
	}
	if out := r.Expire(at.Add(-time.Millisecond)); len(out) != 0 {
		t.Fatalf("before the hold ran out: delivered %v", seqs(out))
	}
	if out := r.Expire(at); !slices.Equal(seqs(out), []string{"n:3", "n:4"}) {
		t.Fatalf("hold ran out: delivered %v, want [n:3 n:4]", seqs(out))
	}
	// The lost 2 turns up late: it is older than what was shown; dropped.
	if out := r.Push(ev("n", 2, notification.StatusProcessing), at.Add(time.Millisecond)); len(out) != 0 {
		t.Fatalf("late predecessor: want it dropped, delivered %v", seqs(out))
	}
	if out := r.Push(ev("n", 5, notification.StatusSent), at.Add(2*time.Millisecond)); !slices.Equal(seqs(out), []string{"n:5"}) {
		t.Fatalf("next after the gap: delivered %v, want [n:5]", seqs(out))
	}
}

func TestReordererGivesUpOneGapAndKeepsWaitingForALaterOne(t *testing.T) {
	r := newReorderer(testHold, testForget)
	r.Seed("n", 1)
	r.Push(ev("n", 3, notification.StatusPending), t0)              // 2 missing
	r.Push(ev("n", 6, notification.StatusSent), t0.Add(testHold/2)) // 4, 5 missing
	if out := r.Expire(t0.Add(testHold)); !slices.Equal(seqs(out), []string{"n:3"}) {
		t.Fatalf("first hold ran out: delivered %v, want [n:3]", seqs(out))
	}
	if out := r.Push(ev("n", 4, notification.StatusProcessing), t0.Add(testHold+time.Millisecond)); !slices.Equal(seqs(out), []string{"n:4"}) {
		t.Fatalf("4 after 3: delivered %v, want [n:4]", seqs(out))
	}
	if out := r.Expire(t0.Add(testHold/2 + testHold)); !slices.Equal(seqs(out), []string{"n:6"}) {
		t.Fatalf("second hold ran out: delivered %v, want [n:6]", seqs(out))
	}
}

func TestReordererUnseededNotificationStartsAtItsFirstEvent(t *testing.T) {
	// An unfiltered stream has no snapshot: the claim overtook the create.
	r := newReorderer(testHold, testForget)
	if out := r.Push(ev("n", 2, notification.StatusProcessing), t0); !slices.Equal(seqs(out), []string{"n:2"}) {
		t.Fatalf("first event of an unseen notification: delivered %v, want it at once", seqs(out))
	}
	if out := r.Push(ev("n", 1, notification.StatusPending), t0); len(out) != 0 {
		t.Fatalf("the create, after the claim was shown: want it dropped, delivered %v", seqs(out))
	}
	if out := r.Push(ev("n", 3, notification.StatusSent), t0); !slices.Equal(seqs(out), []string{"n:3"}) {
		t.Fatalf("next: delivered %v, want [n:3]", seqs(out))
	}
	if out := r.Push(ev("n", 2, notification.StatusProcessing), t0.Add(testForget-time.Millisecond)); len(out) != 0 {
		t.Fatalf("a late event inside the forget window: want it dropped, delivered %v", seqs(out))
	}
}

func TestReordererForgetsQuietUnseededNotificationsWhateverTheirStatus(t *testing.T) {
	r := newReorderer(testHold, testForget)
	r.Push(ev("sent", 3, notification.StatusSent), t0)
	r.Push(ev("waiting", 1, notification.StatusPending), t0.Add(time.Second)) // scheduled far ahead: no more events soon
	at, ok := r.Deadline()
	if !ok || !at.Equal(t0.Add(testForget)) {
		t.Fatalf("deadline: want %s (forget the oldest), got %s (ok %v)", t0.Add(testForget), at, ok)
	}
	r.Expire(t0.Add(time.Second + testForget))
	if _, ok := r.Deadline(); ok {
		t.Fatal("after the forget window: want nothing left to expire (both forgotten)")
	}
	// Forgotten: an event for it starts over as an unseen notification.
	if out := r.Push(ev("waiting", 2, notification.StatusProcessing), t0.Add(time.Hour)); !slices.Equal(seqs(out), []string{"waiting:2"}) {
		t.Fatalf("after forgetting: delivered %v, want [waiting:2]", seqs(out))
	}
}

func TestReordererNeverForgetsASeededNotification(t *testing.T) {
	// The verifier's case: the pending of a release lands long after the
	// reclaim's processing and sent were shown.
	r := newReorderer(testHold, testForget)
	r.Seed("n", 2)
	r.Push(ev("n", 4, notification.StatusProcessing), t0)
	r.Push(ev("n", 5, notification.StatusSent), t0)
	if out := r.Expire(t0.Add(testHold)); !slices.Equal(seqs(out), []string{"n:4", "n:5"}) {
		t.Fatalf("hold ran out: delivered %v, want [n:4 n:5]", seqs(out))
	}
	if _, ok := r.Deadline(); ok {
		t.Fatal("a seeded notification has nothing to expire")
	}
	if out := r.Push(ev("n", 3, notification.StatusPending), t0.Add(24*time.Hour)); len(out) != 0 {
		t.Fatalf("the release's pending a day late: want it dropped, delivered %v", seqs(out))
	}
}

func TestReordererSeededFailedNotificationStaysExactThroughAReplay(t *testing.T) {
	r := newReorderer(testHold, testForget)
	r.Seed("n", 7) // failed
	r.Expire(t0.Add(time.Hour))
	// The replay's claim overtakes the replay itself.
	if out := r.Push(ev("n", 9, notification.StatusProcessing), t0.Add(time.Hour)); len(out) != 0 {
		t.Fatalf("claim before the replay: want it held, delivered %v", seqs(out))
	}
	if out := r.Push(ev("n", 8, notification.StatusPending), t0.Add(time.Hour)); !slices.Equal(seqs(out), []string{"n:8", "n:9"}) {
		t.Fatalf("replay then claim: delivered %v, want [n:8 n:9]", seqs(out))
	}
}

func TestReordererPassesUnsequencedEventsOn(t *testing.T) {
	r := newReorderer(testHold, testForget)
	r.Seed("n", 4)
	if out := r.Push(ev("n", 0, notification.StatusPending), t0); !slices.Equal(seqs(out), []string{"n:0"}) {
		t.Fatalf("an event without seq (older publisher): delivered %v, want it as it came", seqs(out))
	}
}
