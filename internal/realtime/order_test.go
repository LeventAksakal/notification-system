package realtime

import (
	"slices"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/core"
)

func msg(st core.Status, attempts int, origin Origin, at time.Time) Message {
	return Message{Origin: origin, Event: core.Event{NotificationID: "n1", Status: st, Attempts: attempts, At: at}}
}

func labels(ms []Message) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = string(m.Status)
	}
	return out
}

func TestSequencerRestoresCommitOrderPerNotification(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	ms := func(d int) time.Time { return t0.Add(time.Duration(d) * time.Millisecond) }
	pending, processing, sent := core.StatusPending, core.StatusProcessing, core.StatusSent
	cases := []struct {
		name string
		in   []Message
		// expire after the last message, past the hold
		want []string
	}{
		{"in order passes straight through",
			[]Message{msg(pending, 0, OriginAPI, ms(0)), msg(processing, 0, OriginWorker, ms(5)), msg(sent, 1, OriginWorker, ms(9))},
			[]string{"pending", "processing", "sent"}},
		{"a claim that overtakes the create waits for it",
			[]Message{msg(processing, 0, OriginWorker, ms(5)), msg(sent, 1, OriginWorker, ms(9)), msg(pending, 0, OriginAPI, ms(0))},
			[]string{"pending", "processing", "sent"}},
		{"a claim that overtakes the reschedule waits for it",
			[]Message{msg(pending, 0, OriginAPI, ms(0)), msg(processing, 0, OriginWorker, ms(1)),
				msg(processing, 1, OriginWorker, ms(9)), msg(pending, 1, OriginWorker, ms(10)).withError("http 503")},
			[]string{"pending", "processing", "pending", "processing"}},
		{"a release after an unseen claim stays after it",
			[]Message{msg(processing, 0, OriginWorker, ms(5)), msg(pending, 0, OriginWorker, ms(6))},
			[]string{"processing", "pending"}},
		{"a claim with no predecessor in sight goes out after the hold",
			[]Message{msg(processing, 2, OriginWorker, ms(5)), msg(sent, 3, OriginWorker, ms(9))},
			[]string{"processing", "sent"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSequencer(holdFor, rememberFor)
			var got []Message
			for _, m := range tc.in {
				got = append(got, s.push(m, t0)...)
			}
			got = append(got, s.expire(t0.Add(holdFor))...)
			if !slices.Equal(labels(got), tc.want) {
				t.Fatalf("want %v, got %v", tc.want, labels(got))
			}
			if len(s.held) != 0 {
				t.Fatalf("still held: %v", s.held)
			}
		})
	}
}

func (m Message) withError(e string) Message { m.Error = e; return m }

func TestSequencerHoldsOnlyUntilTheDeadline(t *testing.T) {
	t0 := time.Now()
	s := newSequencer(holdFor, rememberFor)
	if out := s.push(msg(core.StatusProcessing, 0, OriginWorker, t0), t0); len(out) != 0 {
		t.Fatalf("delivered before its predecessor: %v", labels(out))
	}
	if out := s.expire(t0.Add(holdFor - time.Millisecond)); len(out) != 0 {
		t.Fatalf("released before the hold ran out: %v", labels(out))
	}
	if out := s.expire(t0.Add(holdFor)); !slices.Equal(labels(out), []string{"processing"}) {
		t.Fatalf("want the claim released at the deadline, got %v", labels(out))
	}
}

func TestSequencerForgetsFinishedAndIdleNotifications(t *testing.T) {
	t0 := time.Now()
	s := newSequencer(holdFor, time.Minute)
	s.push(msg(core.StatusPending, 0, OriginAPI, t0), t0)
	s.push(Message{Event: core.Event{NotificationID: "n2", Status: core.StatusPending}}, t0)
	s.push(msg(core.StatusProcessing, 0, OriginWorker, t0), t0)
	s.push(msg(core.StatusSent, 1, OriginWorker, t0), t0)
	if _, ok := s.last["n1"]; ok {
		t.Fatal("a finished notification is still remembered")
	}
	s.expire(t0.Add(2 * time.Minute))
	if len(s.last) != 0 {
		t.Fatalf("idle notifications still remembered: %v", s.last)
	}
}

func TestCoveredDropsOnlyWhatTheSnapshotAlreadyShows(t *testing.T) {
	ev := func(st core.Status, a int) core.Event { return core.Event{Status: st, Attempts: a} }
	cases := []struct {
		name      string
		snap, evt core.Event
		want      bool
	}{
		{"the change the snapshot shows", ev(core.StatusPending, 0), ev(core.StatusPending, 0), true},
		{"an earlier phase", ev(core.StatusProcessing, 0), ev(core.StatusPending, 0), true},
		{"an earlier attempt", ev(core.StatusSent, 1), ev(core.StatusProcessing, 0), true},
		{"a reschedule before the next claim", ev(core.StatusProcessing, 1), ev(core.StatusPending, 1), true},
		{"the next claim", ev(core.StatusPending, 0), ev(core.StatusProcessing, 0), false},
		{"the result", ev(core.StatusProcessing, 0), ev(core.StatusSent, 1), false},
		{"a cancel", ev(core.StatusPending, 0), ev(core.StatusCancelled, 0), false},
		{"a replay after failure", ev(core.StatusFailed, 5), ev(core.StatusPending, 0), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Covered(tc.snap, tc.evt); got != tc.want {
				t.Fatalf("Covered(%v, %v) = %t, want %t", tc.snap, tc.evt, got, tc.want)
			}
		})
	}
}
