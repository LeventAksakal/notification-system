package delivery

import (
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// selector splits one channel's send slots between the priorities (decision
// 0010). Each slot goes to the priority chosen by smooth weighted round robin
// over the weights; a priority known to have nothing due lends the slot to
// the next priority down, then up. A priority found empty is not asked again
// until its emptyUntil passes, so borrowing costs no extra claim query.
//
// A priority found empty sleeps until its next due time, until a wake hint
// says work arrived, or for at most the safety poll (wake.SafetyPoll). A hint
// that arrives while a round is in flight (a claim that may have started
// before the hinted row was committed) survives that round: see beginRound
// and settle.
//
// A selector is guarded by its channel's mutex.
type selector struct {
	weights [3]int
	total   int
	current [3]int
	// emptyUntil[i]: priority i had nothing (more) due when last asked, and
	// is not asked again before this time. Zero: ask it.
	emptyUntil [3]time.Time
	// check[i]: emptyUntil[i] is only the safety poll, not a due time or a
	// hint; when it passes the channel asks Queue.NextDue before it claims.
	check [3]bool
	// hintSeq[i] counts the hints received for priority i; hintDue[i] is the
	// earliest due time among those received since the current round began
	// (valid while hinted[i]).
	hintSeq [3]uint64
	hintDue [3]time.Time
	hinted  [3]bool
	// raced[i]: priority i had a row due that its last round did not get
	// (another claimer held it); its short wait is a busy channel's, not
	// an idle one's.
	raced [3]bool
	// known[i]: priority i has been answered for at least once (claimed,
	// settled or hinted). A borrowed slot never goes to a priority not yet
	// known (see Worker.claim).
	known [3]bool
}

// round is the hint state when a round (a claim, or a NextDue read) began.
type round struct{ seq [3]uint64 }

func newSelector(weights [3]int) *selector {
	s := &selector{weights: weights}
	for _, w := range weights {
		s.total += w
	}
	if s.total == 0 { // config refuses this; equal shares keep the selector sane
		s.weights, s.total = [3]int{1, 1, 1}, 3
	}
	return s
}

// next advances the round robin and returns the priority the next slot belongs to.
func (s *selector) next() int {
	best := 0
	for i := range s.current {
		s.current[i] += s.weights[i]
		if s.current[i] > s.current[best] {
			best = i
		}
	}
	s.current[best] -= s.total
	return best
}

// plan splits n slots: counts[i] is how many priority i should claim. A slot
// whose priority is known to be empty at now goes to the first priority in
// lending order (down, then up) that is not.
func (s *selector) plan(n int, now time.Time) [3]int {
	var counts [3]int
	for range n {
		if i, ok := s.lend(s.next(), now); ok {
			counts[i]++
		}
	}
	return counts
}

// lend returns p, else the nearest lower priority, else the nearest higher
// one, skipping those known to be empty at now.
func (s *selector) lend(p int, now time.Time) (int, bool) {
	for _, i := range lendOrder(p) {
		if !s.empty(i, now) {
			return i, true
		}
	}
	return 0, false
}

// lendOrder is p, the lower priorities, then the higher ones.
func lendOrder(p int) []int {
	out := []int{p}
	for i := p + 1; i < 3; i++ {
		out = append(out, i)
	}
	for i := p - 1; i >= 0; i-- {
		out = append(out, i)
	}
	return out
}

// empty reports whether priority i is not to be claimed at now: known to
// have nothing due before emptyUntil, or past it only by its safety poll,
// which is answered by Queue.NextDue (due), not by a claim query. A claim
// that also asked such a priority would cost a full claim query for nothing
// on most polls; while claims are slow (owner item 0011) that tripled the
// claim time and the lead with it.
func (s *selector) empty(i int, now time.Time) bool {
	return now.Before(s.emptyUntil[i]) || s.check[i]
}

// beginRound starts a round that may settle priorities as empty: hints
// received from now on are kept for its settle. Rounds of one channel never
// overlap (channelRun.claiming).
func (s *selector) beginRound() round {
	s.hinted = [3]bool{}
	return round{seq: s.hintSeq}
}

// settle records that priority i is not asked again before until; check says
// until is only the safety poll. A hint received since r began wins when it
// is earlier: the round may not have seen the row it announces.
func (s *selector) settle(i int, until time.Time, check bool, r round) {
	s.emptyUntil[i], s.check[i], s.raced[i], s.known[i] = until, check, false, true
	if s.hintSeq[i] != r.seq[i] && s.hinted[i] && s.hintDue[i].Before(until) {
		s.emptyUntil[i], s.check[i] = s.hintDue[i], false
	}
}

// hint records that priority i has work due at due: it is asked again by
// then (at once when due has passed).
func (s *selector) hint(i int, due time.Time) {
	s.hintSeq[i]++
	s.known[i] = true
	if !s.hinted[i] || due.Before(s.hintDue[i]) {
		s.hintDue[i], s.hinted[i] = due, true
	}
	if s.emptyUntil[i].After(due) {
		s.emptyUntil[i], s.check[i] = due, false
	}
}

// markBusy records that priority i returned every row asked for.
func (s *selector) markBusy(i int) {
	s.emptyUntil[i], s.check[i], s.raced[i], s.known[i] = time.Time{}, false, false, true
}

// contended reports whether a priority is waiting for rows another claimer
// held when it was last asked.
func (s *selector) contended() bool { return s.raced[0] || s.raced[1] || s.raced[2] }

// nextCheck is the earliest time a priority known to be empty is asked again;
// the zero time when one is not known to be empty at now.
func (s *selector) nextCheck(now time.Time) time.Time {
	var earliest time.Time
	for _, u := range s.emptyUntil {
		if !now.Before(u) {
			return time.Time{}
		}
		if earliest.IsZero() || u.Before(earliest) {
			earliest = u
		}
	}
	return earliest
}

// due reports, at now, whether some priority is to be claimed (never found
// empty, or its due time or a hint has come), and which ones only reached
// their safety poll: those are asked Queue.NextDue first.
func (s *selector) due(now time.Time) (claim bool, peek []int) {
	for i, u := range s.emptyUntil {
		switch {
		case now.Before(u):
		case s.check[i]:
			peek = append(peek, i)
		default:
			claim = true
		}
	}
	return claim, peek
}

// priorityIndex is the index of p in notification.Priorities; -1 if none.
func priorityIndex(p notification.Priority) int {
	for i, q := range notification.Priorities {
		if q == p {
			return i
		}
	}
	return -1
}

// priorityName is the priority at index i (high, normal, low).
func priorityName(i int) notification.Priority { return notification.Priorities[i] }
