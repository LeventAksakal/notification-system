package worker

import (
	"time"

	"github.com/LeventAksakal/notification-system/internal/core"
)

// selector splits one channel's send slots between the priorities (decision
// 0010). Each slot goes to the priority chosen by smooth weighted round robin
// over the weights; a priority known to have nothing due lends the slot to
// the next priority down, then up. A priority found empty is not asked again
// until its emptyUntil passes, so borrowing costs no extra claim query.
//
// A selector is guarded by its channel's mutex.
type selector struct {
	weights [3]int
	total   int
	current [3]int
	// emptyUntil[i]: priority i had nothing (more) due when last asked.
	emptyUntil [3]time.Time
	// idle[i] is priority i's current idle poll.
	idle [3]time.Duration
}

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

func (s *selector) empty(i int, now time.Time) bool { return now.Before(s.emptyUntil[i]) }

// markEmpty records at now that priority i had nothing more due; it is
// skipped for its idle poll: lo after it had rows, doubling up to hi while it
// keeps having none.
func (s *selector) markEmpty(i int, now time.Time, hadRows bool, lo, hi time.Duration) {
	switch {
	case hadRows || s.idle[i] < lo:
		s.idle[i] = lo
	default:
		s.idle[i] = min(s.idle[i]*2, hi)
	}
	s.emptyUntil[i] = now.Add(s.idle[i])
}

// markBusy forgets that p was empty.
func (s *selector) markBusy(i int) { s.emptyUntil[i], s.idle[i] = time.Time{}, 0 }

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

// priorityName is the priority at index i (high, normal, low).
func priorityName(i int) core.Priority { return core.Priorities[i] }
