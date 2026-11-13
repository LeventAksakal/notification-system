package worker

import (
	"time"

	"github.com/LeventAksakal/notification-system/internal/core"
)

// selector splits one channel's permits between the priorities (decision
// 0010). Each permit goes to the priority chosen by smooth weighted round
// robin over the weights; a priority with nothing due lends the permit to the
// next priority down, then up. A priority found empty is not asked again
// until its emptyUntil passes, so borrowing costs one claim query, not three.
//
// A selector belongs to one channel loop goroutine; it is not locked.
type selector struct {
	weights [3]int
	total   int
	current [3]int
	// emptyUntil[i]: priority i had nothing due when last asked.
	emptyUntil [3]time.Time
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

// next advances the round robin and returns the priority this permit belongs to.
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

// order is the claim order for one permit: the chosen priority, then the
// lower ones, then the higher ones, skipping those known to be empty at now.
func (s *selector) order(now time.Time) []core.Priority {
	p := s.next()
	out := make([]core.Priority, 0, 3)
	add := func(i int) {
		if !now.Before(s.emptyUntil[i]) {
			out = append(out, core.Priorities[i])
		}
	}
	add(p)
	for i := p + 1; i < 3; i++ {
		add(i)
	}
	for i := p - 1; i >= 0; i-- {
		add(i)
	}
	return out
}

// markEmpty records that p had nothing due; it is skipped until until.
func (s *selector) markEmpty(p core.Priority, until time.Time) {
	s.emptyUntil[index(p)] = until
}

// markBusy forgets that p was empty.
func (s *selector) markBusy(p core.Priority) { s.emptyUntil[index(p)] = time.Time{} }

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

func index(p core.Priority) int {
	for i, q := range core.Priorities {
		if q == p {
			return i
		}
	}
	return len(core.Priorities) - 1
}
