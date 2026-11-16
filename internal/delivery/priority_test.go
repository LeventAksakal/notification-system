package delivery

import (
	"testing"
	"time"
)

func TestSelectorPlan(t *testing.T) {
	now := time.Now()
	s := newSelector([3]int{70, 20, 10})
	if got := s.plan(100, now); got != [3]int{70, 20, 10} {
		t.Errorf("100 slots: got %v, want exactly 70/20/10", got)
	}
	s = newSelector([3]int{70, 20, 10})
	s.settle(1, now.Add(time.Second), true, s.beginRound())
	if got := s.plan(100, now); got != [3]int{70, 0, 30} {
		t.Errorf("normal empty: got %v, want its share lent down to low", got)
	}
	s = newSelector([3]int{70, 20, 10})
	s.settle(2, now.Add(time.Second), true, s.beginRound())
	if got := s.plan(100, now); got != [3]int{70, 30, 0} {
		t.Errorf("low empty: got %v, want its share lent up to normal", got)
	}
	s = newSelector([3]int{70, 20, 0})
	if got := s.plan(100, now); got[2] != 0 {
		t.Errorf("weight 0: low got %d slots of its own, want none", got[2])
	}
}
