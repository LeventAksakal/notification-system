package core

import (
	"errors"
	"testing"
)

// TestTransitionCoversEveryPair pins the whole state machine: every (from, to)
// pair is either in the allowed list below or refused.
func TestTransitionCoversEveryPair(t *testing.T) {
	allowed := map[[2]Status]bool{
		{StatusPending, StatusProcessing}: true,
		{StatusPending, StatusCancelled}:  true,
		{StatusProcessing, StatusSent}:    true,
		{StatusProcessing, StatusPending}: true,
		{StatusProcessing, StatusFailed}:  true,
		{StatusFailed, StatusPending}:     true,
	}
	for _, from := range Statuses {
		for _, to := range Statuses {
			err := Transition(from, to)
			if allowed[[2]Status{from, to}] {
				if err != nil {
					t.Errorf("Transition(%q, %q) = %v, want allowed", from, to, err)
				}
				continue
			}
			if !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("Transition(%q, %q) = %v, want ErrInvalidTransition", from, to, err)
			}
		}
	}
}

func TestTransitionRefusesUnknownStates(t *testing.T) {
	if err := Transition("scheduled", StatusProcessing); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("unknown from: got %v", err)
	}
	if err := Transition(StatusPending, "delivered"); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("unknown to: got %v", err)
	}
}

func TestTerminalStates(t *testing.T) {
	want := map[Status]bool{StatusSent: true, StatusFailed: true, StatusCancelled: true}
	for _, s := range Statuses {
		if s.Terminal() != want[s] {
			t.Errorf("%q.Terminal() = %v, want %v", s, s.Terminal(), want[s])
		}
		if !s.Valid() {
			t.Errorf("%q.Valid() = false", s)
		}
	}
}

func TestContentLimitPerChannel(t *testing.T) {
	for _, c := range Channels {
		if n := MaxContentLenFor(c); n <= 0 || n > MaxContentLen {
			t.Errorf("MaxContentLenFor(%q) = %d, want within (0, %d]", c, n, MaxContentLen)
		}
		if n := MaxRecipientLenFor(c); n <= 0 || n > MaxRecipientLen {
			t.Errorf("MaxRecipientLenFor(%q) = %d, want within (0, %d]", c, n, MaxRecipientLen)
		}
	}
	if MaxContentLenFor("fax") != 0 || MaxRecipientLenFor("fax") != 0 {
		t.Error("unknown channel must have no limit value")
	}
}
