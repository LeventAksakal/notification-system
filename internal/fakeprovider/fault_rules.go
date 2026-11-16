package fakeprovider

import (
	"errors"
	"fmt"
)

// Mode is what a fault rule does to a matching request.
type Mode string

// Fault modes.
const (
	ModeStatus         Mode = "status"           // respond with Status (and Retry-After if set)
	ModeDelay          Mode = "delay"            // wait DelayMS, then respond normally
	ModeAcceptThenHang Mode = "accept_then_hang" // count the delivery, then hold the response DelayMS
	ModeDrop           Mode = "drop"             // close the connection without a response
)

// ErrInvalidFault is returned by SetFaults for a rule that cannot apply.
var ErrInvalidFault = errors.New("invalid fault rule")

// Fault is one scripted failure. Count > 0 applies it to the next Count
// matching requests; Ratio in (0,1] applies it to that share of them; neither
// applies it until cleared. A ratio rule that does not fire falls through to
// the next rule. Channel and To narrow a rule to one channel or one recipient
// (a message-specific failure while the others succeed); empty matches any.
type Fault struct {
	Channel           string  `json:"channel,omitempty"`
	To                string  `json:"to,omitempty"`
	Mode              Mode    `json:"mode"`
	Status            int     `json:"status,omitempty"`
	RetryAfterSeconds int     `json:"retryAfterSeconds,omitempty"`
	DelayMS           int     `json:"delayMs,omitempty"`
	Count             int     `json:"count,omitempty"`
	Ratio             float64 `json:"ratio,omitempty"`
}

func (f Fault) validate() error {
	if f.Channel != "" && !validChannels[f.Channel] {
		return fmt.Errorf("%w: unknown channel %q", ErrInvalidFault, f.Channel)
	}
	switch f.Mode {
	case ModeStatus:
		if f.Status < 200 || f.Status > 599 {
			return fmt.Errorf("%w: status %d outside 200..599", ErrInvalidFault, f.Status)
		}
	case ModeDelay, ModeAcceptThenHang, ModeDrop:
	default:
		return fmt.Errorf("%w: unknown mode %q", ErrInvalidFault, f.Mode)
	}
	switch {
	case f.Count < 0 || f.DelayMS < 0 || f.RetryAfterSeconds < 0:
		return fmt.Errorf("%w: count, delayMs and retryAfterSeconds must be >= 0", ErrInvalidFault)
	case f.Ratio < 0 || f.Ratio > 1:
		return fmt.Errorf("%w: ratio %v outside 0..1", ErrInvalidFault, f.Ratio)
	case f.Count > 0 && f.Ratio > 0:
		return fmt.Errorf("%w: set count or ratio, not both", ErrInvalidFault)
	}
	return nil
}

// SetFaults replaces every fault rule; nil or empty clears them.
func (s *Server) SetFaults(rules []Fault) error {
	for i, f := range rules {
		if err := f.validate(); err != nil {
			return fmt.Errorf("rule %d: %w", i, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = append([]Fault(nil), rules...) // copy: the caller keeps its slice
	return nil
}

// pickFaultLocked returns the first rule that fires for channel, spending one
// of its Count if it has one. Caller holds s.mu.
func (s *Server) pickFaultLocked(channel, to string) (Fault, bool) {
	for i := range s.faults {
		f := &s.faults[i] // pointer into the slice so the Count decrement sticks
		if f.Channel != "" && f.Channel != channel || f.To != "" && f.To != to {
			continue
		}
		switch {
		case f.Count > 0:
			f.Count--
			got := *f
			if f.Count == 0 {
				s.faults = append(s.faults[:i:i], s.faults[i+1:]...) // spent: remove
			}
			return got, true
		case f.Ratio > 0:
			if s.rand() < f.Ratio {
				return *f, true
			}
		default:
			return *f, true
		}
	}
	return Fault{}, false
}
