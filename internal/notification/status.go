// Package notification holds the contracts every other package builds against: the
// notification vocabulary, the one state machine, the limits and the ports
// between the API, the workers and their dependencies. Only the coordinator
// edits it.
package notification

import (
	"errors"
	"fmt"
)

// Channel is where a notification is delivered.
type Channel string

// The channels the brief names.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every channel, in a fixed order.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

// Valid reports whether c is a known channel.
func (c Channel) Valid() bool {
	return c == ChannelSMS || c == ChannelEmail || c == ChannelPush
}

// Priority decides a notification's share of a channel's send rate (decision 0010).
type Priority string

// The priorities the brief names.
const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Priorities lists every priority, highest first.
var Priorities = []Priority{PriorityHigh, PriorityNormal, PriorityLow}

// Valid reports whether p is a known priority.
func (p Priority) Valid() bool {
	return p == PriorityHigh || p == PriorityNormal || p == PriorityLow
}

// Status is a stored state of a notification (decision 0005). The API also
// shows "scheduled" for a pending notification that is not yet due and was
// never tried; that is a view, not a stored state.
type Status string

// Stored states. Sent means the provider accepted it; the provider never
// confirms delivery to the end user.
const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// Statuses lists every stored state.
var Statuses = []Status{StatusPending, StatusProcessing, StatusSent, StatusFailed, StatusCancelled}

// Valid reports whether s is a stored state.
func (s Status) Valid() bool {
	_, ok := transitions[s]
	return ok
}

// Terminal reports whether nothing happens to a notification in state s unless
// someone asks (a replay of a failed one).
func (s Status) Terminal() bool {
	return s == StatusSent || s == StatusFailed || s == StatusCancelled
}

// FailureKind says why a notification is failed.
type FailureKind string

// Permanent: the provider refused it. Exhausted: every attempt failed; this is
// the dead letter that can be replayed.
const (
	FailurePermanent FailureKind = "permanent"
	FailureExhausted FailureKind = "exhausted"
)

// AttemptOutcome is where one provider call stands, or how it ended.
type AttemptOutcome string

// InFlight: recorded and committed before the request leaves; no result yet.
// Accepted: 2xx. Transient: will be retried. Permanent: will not. Unknown:
// the request may have left but its result was never recorded (the lease ran
// out, or shutdown aborted the call); a late result may still replace it.
const (
	OutcomeInFlight  AttemptOutcome = "in_flight"
	OutcomeAccepted  AttemptOutcome = "accepted"
	OutcomeTransient AttemptOutcome = "transient"
	OutcomePermanent AttemptOutcome = "permanent"
	OutcomeUnknown   AttemptOutcome = "unknown"
)

// AttemptOutcomes lists every attempt outcome.
var AttemptOutcomes = []AttemptOutcome{OutcomeInFlight, OutcomeAccepted, OutcomeTransient, OutcomePermanent, OutcomeUnknown}

// attemptTransitions is the attempt's own state machine; an attempt is
// created in flight.
//
//	in_flight -> accepted, transient, permanent  the worker recorded the result
//	in_flight -> unknown                         the lease ran out or the call was aborted
//	unknown   -> accepted, transient, permanent  the late result of a worker whose lease ran out
var attemptTransitions = map[AttemptOutcome][]AttemptOutcome{
	OutcomeInFlight:  {OutcomeAccepted, OutcomeTransient, OutcomePermanent, OutcomeUnknown},
	OutcomeUnknown:   {OutcomeAccepted, OutcomeTransient, OutcomePermanent},
	OutcomeAccepted:  {},
	OutcomeTransient: {},
	OutcomePermanent: {},
}

// AttemptTransition is the only gate for a change of an attempt's outcome.
func AttemptTransition(from, to AttemptOutcome) error {
	for _, allowed := range attemptTransitions[from] {
		if allowed == to {
			return nil
		}
	}
	return fmt.Errorf("%w: attempt %q to %q", ErrInvalidTransition, from, to)
}

// ErrInvalidTransition is returned by Transition for a change the state machine forbids.
var ErrInvalidTransition = errors.New("invalid status transition")

// transitions is the whole state machine.
//
//	pending    -> processing  a worker claims it
//	pending    -> cancelled   a client cancels it (also while scheduled or waiting to retry)
//	processing -> sent        the provider accepted it
//	processing -> pending     a retry is stored as a later due time, or the lease expired
//	processing -> failed      permanent refusal, or the attempt budget is spent
//	failed     -> pending     a replay
var transitions = map[Status][]Status{
	StatusPending:    {StatusProcessing, StatusCancelled},
	StatusProcessing: {StatusSent, StatusPending, StatusFailed},
	StatusSent:       {},
	StatusFailed:     {StatusPending},
	StatusCancelled:  {},
}

// Transition is the only gate for a status change. Callers check it, then
// write the change as a conditional UPDATE ... WHERE status = <from> (and
// claim_version = ? when a worker writes), so a lost race changes nothing.
func Transition(from, to Status) error {
	for _, allowed := range transitions[from] {
		if allowed == to {
			return nil
		}
	}
	return fmt.Errorf("%w: %q to %q", ErrInvalidTransition, from, to)
}
