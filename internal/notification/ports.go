package notification

import (
	"context"
	"errors"
	"time"
)

// ErrLeaseLost is returned by a Queue write whose claim_version no longer
// matches: the lease expired and another worker owns the row. The caller
// discards its result (decision 0018).
var ErrLeaseLost = errors.New("lease lost")

// Job is a claimed notification: what a worker needs to send it and to write
// the result back.
type Job struct {
	ID            string
	BatchID       string // "" outside a batch
	Channel       Channel
	Priority      Priority
	To            string
	Content       string
	Attempts      int    // provider calls already made in this attempt budget
	ClaimVersion  uint64 // carried on every write for this claim
	LeaseUntil    time.Time
	DueAt         time.Time
	CorrelationID string
	Traceparent   string // W3C traceparent of the accepting request; "" if none
}

// Attempt is the record of one provider call.
type Attempt struct {
	StartedAt         time.Time
	Duration          time.Duration
	Outcome           AttemptOutcome
	HTTPStatus        int // 0 when no response arrived
	Error             string
	ProviderMessageID string
}

// Queue is the work queue. MySQL implements it (decision 0003); a broker could
// replace it behind this interface. Every method that takes a Job writes with
// WHERE status = 'processing' AND claim_version = job.ClaimVersion and returns
// ErrLeaseLost when no row matched.
type Queue interface {
	// Claim leases up to n due pending notifications of one channel and
	// priority for the lease duration, oldest due time first. Rows locked by
	// another claimer are skipped, never waited for.
	Claim(ctx context.Context, channel Channel, priority Priority, n int, lease time.Duration) ([]Job, error)
	// MarkSent records the accepted attempt and ends the notification as sent.
	MarkSent(ctx context.Context, job Job, attempt Attempt) error
	// Reschedule records the failed attempt and returns the notification to
	// pending, due again at dueAt (a retry is a stored due time, decision 0007).
	Reschedule(ctx context.Context, job Job, attempt Attempt, dueAt time.Time) error
	// MarkFailed records the failed attempt and ends the notification as failed.
	MarkFailed(ctx context.Context, job Job, attempt Attempt, kind FailureKind) error
	// Release returns a claimed notification to pending without recording an
	// attempt, due again at dueAt: the worker could not send it (breaker open,
	// shutdown), so no attempt is burned (decision 0008).
	Release(ctx context.Context, job Job, dueAt time.Time) error
	// ReapExpired returns notifications whose lease ran out to pending and
	// reports them, so the caller can publish the change (decision 0018). Of
	// each Job only ID, BatchID, Channel, Attempts and CorrelationID are set.
	ReapExpired(ctx context.Context) ([]Job, error)
}

// Grant is a reservation of consecutive send slots on one channel.
type Grant struct {
	// N is how many slots were reserved; 0 when none is available yet.
	N int
	// At is the limiter's clock (Valkey's) at the moment of the reservation:
	// the "now" that First is measured from. The reply reaches the caller
	// some unknown time later, so the caller must place slots on its own
	// clock from At and what it has learned about the two clocks' offset,
	// never from the moment the reply arrived: a late reply would otherwise
	// send its whole group late, on top of the next group.
	At time.Time
	// First is how long after At the first slot is; slot i is due at
	// At + First + i*Interval. A slot is the moment its request may leave.
	First time.Duration
	// Interval is the spacing between slots: one second divided by the paced rate.
	Interval time.Duration
	// Wait is how long until asking again can reserve something; set when N
	// is less than what was asked for.
	Wait time.Duration
}

// Limiter is the send-rate limit shared by every worker (decision 0009). Its
// state and its clock live in Valkey. It hands out send slots spaced one
// interval apart across all workers, so the provider sees an even stream
// whatever the workers' own timing is.
type Limiter interface {
	// Take reserves up to n consecutive send slots on a channel, none further
	// ahead than the limiter's horizon. A slot that is not used is lost; a
	// request must never leave before its slot, nor more than one interval
	// after it, whatever the reply's delay was. An error means the limiter is
	// unreachable: the caller sends nothing (fail closed). With n = 0 nothing
	// is reserved and the grant still carries At: a reading of the limiter's
	// clock that costs no slot.
	Take(ctx context.Context, channel Channel, n int) (Grant, error)
}

// Event is one committed status change, as subscribers see it (decision 0013).
type Event struct {
	NotificationID string
	BatchID        string // "" outside a batch
	Status         Status
	Attempts       int
	Error          string // last error, when there is one
	At             time.Time
	CorrelationID  string
}

// Publisher fans status changes out to subscribers. It is called once per
// change, after the change is committed. Publishing is best effort: an error
// is logged and never undoes or blocks the change; the REST API stays the
// source of truth.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}
