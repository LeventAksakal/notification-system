package core

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
	// reports how many (decision 0018).
	ReapExpired(ctx context.Context) (int, error)
}

// Limiter is the send-rate limit shared by every worker (decision 0009). Its
// state and its clock live in Valkey.
type Limiter interface {
	// Take asks for up to n send permits on a channel and returns how many
	// were granted now. When fewer than n were granted, wait is how long until
	// the next permit can be had. An error means the limiter is unreachable:
	// the caller sends nothing (fail closed).
	Take(ctx context.Context, channel Channel, n int) (granted int, wait time.Duration, err error)
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
