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
	ID           string
	BatchID      string // "" outside a batch
	Channel      Channel
	Priority     Priority
	To           string
	Content      string
	Attempts     int    // provider calls already made in this attempt budget
	BudgetSpent  int    // of those, the failures counted against the budget (decision 0026)
	ClaimVersion uint64 // carried on every write for this claim
	// StatusSeq is the row's status_seq in the state this Job reports: the
	// claim's (processing) from Claim, the new pending one from ReapExpired.
	// A write fenced by ClaimVersion that succeeds sets it to StatusSeq+1.
	StatusSeq     uint64
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
	// Counted is true when a failed call counts against the notification's
	// attempt budget: its provider was believed healthy after the failure
	// was recorded. A failure that belongs to a declared outage is recorded
	// with Counted false (decision 0026). Ignored for an accepted call.
	Counted bool
}

// Queue is the work queue. MySQL implements it (decision 0003); a broker could
// replace it behind this interface. Every method that takes a Job writes with
// WHERE status = 'processing' AND claim_version = job.ClaimVersion and returns
// ErrLeaseLost when no row matched.
type Queue interface {
	// Claim leases up to n due pending notifications of one channel and
	// priority for the lease duration, oldest due time first. Rows locked by
	// another claimer are skipped, never waited for. Each claimed job's
	// provider call is recorded as an in-flight attempt in the same commit,
	// so a crash cannot lose a call from the history (decision 0005); one
	// claim makes at most one call.
	Claim(ctx context.Context, channel Channel, priority Priority, n int, lease time.Duration) ([]Job, error)
	// MarkSent records the claim's attempt as accepted and ends the
	// notification as sent. The three result writes record the attempt even
	// when the lease was lost (then they return ErrLeaseLost and leave the
	// notification alone).
	MarkSent(ctx context.Context, job Job, attempt Attempt) error
	// Reschedule records the failed attempt and returns the notification to
	// pending, due again at dueAt (a retry is a stored due time, decision 0007).
	// It adds 1 to the budget spent only when attempt.Counted (decision 0026).
	Reschedule(ctx context.Context, job Job, attempt Attempt, dueAt time.Time) error
	// MarkFailed records the failed attempt and ends the notification as
	// failed; the failure always counts against the budget. An attempt
	// without an Outcome means no call was made: unknown calls already spent
	// the budget; the notification fails as exhausted, its counts stay as
	// they are and the claim's in-flight attempt goes.
	MarkFailed(ctx context.Context, job Job, attempt Attempt, kind FailureKind) error
	// Release returns a claimed notification to pending, due again at dueAt,
	// when its call was never made (provider outage declared, slot missed,
	// shutdown; decision 0026): no attempt is burned; the claim's in-flight
	// attempt is removed. A call that may have left is a result (unknown),
	// not a release.
	Release(ctx context.Context, job Job, dueAt time.Time) error
	// ReapExpired returns notifications whose lease ran out to pending and
	// reports them, so the caller can publish the change (decision 0018); an
	// attempt still in flight on such a lease becomes unknown and counts,
	// against the budget too (decision 0005). Of each Job only ID, BatchID,
	// Channel, Attempts, StatusSeq and CorrelationID are set.
	ReapExpired(ctx context.Context) ([]Job, error)
	// NextDue reports, for each priority of the channel that has pending
	// notifications, how long until the earliest of them is due, measured on
	// the database's clock (zero or negative: one is due now). A priority with
	// nothing pending is absent. It reads the claim index only, so an idle
	// worker can ask it instead of claiming.
	NextDue(ctx context.Context, channel Channel) (map[Priority]time.Duration, error)
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
	// Idle is how long the channel's slot grid had been free when this
	// reservation was made: its next free slot, one interval after the last
	// slot reserved before, had passed Idle before At. Zero while that slot
	// was still ahead (the grid was busy). After the grid's state expired it
	// is the least time that proves, never more.
	// A caller that knows how late a request may leave after its slot can
	// tell from it that no earlier request is still waiting to leave.
	Idle time.Duration
}

// Limiter is the send-rate limit shared by every worker (decision 0009). Its
// state and its clock live in Valkey. It hands out send slots spaced one
// interval apart across all workers, so the provider sees an even stream
// whatever the workers' own timing is.
type Limiter interface {
	// Take reserves up to r.N consecutive send slots on a channel, none later
	// than r.Lead+r.Horizon after the limiter's now. A slot that is not used is lost; a request
	// must never leave before its slot, nor more than one interval after it,
	// whatever the reply's delay was. An error means the limiter is
	// unreachable: the caller sends nothing (fail closed). With r.N = 0
	// nothing is reserved and the grant still carries At: a reading of the
	// limiter's clock that costs no slot.
	Take(ctx context.Context, channel Channel, r SlotRequest) (Grant, error)
	// GiveBack returns what is left of grant g from the moment from (on the
	// limiter's clock) on: if no slot was reserved on the channel after g,
	// the channel's next free slot moves back to from, so the next Take
	// gets it, or finds the grid free since then when it has passed. If
	// slots were reserved after g, the range is kept and returned once
	// everything reserved after it has been given back from its start, so
	// the order of give-backs does not matter. The caller sends nothing at
	// or after from on g's account, and every request it did send on g
	// leaves before from. It reports whether the next free slot moved back
	// now; false or an error only leaves the slots unused for now.
	GiveBack(ctx context.Context, channel Channel, g Grant, from time.Time) (bool, error)
}

// SlotRequest is what one Take asks for.
type SlotRequest struct {
	// N is the most slots wanted.
	N int
	// Lead is the time the caller needs between asking and sending (its
	// claim of rows for the slots). A free grid (no slot reserved after now)
	// restarts at now+Lead; a busy grid continues at its next free slot even
	// when that is sooner, since skipping ahead would leave a gap the other
	// callers cannot fill. Slots are send moments, so callers with different
	// leads still share one grid one interval apart.
	Lead time.Duration
	// Horizon is how far past Lead slots may be reserved.
	Horizon time.Duration
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
	// Seq is the notification's status_seq after this change: 1 at creation,
	// one more with every change. Subscribers order a notification's events
	// by it and drop what they already have. 0 means unknown (a publisher
	// that predates it); such an event is passed on unordered.
	Seq uint64
}

// Publisher fans status changes out to subscribers. It is called once per
// change, after the change is committed. Publishing is best effort: an error
// is logged and never undoes or blocks the change; the REST API stays the
// source of truth.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

// Wake is a hint that notifications of one channel and priority became
// pending (created, replayed, released, rescheduled or reaped), the earliest
// of them due at Due.
type Wake struct {
	Channel  Channel
	Priority Priority
	Due      time.Time
}

// Waker tells idle workers that work became pending. It is called after the
// change is committed, never before: a worker woken by it must find the row.
// Best effort: a lost hint delays a send by at most the workers' safety poll
// and never loses it; an error is logged and never undoes or blocks the
// change.
type Waker interface {
	Wake(ctx context.Context, hints ...Wake) error
}
