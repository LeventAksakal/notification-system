package delivery

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// memQueue is an in-memory notification.Queue with the same fencing rules.
type memQueue struct {
	mu       sync.Mutex
	rows     map[string]*memRow
	order    []string
	claims   map[notification.Priority]int
	results  map[string][]notification.AttemptOutcome
	released int
	lost     int // result writes refused because the lease had run out
	peeks    int // NextDue calls
	// claimCalls counts Claim calls, empty ones included; asks counts them
	// per priority.
	claimCalls int
	asks       map[notification.Priority]int
}

type memRow struct {
	job    notification.Job
	status notification.Status
	kind   notification.FailureKind
	due    time.Time
}

func newMemQueue() *memQueue {
	return &memQueue{rows: map[string]*memRow{}, claims: map[notification.Priority]int{}, asks: map[notification.Priority]int{}, results: map[string][]notification.AttemptOutcome{}}
}

func (q *memQueue) add(n int, ch notification.Channel, p notification.Priority) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for range n {
		id := fmt.Sprintf("%s-%s-%d", ch, p, len(q.order))
		q.rows[id] = &memRow{job: notification.Job{ID: id, Channel: ch, Priority: p, CorrelationID: "c-" + id}, status: notification.StatusPending}
		q.order = append(q.order, id)
	}
}

func (q *memQueue) Claim(_ context.Context, ch notification.Channel, p notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.claimCalls++
	q.asks[p]++
	// Due rows of this lane, oldest due first, as the store's claim (ORDER BY
	// due_at); ties in insertion order.
	now := time.Now()
	var due []*memRow
	for _, id := range q.order {
		r := q.rows[id]
		if r.status == notification.StatusPending && r.job.Channel == ch && r.job.Priority == p && !now.Before(r.due) {
			due = append(due, r)
		}
	}
	slices.SortStableFunc(due, func(a, b *memRow) int { return a.due.Compare(b.due) })
	out := make([]notification.Job, 0, min(n, len(due)))
	for _, r := range due[:min(n, len(due))] {
		r.status = notification.StatusProcessing
		r.job.ClaimVersion++
		r.job.LeaseUntil = now.Add(lease)
		out = append(out, r.job)
		q.claims[p]++
	}
	return out, nil
}

func (q *memQueue) finish(job notification.Job, a *notification.Attempt, to notification.Status, kind notification.FailureKind, due time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	r := q.rows[job.ID]
	if r == nil || r.status != notification.StatusProcessing || r.job.ClaimVersion != job.ClaimVersion {
		return notification.ErrLeaseLost
	}
	if time.Now().After(r.job.LeaseUntil) {
		// As if the reaper had taken it back: the claimer's write is fenced off.
		q.lost++
		return notification.ErrLeaseLost
	}
	r.status, r.kind, r.due = to, kind, due
	switch {
	case a == nil:
		q.released++
	case a.Outcome == "":
		// Failed without a call (the budget was spent): counts unchanged.
	default:
		r.job.Attempts++
		if to == notification.StatusFailed || a.Counted && to == notification.StatusPending {
			r.job.BudgetSpent++ // as the store: a failure counts when Counted, a final failure always
		}
		q.results[job.ID] = append(q.results[job.ID], a.Outcome)
	}
	return nil
}

func (q *memQueue) MarkSent(_ context.Context, j notification.Job, a notification.Attempt) error {
	return q.finish(j, &a, notification.StatusSent, "", time.Time{})
}

func (q *memQueue) Reschedule(_ context.Context, j notification.Job, a notification.Attempt, due time.Time) error {
	return q.finish(j, &a, notification.StatusPending, "", due)
}

func (q *memQueue) MarkFailed(_ context.Context, j notification.Job, a notification.Attempt, k notification.FailureKind) error {
	return q.finish(j, &a, notification.StatusFailed, k, time.Time{})
}

func (q *memQueue) Release(_ context.Context, j notification.Job, due time.Time) error {
	return q.finish(j, nil, notification.StatusPending, "", due)
}

func (q *memQueue) ReapExpired(context.Context) ([]notification.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []notification.Job
	for _, id := range q.order {
		r := q.rows[id]
		if r.status == notification.StatusProcessing && time.Now().After(r.job.LeaseUntil) {
			r.status = notification.StatusPending
			out = append(out, notification.Job{ID: r.job.ID, BatchID: r.job.BatchID, Channel: r.job.Channel,
				Attempts: r.job.Attempts, CorrelationID: r.job.CorrelationID})
		}
	}
	return out, nil
}

func (q *memQueue) NextDue(_ context.Context, ch notification.Channel) (map[notification.Priority]time.Duration, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.peeks++
	out := map[notification.Priority]time.Duration{}
	now := time.Now()
	for _, r := range q.rows {
		if r.status != notification.StatusPending || r.job.Channel != ch {
			continue
		}
		w := r.due.Sub(now)
		if cur, ok := out[r.job.Priority]; !ok || w < cur {
			out[r.job.Priority] = w
		}
	}
	return out, nil
}

func (q *memQueue) count(st notification.Status) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, r := range q.rows {
		if r.status == st {
			n++
		}
	}
	return n
}

func (q *memQueue) row(id string) memRow {
	q.mu.Lock()
	defer q.mu.Unlock()
	return *q.rows[id]
}
