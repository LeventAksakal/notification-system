//go:build integration

package delivery_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// errProcessGone is what a result write returns from a worker whose process
// died after its requests left: nothing reaches the database.
var errProcessGone = errors.New("the process is gone")

// diesAfterSend lets a worker claim and begin its attempts as usual, then
// loses every write after the provider call, as a process killed between the
// request and the result would. It reaps nothing, so only the next worker
// finds the expired leases.
type diesAfterSend struct{ notification.Queue }

func (diesAfterSend) MarkSent(context.Context, notification.Job, notification.Attempt) error {
	return errProcessGone
}

func (diesAfterSend) Reschedule(context.Context, notification.Job, notification.Attempt, time.Time) error {
	return errProcessGone
}

func (diesAfterSend) MarkFailed(context.Context, notification.Job, notification.Attempt, notification.FailureKind) error {
	return errProcessGone
}

func (diesAfterSend) Release(context.Context, notification.Job, time.Time) error {
	return errProcessGone
}

func (diesAfterSend) ReapExpired(context.Context) ([]notification.Job, error) { return nil, nil }

// TestACallWhoseWorkerDiedStaysInTheHistory: a worker sends and dies before
// it records the results; the next worker's reaper marks those calls
// unknown, it calls again, and the provider's dedupe delivers each once
// (decision 0005, "Attempts written before the call").
func TestACallWhoseWorkerDiedStaysInTheHistory(t *testing.T) {
	const n = 10
	r := newRig(t) // the provider double dedupes by default
	ids := r.seed(t, notification.ChannelSMS, n)

	first := r.worker(t, nil, 2*time.Second, func(q notification.Queue) notification.Queue { return diesAfterSend{q} })
	waitFor(t, 15*time.Second, "the first worker's requests reach the provider", func() (bool, string) {
		got := r.prov.Stats().Channels["sms"].Received
		return got == n, fmt.Sprintf("%d of %d", got, n)
	})
	first()
	if p := r.count(t, "SELECT COUNT(*) FROM delivery_attempts WHERE outcome = 'in_flight'"); p != n {
		t.Fatalf("%d attempts in flight after the crash, want %d", p, n)
	}

	r.worker(t, nil, 2*time.Second, nil)
	waitFor(t, 30*time.Second, "the second worker sends them all", func() (bool, string) {
		got := r.count(t, "SELECT COUNT(*) FROM notifications WHERE status = 'sent'")
		return got == n, fmt.Sprintf("%d of %d sent", got, n)
	})
	for _, id := range ids {
		history := r.history(t, id)
		if want := []string{"attempt 1 unknown", "attempt 2 accepted"}; !slices.Equal(history, want) {
			t.Errorf("%s: history %v, want %v", id, history, want)
		}
	}
	st := r.prov.Stats()
	t.Logf("provider: maxRequestsPerKey %d, maxDeliveriesPerKey %d", st.MaxRequestsPerKey, st.MaxDeliveriesPerKey)
	if st.MaxRequestsPerKey != 2 || st.MaxDeliveriesPerKey != 1 {
		t.Errorf("want two requests and one delivery per key, got %d and %d", st.MaxRequestsPerKey, st.MaxDeliveriesPerKey)
	}
}

// history lists the attempts of id as "attempt <no> <outcome>", in order.
func (r *rig) history(t *testing.T, id string) []string {
	t.Helper()
	rows, err := r.db.QueryContext(t.Context(), `SELECT attempt_no, outcome FROM delivery_attempts WHERE notification_id = ? ORDER BY attempt_no`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var no int
		var outcome string
		if err := rows.Scan(&no, &outcome); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("attempt %d %s", no, outcome))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
