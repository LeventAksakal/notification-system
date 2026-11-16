//go:build integration

package realtime_test

import (
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/goleak"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/realtime"
	"github.com/LeventAksakal/notification-system/internal/realtime/realtimetest"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

// collect reads sub through order until nothing arrived or came out for
// quiet; it returns what came out and the raw arrival order.
func collect(t *testing.T, sub *realtime.Subscription, order *realtime.Reorderer, quiet time.Duration) (out, raw []notification.Event) {
	t.Helper()
	hold := time.NewTimer(time.Hour)
	defer hold.Stop()
	rearm := func() {
		hold.Stop()
		if at, ok := order.Deadline(); ok {
			hold.Reset(max(time.Until(at), 0))
		}
	}
	idle := time.NewTimer(quiet)
	defer idle.Stop()
	for {
		select {
		case e, ok := <-sub.C:
			if !ok {
				t.Fatalf("subscription ended: %v", sub.Err())
			}
			raw = append(raw, e)
			out = append(out, order.Push(e, time.Now())...)
			rearm()
		case <-hold.C:
			out = append(out, order.Expire(time.Now())...)
			rearm()
		case <-idle.C:
			return out, raw
		}
		idle.Reset(quiet)
	}
}

func render(es []notification.Event) string {
	s := ""
	for _, e := range es {
		s += fmt.Sprintf("%s(%d) ", e.Status, e.Seq)
	}
	return s
}

// Owner item 0009: a worker releases a job while its Valkey is slow, another
// worker claims and sends it over a fast one. The release's pending reaches
// Valkey after the reclaim's processing and sent. The subscriber must never
// see an older state after a newer one: with the delay inside the hold (2 s)
// it sees every state in commit order; past the hold, the late pending is
// dropped, also after the notification finished.
func TestReleaseAndReclaimRaceReachesTheSubscriberInCommitOrder(t *testing.T) {
	db, _ := support.MySQL(t)
	addr := support.Valkey(t)
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	q := store.NewQueue(db)

	for _, tc := range []struct {
		name  string
		delay time.Duration // on the releasing worker's Valkey
		want  string
	}{
		{"pending 300 ms late, within the hold: every state, in order", 300 * time.Millisecond,
			"pending(3) processing(4) sent(5) "},
		{"pending 1.5 s late (its publish timed out, the bytes still land), within the hold: every state, in order", 1500 * time.Millisecond,
			"pending(3) processing(4) sent(5) "},
		{"pending 3 s late, past the hold: skipped, never shown after sent", 3 * time.Second,
			"processing(4) sent(5) "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxy := realtimetest.NewProxy(t, addr)
			defer proxy.Close()
			slowClient, fastClient, subClient := client(t, proxy.Addr), client(t, addr), client(t, addr)
			defer slowClient.Close()
			defer fastClient.Close()
			defer subClient.Close()
			workerA, workerB := realtime.NewPublisher(slowClient), realtime.NewPublisher(fastClient)
			h, stop := startHub(t, subClient)
			defer stop()

			id := insertPending(t, db)
			jobsA, err := q.Claim(t.Context(), notification.ChannelPush, notification.PriorityNormal, 1, time.Minute)
			if err != nil || len(jobsA) != 1 || jobsA[0].ID != id {
				t.Fatalf("worker A claim: %v %+v", err, jobsA)
			}
			a := jobsA[0]

			sub, err := h.Subscribe(realtime.Filter{NotificationID: id}, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			order := realtime.NewReorderer()
			order.Seed(id, storedSeq(t, db, id)) // the snapshot: processing(2)

			proxy.SetDelay(tc.delay)
			if err := q.Release(t.Context(), a, time.Now()); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				// A delay past publishTimeout fails the call, but the bytes
				// still reach Valkey late: the late predecessor.
				_ = workerA.Publish(t.Context(), event(a, notification.StatusPending, a.StatusSeq+1))
			}()
			jobsB, err := q.Claim(t.Context(), notification.ChannelPush, notification.PriorityNormal, 1, time.Minute)
			if err != nil || len(jobsB) != 1 {
				t.Fatalf("worker B claim: %v %+v", err, jobsB)
			}
			b := jobsB[0]
			if err := workerB.Publish(t.Context(), event(b, notification.StatusProcessing, b.StatusSeq)); err != nil {
				t.Fatal(err)
			}
			if err := q.MarkSent(t.Context(), b, notification.Attempt{StartedAt: time.Now(), Outcome: notification.OutcomeAccepted, HTTPStatus: http.StatusOK}); err != nil {
				t.Fatal(err)
			}
			if err := workerB.Publish(t.Context(), event(b, notification.StatusSent, b.StatusSeq+1)); err != nil {
				t.Fatal(err)
			}

			out, raw := collect(t, sub, order, tc.delay+time.Second)
			wg.Wait()
			t.Logf("arrived %s; passed on %s", render(raw), render(out))
			if raw[0].Seq == 3 {
				t.Fatalf("the race did not happen: pending arrived first (raw %s)", render(raw))
			}
			if got := render(out); got != tc.want {
				t.Fatalf("subscriber saw %s(arrived %s), want %s", got, render(raw), tc.want)
			}
			if s := storedSeq(t, db, id); s != 5 {
				t.Fatalf("stored status_seq: want 5 (created, claimed, released, claimed, sent), got %d", s)
			}
		})
	}
}

func insertPending(t *testing.T, db *sql.DB) string {
	t.Helper()
	id := uuid.Must(uuid.NewV7()).String()
	if _, err := db.ExecContext(t.Context(), `INSERT INTO notifications (id, recipient, channel, priority, content, due_at, correlation_id)
		VALUES (?, 'device-1', 'push', 'normal', 'hello', NOW(3) - INTERVAL 1 SECOND, 'corr')`, id); err != nil {
		t.Fatal(err)
	}
	return id
}

func storedSeq(t *testing.T, db *sql.DB, id string) uint64 {
	t.Helper()
	var s uint64
	if err := db.QueryRowContext(t.Context(), `SELECT status_seq FROM notifications WHERE id = ?`, id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func event(j notification.Job, st notification.Status, seq uint64) notification.Event {
	return notification.Event{NotificationID: j.ID, Status: st, Attempts: j.Attempts, At: time.Now().UTC(),
		CorrelationID: j.CorrelationID, Seq: seq}
}
