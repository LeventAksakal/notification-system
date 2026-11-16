//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/migrations"
	"github.com/LeventAksakal/notification-system/test/support"
)

// recorder is a notification.Publisher that keeps what it was given.
type recorder struct {
	mu sync.Mutex
	es []notification.Event
}

func (r *recorder) Publish(_ context.Context, e notification.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.es = append(r.es, e)
	return nil
}

func (r *recorder) last(t *testing.T, id string) notification.Event {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.es) - 1; i >= 0; i-- {
		if r.es[i].NotificationID == id {
			return r.es[i]
		}
	}
	t.Fatalf("nothing published for %s", id)
	return notification.Event{}
}

func seqOf(t *testing.T, db *sql.DB, id string) uint64 {
	t.Helper()
	var s uint64
	if err := db.QueryRowContext(t.Context(), `SELECT status_seq FROM notifications WHERE id = ?`, id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// Every status change raises status_seq by exactly one, and the writer knows
// the new value without reading it back: the number it publishes is the
// stored one (decision 0013).
func TestStatusSeq(t *testing.T) {
	db, _ := support.MySQL(t)
	q := store.NewQueue(db)
	attempt := notification.Attempt{StartedAt: time.Now(), Outcome: notification.OutcomeTransient, Error: "x"}

	// First, while the table is new: the migration adds the column to rows
	// that already exist, at 1.
	t.Run("the migration gives existing rows status_seq 1", func(t *testing.T) {
		p, err := goose.NewProvider(goose.DialectMySQL, db, migrations.FS)
		if err != nil {
			t.Fatal(err)
		}
		const before = 20261115233413 // the migration before status_seq
		if _, err := p.DownTo(t.Context(), before); err != nil {
			t.Fatalf("down to %d: %v", before, err)
		}
		ids := insertDue(t, db, 3, notification.ChannelSMS, notification.PriorityNormal)
		if _, err := db.ExecContext(t.Context(), `UPDATE notifications SET status = 'cancelled', finished_at = NOW(3) WHERE id = ?`, ids[0]); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Up(t.Context()); err != nil {
			t.Fatalf("up: %v", err)
		}
		for _, id := range ids {
			if s := seqOf(t, db, id); s != 1 {
				t.Errorf("%s after the migration: status_seq %d, want 1", id, s)
			}
		}
		if _, err := db.ExecContext(t.Context(), `DELETE FROM notifications WHERE id IN (?, ?, ?)`, ids[0], ids[1], ids[2]); err != nil {
			t.Fatal(err)
		}
	})

	// The triggers own the column: whatever a statement writes to status_seq,
	// an insert stores 1, a status change stores the old value plus one and
	// any other update keeps it. The CHECK (status_seq >= 1) is the backstop
	// for a database without the triggers; through them it cannot fail.
	t.Run("the triggers set status_seq whatever a statement writes", func(t *testing.T) {
		ctx := t.Context()
		exec := func(q string, args ...any) {
			t.Helper()
			if _, err := db.ExecContext(ctx, q, args...); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		id := uuid.Must(uuid.NewV7()).String()
		exec(`INSERT INTO notifications (id, recipient, channel, priority, content, status_seq, due_at, correlation_id)
			VALUES (?, 'device-1', 'push', 'high', 'hello', 7, NOW(3) + INTERVAL 1 HOUR, 'corr')`, id)
		steps := []struct {
			name string
			sql  string
			want uint64
		}{
			{"an insert that writes 7 stores 1", ``, 1},
			{"an update that writes 99 without a status change keeps 1",
				`UPDATE notifications SET content = 'changed', status_seq = 99 WHERE id = ?`, 1},
			{"a status change that does not mention status_seq raises it to 2",
				`UPDATE notifications SET status = 'processing', lease_until = NOW(3) + INTERVAL 1 MINUTE WHERE id = ?`, 2},
			{"rewriting the same status with status_seq 0 keeps 2",
				`UPDATE notifications SET status = 'processing', status_seq = 0 WHERE id = ?`, 2},
			{"a status change that writes old + 5 stores old + 1",
				`UPDATE notifications SET status = 'pending', lease_until = NULL, status_seq = status_seq + 5 WHERE id = ?`, 3},
		}
		for _, st := range steps {
			if st.sql != "" {
				exec(st.sql, id)
			}
			if s := seqOf(t, db, id); s != st.want {
				t.Errorf("%s: stored %d", st.name, s)
			}
		}
		exec(`DELETE FROM notifications WHERE id = ?`, id)
	})

	claimOne := func(t *testing.T, lease time.Duration) notification.Job {
		t.Helper()
		id := insertDue(t, db, 1, notification.ChannelEmail, notification.PriorityLow)[0]
		if s := seqOf(t, db, id); s != 1 {
			t.Fatalf("created: status_seq %d, want 1", s)
		}
		jobs, err := q.Claim(t.Context(), notification.ChannelEmail, notification.PriorityLow, 1, lease)
		if err != nil || len(jobs) != 1 || jobs[0].ID != id {
			t.Fatalf("claim: %v %+v", err, jobs)
		}
		if s := seqOf(t, db, id); jobs[0].StatusSeq != 2 || s != 2 {
			t.Fatalf("claimed: job says %d, stored %d, want 2", jobs[0].StatusSeq, s)
		}
		return jobs[0]
	}

	for _, tc := range []struct {
		name  string
		write func(context.Context, notification.Job) error
	}{
		{"mark sent", func(ctx context.Context, j notification.Job) error {
			return q.MarkSent(ctx, j, notification.Attempt{StartedAt: time.Now(), Outcome: notification.OutcomeAccepted, HTTPStatus: http.StatusOK})
		}},
		{"reschedule", func(ctx context.Context, j notification.Job) error {
			return q.Reschedule(ctx, j, attempt, time.Now().Add(time.Hour))
		}},
		{"mark failed", func(ctx context.Context, j notification.Job) error {
			return q.MarkFailed(ctx, j, attempt, notification.FailureExhausted)
		}},
		{"release", func(ctx context.Context, j notification.Job) error {
			return q.Release(ctx, j, time.Now().Add(time.Hour))
		}},
	} {
		t.Run(tc.name+" stores the claim's status_seq plus one", func(t *testing.T) {
			j := claimOne(t, time.Minute)
			if err := tc.write(t.Context(), j); err != nil {
				t.Fatal(err)
			}
			if s := seqOf(t, db, j.ID); s != j.StatusSeq+1 {
				t.Fatalf("stored %d, the worker publishes %d", s, j.StatusSeq+1)
			}
			// A second write with the same claim lost its lease: no change.
			if err := tc.write(t.Context(), j); !errors.Is(err, notification.ErrLeaseLost) {
				t.Fatalf("second write: want ErrLeaseLost, got %v", err)
			}
			if s := seqOf(t, db, j.ID); s != j.StatusSeq+1 {
				t.Fatalf("after a fenced-out write: stored %d, want %d", s, j.StatusSeq+1)
			}
		})
	}

	t.Run("reap reports the stored status_seq", func(t *testing.T) {
		j := claimOne(t, time.Millisecond)
		time.Sleep(20 * time.Millisecond) // let the lease run out on the database clock
		reaped, err := q.ReapExpired(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var got *notification.Job
		for i := range reaped {
			if reaped[i].ID == j.ID {
				got = &reaped[i]
			}
		}
		if got == nil {
			t.Fatalf("%s not reaped", j.ID)
		}
		if s := seqOf(t, db, j.ID); got.StatusSeq != 3 || s != 3 {
			t.Fatalf("reaped: reported %d, stored %d, want 3", got.StatusSeq, s)
		}
		// Out of the way of the next claims (it is due now). The reaper
		// records the expired claim as an attempt (decision 0026), so that
		// row goes first.
		for _, q := range []string{`DELETE FROM delivery_attempts WHERE notification_id = ?`, `DELETE FROM notifications WHERE id = ?`} {
			if _, err := db.ExecContext(t.Context(), q, j.ID); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("create, cancel and replay publish the stored status_seq", func(t *testing.T) {
		rec := &recorder{}
		svc := intake.New(db, rec, time.Hour)
		c := "hello"
		n, _, err := svc.Create(t.Context(), intake.Request{To: "+905551234567", Channel: notification.ChannelSMS, Content: &c,
			ScheduledAt: new(time.Now().Add(time.Hour))}, intake.Meta{CorrelationID: "c"})
		if err != nil {
			t.Fatal(err)
		}
		if e := rec.last(t, n.ID); e.Seq != 1 || e.Status != notification.StatusPending {
			t.Fatalf("create published %s(%d), want pending(1)", e.Status, e.Seq)
		}
		if _, err := svc.Cancel(t.Context(), n.ID); err != nil {
			t.Fatal(err)
		}
		if e, s := rec.last(t, n.ID), seqOf(t, db, n.ID); e.Seq != 2 || s != 2 || e.Status != notification.StatusCancelled {
			t.Fatalf("cancel published %s(%d), stored %d, want cancelled(2)", e.Status, e.Seq, s)
		}

		f := claimOne(t, time.Minute)
		if err := q.MarkFailed(t.Context(), f, attempt, notification.FailurePermanent); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Retry(t.Context(), f.ID); err != nil {
			t.Fatal(err)
		}
		if e, s := rec.last(t, f.ID), seqOf(t, db, f.ID); e.Seq != 4 || s != 4 || e.Status != notification.StatusPending || e.Attempts != 0 {
			t.Fatalf("replay published %s(%d) attempts %d, stored %d, want pending(4) attempts 0", e.Status, e.Seq, e.Attempts, s)
		}
	})

	t.Run("concurrent claims and cancels: one wins each row, its status_seq is the stored one", func(t *testing.T) {
		const rows = 200
		ids := insertDue(t, db, rows, notification.ChannelPush, notification.PriorityNormal)
		rec := &recorder{}
		svc := intake.New(db, rec, time.Hour)
		var mu sync.Mutex
		claimed := map[string]uint64{}
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for {
					jobs, err := q.Claim(t.Context(), notification.ChannelPush, notification.PriorityNormal, 2, time.Minute)
					if err != nil {
						t.Error(err)
						return
					}
					if len(jobs) == 0 {
						return
					}
					mu.Lock()
					for _, j := range jobs {
						claimed[j.ID] = j.StatusSeq
					}
					mu.Unlock()
				}
			})
		}
		cancelled := map[string]bool{}
		// Claims take the oldest due first; cancels start from the newest, so
		// the two meet on the same rows.
		wg.Go(func() {
			for i := range ids {
				id := ids[len(ids)-1-i]
				_, err := svc.Cancel(t.Context(), id)
				switch {
				case err == nil:
					mu.Lock()
					cancelled[id] = true
					mu.Unlock()
				case !errors.Is(err, intake.ErrNotCancellable):
					t.Error(err)
				}
			}
		})
		wg.Wait()
		for _, id := range ids {
			s := seqOf(t, db, id)
			seq, isClaimed := claimed[id]
			switch {
			case isClaimed && cancelled[id]:
				t.Fatalf("%s both claimed and cancelled", id)
			case isClaimed && (seq != 2 || s != 2):
				t.Fatalf("%s claimed: job %d, stored %d, want 2", id, seq, s)
			case cancelled[id] && (rec.last(t, id).Seq != 2 || s != 2):
				t.Fatalf("%s cancelled: published %d, stored %d, want 2", id, rec.last(t, id).Seq, s)
			case !isClaimed && !cancelled[id]:
				t.Fatalf("%s neither claimed nor cancelled (stored %d)", id, s)
			}
		}
		t.Logf("%d claimed, %d cancelled", len(claimed), len(cancelled))
		if len(cancelled) == 0 || len(claimed) == 0 {
			t.Fatalf("no contention: %d claimed, %d cancelled", len(claimed), len(cancelled))
		}
	})
}
