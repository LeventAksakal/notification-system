//go:build integration

package intake

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

type nopPub struct{}

func (nopPub) Publish(context.Context, notification.Event) error { return nil }

// rescheduleFixture is one MySQL with a service and a queue on it.
type rescheduleFixture struct {
	db  *sql.DB
	svc *Service
	q   *store.Queue
}

func newRescheduleFixture(t *testing.T) rescheduleFixture {
	t.Helper()
	db, _ := support.MySQL(t)
	return rescheduleFixture{db: db, svc: New(db, nopPub{}, time.Hour), q: store.NewQueue(db)}
}

// dbNow reads the database clock, the one the schedule rules use.
func (f rescheduleFixture) dbNow(t *testing.T) time.Time {
	t.Helper()
	now, err := f.svc.q.DatabaseNow(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return now
}

// create stores one push notification on its own channel and priority, so a
// claim in the test sees only the rows that test made.
func (f rescheduleFixture) create(t *testing.T, p notification.Priority, at *time.Time) Notification {
	t.Helper()
	c := "reschedule me"
	n, _, err := f.svc.Create(t.Context(), Request{To: "device-1", Channel: notification.ChannelPush, Content: &c, Priority: p, ScheduledAt: at}, Meta{CorrelationID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// claimAll leases every due push notification of priority p.
func (f rescheduleFixture) claimAll(t *testing.T, p notification.Priority) []notification.Job {
	t.Helper()
	jobs, err := f.q.Claim(t.Context(), notification.ChannelPush, p, 1000, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func TestRescheduleMovesOnlyNotificationsNoWorkerPickedUp(t *testing.T) {
	f := newRescheduleFixture(t)
	ctx := t.Context()
	now := f.dbNow(t)
	inADay := now.Add(24 * time.Hour).Truncate(time.Millisecond)
	inAWeek := now.Add(7 * 24 * time.Hour).Truncate(time.Millisecond)

	t.Run("a scheduled notification moves to the new time", func(t *testing.T) {
		n := f.create(t, notification.PriorityHigh, &inADay)
		got, err := f.svc.Reschedule(ctx, n.ID, &inAWeek)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status() != "scheduled" || !got.ScheduledAt.Time.Equal(inAWeek) || !got.DueAt.Equal(inAWeek) {
			t.Fatalf("got status %s scheduledAt %v dueAt %v, want scheduled at %v", got.Status(), got.ScheduledAt.Time, got.DueAt, inAWeek)
		}
	})
	t.Run("null makes it due now and clears the schedule", func(t *testing.T) {
		n := f.create(t, notification.PriorityHigh, &inADay)
		got, err := f.svc.Reschedule(ctx, n.ID, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status() != "pending" || got.ScheduledAt.Valid || got.DueAt.After(f.dbNow(t)) {
			t.Fatalf("got status %s scheduledAt %v dueAt %v, want pending, no schedule, due", got.Status(), got.ScheduledAt, got.DueAt)
		}
	})
	t.Run("a past time is kept and means due now", func(t *testing.T) {
		n := f.create(t, notification.PriorityHigh, &inADay)
		past := now.Add(-time.Hour).Truncate(time.Millisecond)
		got, err := f.svc.Reschedule(ctx, n.ID, &past)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status() != "pending" || !got.ScheduledAt.Time.Equal(past) || got.DueAt.Before(now) {
			t.Fatalf("got status %s scheduledAt %v dueAt %v, want pending, scheduledAt %v, due from %v", got.Status(), got.ScheduledAt.Time, got.DueAt, past, now)
		}
	})
	t.Run("a due notification not yet claimed can be put off", func(t *testing.T) {
		n := f.create(t, notification.PriorityHigh, nil)
		got, err := f.svc.Reschedule(ctx, n.ID, &inADay)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status() != "scheduled" || !got.DueAt.Equal(inADay) {
			t.Fatalf("got status %s dueAt %v, want scheduled at %v", got.Status(), got.DueAt, inADay)
		}
	})
	// The second call writes the values the row holds, so MySQL reports 0
	// changed rows: the branch that tells a no-op from a refusal.
	t.Run("the same schedule twice is accepted both times", func(t *testing.T) {
		n := f.create(t, notification.PriorityHigh, &inADay)
		for i := range 2 {
			if _, err := f.svc.Reschedule(ctx, n.ID, &inADay); err != nil {
				t.Fatalf("reschedule %d: %v", i+1, err)
			}
		}
	})
	t.Run("beyond the horizon is refused naming scheduledAt", func(t *testing.T) {
		n := f.create(t, notification.PriorityHigh, &inADay)
		far := f.dbNow(t).Add(notification.ScheduleHorizon + time.Minute)
		_, err := f.svc.Reschedule(ctx, n.ID, &far)
		var ve *ValidationError
		if !errors.As(err, &ve) || ve.Details[0].Field != "scheduledAt" {
			t.Fatalf("got %v, want a ValidationError on scheduledAt", err)
		}
		got, _ := f.svc.Get(ctx, n.ID)
		if !got.DueAt.Equal(inADay) {
			t.Fatalf("a refused reschedule changed dueAt to %v", got.DueAt)
		}
	})
	t.Run("just inside the horizon is accepted", func(t *testing.T) {
		n := f.create(t, notification.PriorityHigh, &inADay)
		edge := f.dbNow(t).Add(notification.ScheduleHorizon - time.Second)
		if _, err := f.svc.Reschedule(ctx, n.ID, &edge); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("an unknown id is not found", func(t *testing.T) {
		if _, err := f.svc.Reschedule(ctx, "01a1e115-35f4-713d-8d33-fbf2f9af0c3c", &inADay); !errors.Is(err, ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	// The refusals, each from a state a worker or a client put the row in.
	refused := func(t *testing.T, id, wantStatus string) {
		t.Helper()
		before, _ := f.svc.Get(ctx, id)
		_, err := f.svc.Reschedule(ctx, id, &inAWeek)
		if !errors.Is(err, ErrNotReschedulable) {
			t.Fatalf("got %v, want ErrNotReschedulable", err)
		}
		after, _ := f.svc.Get(ctx, id)
		if after.Status() != wantStatus || !after.DueAt.Equal(before.DueAt) {
			t.Fatalf("status %s dueAt %v after the refusal, want %s and dueAt %v unchanged", after.Status(), after.DueAt, wantStatus, before.DueAt)
		}
	}
	t.Run("a claimed notification is refused", func(t *testing.T) {
		n := f.create(t, notification.PriorityNormal, nil)
		if jobs := f.claimAll(t, notification.PriorityNormal); len(jobs) != 1 {
			t.Fatalf("claimed %d, want 1", len(jobs))
		}
		refused(t, n.ID, "processing")
	})
	t.Run("one waiting on a retry backoff is refused", func(t *testing.T) {
		n := f.create(t, notification.PriorityLow, nil)
		jobs := f.claimAll(t, notification.PriorityLow)
		if len(jobs) != 1 {
			t.Fatalf("claimed %d, want 1", len(jobs))
		}
		if err := f.q.Reschedule(ctx, jobs[0], notification.Attempt{StartedAt: time.Now(), Outcome: notification.OutcomeTransient, HTTPStatus: 503, Error: "busy"}, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		refused(t, n.ID, "pending")
	})
	t.Run("one released without an attempt is refused", func(t *testing.T) {
		n := f.create(t, notification.PriorityLow, nil)
		jobs := f.claimAll(t, notification.PriorityLow)
		if len(jobs) != 1 {
			t.Fatalf("claimed %d, want 1", len(jobs))
		}
		// A breaker-open release puts it back with attempts still 0, so the
		// API shows it as scheduled; it still carries the release's due time.
		if err := f.q.Release(ctx, jobs[0], time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		refused(t, n.ID, "scheduled")
	})
	t.Run("a cancelled notification is refused", func(t *testing.T) {
		n := f.create(t, notification.PriorityHigh, &inADay)
		if _, err := f.svc.Cancel(ctx, n.ID); err != nil {
			t.Fatal(err)
		}
		refused(t, n.ID, "cancelled")
	})
}

// A reschedule and a claim on the same due row: exactly one wins, and the
// loser changes nothing (no lost update). Run with -race.
func TestRescheduleRacingAClaimHasOneWinner(t *testing.T) {
	f := newRescheduleFixture(t)
	ctx := t.Context()
	// Rounds, arbitrary: enough that both orders occur on a laptop (each
	// round is one claim and one UPDATE racing), few enough to take seconds.
	const rounds = 100
	// The claim is a transaction of two statements and a commit, the
	// reschedule one statement, so started together the reschedule nearly
	// always wins (99 of 100 when measured). Its start is held back by 0 to
	// 9 steps of this size, arbitrary, about one claim round trip here, so
	// rounds land on both sides and in the overlap.
	const stagger = 300 * time.Microsecond
	later := f.dbNow(t).Add(24 * time.Hour).Truncate(time.Millisecond)
	var claimWins, rescheduleWins int
	for round := range rounds {
		n := f.create(t, notification.PriorityHigh, nil)
		var (
			wg      sync.WaitGroup
			jobs    []notification.Job
			claimEr error
			resErr  error
		)
		start := make(chan struct{})
		wg.Go(func() {
			<-start
			jobs, claimEr = f.q.Claim(ctx, notification.ChannelPush, notification.PriorityHigh, 10, time.Minute)
		})
		wg.Go(func() {
			<-start
			time.Sleep(time.Duration(round%10) * stagger)
			_, resErr = f.svc.Reschedule(ctx, n.ID, &later)
		})
		close(start)
		wg.Wait()
		if claimEr != nil {
			t.Fatalf("round %d: claim: %v", round, claimEr)
		}
		claimed := len(jobs) == 1 && jobs[0].ID == n.ID
		rescheduled := resErr == nil
		if resErr != nil && !errors.Is(resErr, ErrNotReschedulable) {
			t.Fatalf("round %d: reschedule: %v", round, resErr)
		}
		got, err := f.svc.Get(ctx, n.ID)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case claimed && rescheduled:
			t.Fatalf("round %d: both won; stored status %s dueAt %v", round, got.Status(), got.DueAt)
		case !claimed && !rescheduled:
			t.Fatalf("round %d: neither won; stored status %s dueAt %v", round, got.Status(), got.DueAt)
		case claimed:
			claimWins++
			if got.Status() != "processing" || got.ScheduledAt.Valid {
				t.Fatalf("round %d: the claim won but the row is %s with scheduledAt %v", round, got.Status(), got.ScheduledAt)
			}
		default:
			rescheduleWins++
			if got.Status() != "scheduled" || !got.DueAt.Equal(later) || got.ClaimVersion != 0 {
				t.Fatalf("round %d: the reschedule won but the row is %s dueAt %v claim_version %d", round, got.Status(), got.DueAt, got.ClaimVersion)
			}
		}
		// Leave nothing due for the next round's claim.
		if claimed {
			if err := f.q.MarkSent(ctx, jobs[0], notification.Attempt{StartedAt: time.Now(), Outcome: notification.OutcomeAccepted, HTTPStatus: 202}); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("claim won %d, reschedule won %d of %d rounds", claimWins, rescheduleWins, rounds)
}

// The horizon is measured on the database clock, whatever the API process's
// clock says (decision 0016): the service's own clock is skewed by more than
// the horizon both ways and the answers do not move.
func TestTheScheduleHorizonIsMeasuredOnTheDatabaseClock(t *testing.T) {
	f := newRescheduleFixture(t)
	skew := notification.ScheduleHorizon + 10*24*time.Hour
	for _, tc := range []struct {
		name   string
		skew   time.Duration
		ahead  time.Duration // scheduledAt minus the database clock
		accept bool
	}{
		{"process clock behind, inside the horizon: accepted", -skew, notification.ScheduleHorizon - time.Hour, true},
		{"process clock ahead, past the horizon: refused", skew, notification.ScheduleHorizon + time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.svc.now = func() time.Time { return time.Now().Add(tc.skew) }
			t.Cleanup(func() { f.svc.now = time.Now })
			at := f.dbNow(t).Add(tc.ahead)
			c := "clock"
			r := Request{To: "device-1", Channel: notification.ChannelPush, Content: &c, ScheduledAt: &at}
			_, _, errOne := f.svc.Create(t.Context(), r, Meta{CorrelationID: "test"})
			_, _, errBatch := f.svc.CreateBatch(t.Context(), []Request{r, r}, Meta{CorrelationID: "test"})
			for what, err := range map[string]error{"create": errOne, "batch": errBatch} {
				var ve *ValidationError
				switch {
				case tc.accept && err != nil:
					t.Errorf("%s: got %v, want accepted", what, err)
				case !tc.accept && (!errors.As(err, &ve) || ve.Details[0].Field != "scheduledAt"):
					t.Errorf("%s: got %v, want a ValidationError on scheduledAt", what, err)
				case !tc.accept && what == "batch" && ve.Details[0].Index == nil:
					// Only the service's check names the item; the database
					// CHECK refuses the batch as a whole. An index proves the
					// service refused it, on the database clock.
					t.Errorf("batch: refused without an item index, so by the CHECK, not the service: %v", err)
				}
			}
		})
	}
}
