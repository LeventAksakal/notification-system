# Reschedule lane: acceptance tests held by the guard (owner item 0002)

Written by test-writer on 16 Nov 2026 for requirement rows B2.1 (reschedule) and B2.2 (one clock); agentguard refused `test/acceptance/schedule_reschedule_test.go` from the worktree. Not run, not vetted. After the lane lands, have test-writer write it in the main checkout, vet it, and run `go test -tags integration ./test/acceptance -run 'TestB21|TestB22' -v`.

| Test | Asserts |
|---|---|
| `TestB21RescheduleMovesAScheduledNotification` | A notification scheduled +1h and moved to +4 s answers 200 scheduled with scheduledAt = dueAt = the new time; it is not at the provider before the new time (200 ms skew allowance, as `TestDeliveryB2ScheduledDelivery`) and is sent once within 2.3 s after it. PATCH null and PATCH to a past time: pending, dueAt at the PATCH, scheduledAt absent or as given, both sent within 2.3 s |
| `TestB21RescheduleRefusals` | 404 unknown id; 400 past the horizon naming scheduledAt with the schedule unchanged (then a PATCH within the horizon succeeds); 409 `not_reschedulable` naming the status after cancel, after sent, while claimed (push call held 3 s: "processing"), and while waiting on a retry (email answers 429 Retry-After 30) |
| `TestB21RescheduleRacingAWorkerHasOneWinner` | 50 due notifications, one worker; each PATCH to +1h follows its create by 0 to 225 ms. A 200 never reaches the provider and still reads scheduled 3 s later; a 409 is sent exactly once; both outcomes occur |
| `TestB22HorizonUsesTheDatabaseClock` | Host and database clocks within 10 s (precondition); create and PATCH at now + 30 days - 1 min accepted, + 1 min refused naming scheduledAt; dueAt of PATCH null, PATCH past and create past lies between two `UTC_TIMESTAMP(3)` reads taken around the request |

Assumptions test-writer made about the harness: a fakeprovider Fault without Count or Ratio applies to every matching request; the worker honours Retry-After 30 s; the MySQL session zone is UTC; every 409 message contains the API status word.

```go
//go:build integration

package acceptance

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// Helpers of the reschedule tests (B2.1, B2.2). Every name here starts with
// "rs" so it cannot collide with the helpers of other files in this package.

// rsPatch sends PATCH /v1/notifications/{id} with scheduledAt (nil sends
// null); the caller checks the answer.
func (s *stack) rsPatch(t *testing.T, id uuid.UUID, at *time.Time) *apiclient.RescheduleNotificationResponse {
	t.Helper()
	r, err := s.API.RescheduleNotificationWithResponse(t.Context(), id, apiclient.NotificationReschedule{ScheduledAt: at})
	if err != nil {
		t.Fatalf("PATCH /v1/notifications/%s: %v", id, err)
	}
	return r
}

// rsMoved reschedules, requires 200 with a body and returns it.
func (s *stack) rsMoved(t *testing.T, id uuid.UUID, at *time.Time) apiclient.Notification {
	t.Helper()
	r := s.rsPatch(t, id, at)
	if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
		t.Fatalf("PATCH /v1/notifications/%s scheduledAt=%v: want 200, got %s", id, at, show(r))
	}
	return *r.JSON200
}

// rsRefused requires 409 not_reschedulable whose message names status.
func rsRefused(t *testing.T, r typedAnswer, status string) {
	t.Helper()
	e := wantError(t, r, http.StatusConflict, "not_reschedulable")
	if status != "" && !strings.Contains(e.Message, status) {
		t.Errorf("409 message does not name the current status %q: %q", status, e.Message)
	}
}

// rsDBNow reads the database clock (UTC, millisecond precision, as due_at is stored).
func (s *stack) rsDBNow(t *testing.T) time.Time {
	t.Helper()
	var now time.Time
	if err := s.DB.QueryRowContext(t.Context(), "SELECT UTC_TIMESTAMP(3)").Scan(&now); err != nil {
		t.Fatalf("database clock: %v", err)
	}
	return now.UTC()
}

// rsTimeEq reports whether p is set and equal to want.
func rsTimeEq(p *time.Time, want time.Time) bool { return p != nil && p.Equal(want) }

// B2.1: Reschedule: PATCH a notification no worker has picked up to a new
// time or to now; refused once a worker has picked it up.
// (Moving a scheduled notification: it is sent after the new time, not at the
// old one; null or a past time makes it due now.) The claim uses the database
// clock, so "not before the new time" stops 200 ms short of it to allow for
// skew between the host and the MySQL container (as TestDeliveryB2ScheduledDelivery).
func TestB21RescheduleMovesAScheduledNotification(t *testing.T) {
	const skew = 200 * time.Millisecond
	s := startStack(t, stackOptions{Workers: 1, Env: dlFastEnv(nil)})
	reached := func(id uuid.UUID) int { return len(s.Provider.Requests(fakeprovider.Filter{Key: id.String()})) }

	old := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	scheduled := func(content string) apiclient.Notification {
		b := msg("push", content)
		b.ScheduledAt = &old
		n := s.create(t, b)
		if n.Status != "scheduled" {
			t.Fatalf("created for +1h: want status scheduled, got %q", n.Status)
		}
		return n
	}
	moved := scheduled("B2.1 moved to a near time")
	toNull := scheduled("B2.1 moved to now with null")
	toPast := scheduled("B2.1 moved to now with a past time")

	// Moved from +1h to +4s: the answer is the notification as stored.
	newAt := time.Now().UTC().Add(4 * time.Second).Truncate(time.Millisecond)
	m := s.rsMoved(t, moved.Id, &newAt)
	for name, x := range map[string]apiclient.Notification{"PATCH answer": m, "GET after PATCH": s.get(t, moved.Id)} {
		if x.Status != "scheduled" || !rsTimeEq(x.ScheduledAt, newAt) || !x.DueAt.Equal(newAt) {
			t.Errorf("%s: want status scheduled, scheduledAt and dueAt %s; got status %q scheduledAt %v dueAt %s",
				name, newAt.Format(time.RFC3339Nano), x.Status, x.ScheduledAt, x.DueAt.Format(time.RFC3339Nano))
		}
	}

	// Moved to now, by null and by a past time: due at once, sent promptly.
	past := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	before := time.Now()
	n := s.rsMoved(t, toNull.Id, nil)
	p := s.rsMoved(t, toPast.Id, &past)
	after := time.Now()
	for name, x := range map[string]apiclient.Notification{"null": n, "past time": p} {
		if x.Status != "pending" {
			t.Errorf("moved to now (%s): want status pending, got %q", name, x.Status)
		}
		if x.DueAt.Before(before.Add(-skew)) || x.DueAt.After(after.Add(skew)) {
			t.Errorf("moved to now (%s): dueAt %s is not the time of the PATCH [%s, %s]", name,
				x.DueAt.Format(time.RFC3339Nano), before.Format(time.RFC3339Nano), after.Format(time.RFC3339Nano))
		}
	}
	if n.ScheduledAt != nil {
		t.Errorf("moved to now with null: want no scheduledAt, got %s", n.ScheduledAt.Format(time.RFC3339Nano))
	}
	if !rsTimeEq(p.ScheduledAt, past) {
		t.Errorf("moved to a past time: want scheduledAt %s (as given), got %v", past.Format(time.RFC3339Nano), p.ScheduledAt)
	}
	// Sent within 2 s of the PATCH (plus 300 ms for this poll and the round trip).
	dlEventually(t, time.Until(after.Add(2*time.Second+300*time.Millisecond)), "both notifications moved to now are sent within 2s", func() (bool, string) {
		a, b := s.storedStatus(t, toNull.Id), s.storedStatus(t, toPast.Id)
		return a == "sent" && b == "sent", "stored status null=" + a + " past=" + b
	})

	// The one moved to +4 s: not before the new time, sent shortly after it, once.
	consistently(t, time.Until(newAt.Add(-skew)), func() bool { return reached(moved.Id) == 0 },
		"the notification moved to a near time does not reach the provider before it")
	dlEventually(t, time.Until(newAt.Add(2*time.Second+300*time.Millisecond)), "the notification moved to a near time is sent within 2s after it", func() (bool, string) {
		st := s.storedStatus(t, moved.Id)
		return st == "sent", "stored status " + st
	})
	rs := s.dlRequests(fakeprovider.Filter{Key: moved.Id.String()})
	if len(rs) != 1 {
		t.Fatalf("moved notification: want exactly 1 provider request, got %d", len(rs))
	}
	if early := newAt.Sub(rs[0].ReceivedAt); early > skew {
		t.Errorf("reached the provider %s before the new time", early)
	}
	t.Logf("moved from %s to %s, reached the provider %s after the new time", old.Format(time.RFC3339), newAt.Format(time.RFC3339Nano), rs[0].ReceivedAt.Sub(newAt).Round(time.Millisecond))
}

// B2.1: Reschedule: PATCH a notification no worker has picked up to a new
// time or to now; refused once a worker has picked it up.
// (Refusals: unknown id 404; past the 30-day horizon 400 naming scheduledAt;
// claimed, already attempted, cancelled or sent 409 not_reschedulable naming
// the current status.) Default timings (provider timeout 5s) so a held
// provider call keeps its notification claimed for the whole check.
func TestB21RescheduleRefusals(t *testing.T) {
	s := startStack(t, stackOptions{Workers: 1})
	// email: every call answers 429 Retry-After 30 s, so an attempted email
	// waits for its retry. push: the first call is held 3 s, so a push stays
	// claimed. sms is untouched and delivers.
	s.dlFaults(t,
		fakeprovider.Fault{Channel: "email", Mode: fakeprovider.ModeStatus, Status: http.StatusTooManyRequests, RetryAfterSeconds: 30},
		fakeprovider.Fault{Channel: "push", Mode: fakeprovider.ModeDelay, DelayMS: 3000, Count: 1},
	)
	later := func(d time.Duration) *time.Time { return ptr(time.Now().UTC().Add(d).Truncate(time.Millisecond)) }
	scheduledFor := func(channel, content string, d time.Duration) apiclient.Notification {
		b := msg(channel, content)
		b.ScheduledAt = later(d)
		return s.create(t, b)
	}

	t.Run("unknown id: 404 not_found", func(t *testing.T) {
		wantError(t, s.rsPatch(t, uuid.New(), later(time.Hour)), http.StatusNotFound, "not_found")
	})

	t.Run("past the horizon: 400 naming scheduledAt, schedule unchanged", func(t *testing.T) {
		n := scheduledFor("sms", "B2.1 horizon", time.Hour)
		wantError(t, s.rsPatch(t, n.Id, later(notification.ScheduleHorizon+2*time.Hour)), http.StatusBadRequest, "invalid_request", "scheduledAt")
		if got := s.get(t, n.Id); !rsTimeEq(got.ScheduledAt, *n.ScheduledAt) || !got.DueAt.Equal(n.DueAt) {
			t.Errorf("after a refused PATCH: want scheduledAt and dueAt %s, got scheduledAt %v dueAt %s", n.ScheduledAt, got.ScheduledAt, got.DueAt)
		}
		// Baseline: the same notification can be moved within the horizon,
		// so the refusal above is about the time, not about PATCH.
		at := later(2 * time.Hour)
		if m := s.rsMoved(t, n.Id, at); !rsTimeEq(m.ScheduledAt, *at) {
			t.Errorf("move within the horizon: want scheduledAt %s, got %v", at, m.ScheduledAt)
		}
	})

	t.Run("cancelled: 409 naming cancelled", func(t *testing.T) {
		n := scheduledFor("sms", "B2.1 cancelled", time.Hour)
		s.cancel(t, n.Id)
		rsRefused(t, s.rsPatch(t, n.Id, later(2*time.Hour)), "cancelled")
		if st := s.storedStatus(t, n.Id); st != "cancelled" {
			t.Errorf("after a refused PATCH: stored status %q, want cancelled", st)
		}
	})

	t.Run("sent: 409 naming sent", func(t *testing.T) {
		n := s.create(t, msg("sms", "B2.1 already sent"))
		dlEventually(t, 30*time.Second, "the notification is sent", func() (bool, string) {
			st := s.storedStatus(t, n.Id)
			return st == "sent", "stored status " + st
		})
		rsRefused(t, s.rsPatch(t, n.Id, later(time.Hour)), "sent")
		if got := s.get(t, n.Id); got.Status != "sent" {
			t.Errorf("after a refused PATCH: status %q, want sent", got.Status)
		}
		if c := len(s.Provider.Requests(fakeprovider.Filter{Key: n.Id.String()})); c != 1 {
			t.Errorf("provider requests: want 1, got %d", c)
		}
	})

	t.Run("claimed by a worker: 409 naming processing", func(t *testing.T) {
		n := s.create(t, msg("push", "B2.1 claimed"))
		dlEventually(t, 10*time.Second, "a worker claims the notification", func() (bool, string) {
			st := s.storedStatus(t, n.Id)
			return st == "processing", "stored status " + st
		})
		rsRefused(t, s.rsPatch(t, n.Id, later(time.Hour)), "processing")
		dlEventually(t, 15*time.Second, "the claimed notification is still sent", func() (bool, string) {
			st := s.storedStatus(t, n.Id)
			return st == "sent", "stored status " + st
		})
	})

	t.Run("attempted and waiting for its retry: 409", func(t *testing.T) {
		n := s.create(t, msg("email", "B2.1 retrying"))
		dlEventually(t, 15*time.Second, "the first attempt fails and the notification waits for its retry", func() (bool, string) {
			g := s.get(t, n.Id)
			return g.Attempts >= 1 && g.Status == "pending", fmt.Sprintf("status %s attempts %d", g.Status, g.Attempts)
		})
		r := s.rsPatch(t, n.Id, later(time.Hour))
		rsRefused(t, r, s.get(t, n.Id).Status)
		if g := s.get(t, n.Id); g.ScheduledAt != nil {
			t.Errorf("after a refused PATCH: want no scheduledAt, got %s", g.ScheduledAt)
		}
	})
}

// B2.1: Reschedule: PATCH a notification no worker has picked up to a new
// time or to now; refused once a worker has picked it up.
// (A reschedule racing a worker has exactly one winner: 200 and it never
// reaches the provider, or 409 and the provider gets it exactly once.)
// 50 notifications on one channel are about half a second of the limiter's
// paced rate (95/s), so the worker's claims are spread over the window in
// which the PATCHes land; each PATCH follows its own create after 0 to 225 ms
// (beyond the 50 ms idle poll of a busy worker), so both outcomes occur.
func TestB21RescheduleRacingAWorkerHasOneWinner(t *testing.T) {
	const n = 50
	s := startStack(t, stackOptions{Workers: 1, Env: dlFastEnv(nil)})
	reached := func(id uuid.UUID) int { return len(s.Provider.Requests(fakeprovider.Filter{Key: id.String()})) }

	// Baseline: the worker delivers; without it an all-200 run proves nothing.
	base := s.create(t, msg("push", "B2.1 race baseline"))
	dlEventually(t, 15*time.Second, "the baseline notification is sent", func() (bool, string) {
		st := s.storedStatus(t, base.Id)
		return st == "sent", "stored status " + st
	})

	later := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	type result struct {
		id     uuid.UUID
		status int
		body   []byte
		moved  *apiclient.Notification
		err    error
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			r, err := s.API.CreateNotificationWithResponse(t.Context(), nil, msg("push", fmt.Sprintf("B2.1 race %d", i)))
			if err != nil {
				results[i].err = fmt.Errorf("create: %w", err)
				return
			}
			if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
				results[i].err = fmt.Errorf("create: %s", show(r))
				return
			}
			results[i].id = r.JSON201.Id
			if d := time.Duration(i%10) * 25 * time.Millisecond; d > 0 {
				timer := time.NewTimer(d)
				<-timer.C
			}
			p, err := s.API.RescheduleNotificationWithResponse(t.Context(), results[i].id, apiclient.NotificationReschedule{ScheduledAt: &later})
			if err != nil {
				results[i].err = fmt.Errorf("PATCH: %w", err)
				return
			}
			results[i].status, results[i].body, results[i].moved = p.StatusCode(), p.Body, p.JSON200
		})
	}
	wg.Wait()

	var won, lost []uuid.UUID
	for _, r := range results {
		if r.err != nil {
			t.Fatalf("race setup: %v", r.err)
		}
		switch r.status {
		case http.StatusOK:
			won = append(won, r.id)
			if r.moved == nil || r.moved.Status != "scheduled" || !rsTimeEq(r.moved.ScheduledAt, later) {
				t.Errorf("%s: 200 but the answer is not scheduled for %s: %s", r.id, later.Format(time.RFC3339Nano), truncate(r.body))
			}
		case http.StatusConflict:
			lost = append(lost, r.id)
			var e apiclient.Error
			if err := json.Unmarshal(r.body, &e); err != nil || e.Code != "not_reschedulable" {
				t.Errorf("%s: 409 without code not_reschedulable: %s", r.id, truncate(r.body))
			}
		default:
			t.Fatalf("%s: PATCH want 200 or 409, got HTTP %d %s", r.id, r.status, truncate(r.body))
		}
	}
	t.Logf("race outcome: %d rescheduled (200), %d refused (409) of %d", len(won), len(lost), n)

	// Every loser is sent; every winner stays pending and away from the provider.
	dlEventually(t, 20*time.Second, "every notification whose PATCH was refused is sent", func() (bool, string) {
		c := map[string]int{}
		for _, id := range lost {
			c[s.storedStatus(t, id)]++
		}
		return c["sent"] == len(lost), fmt.Sprintf("%v", c)
	})
	consistently(t, 3*time.Second, func() bool {
		for _, id := range won {
			if reached(id) != 0 {
				return false
			}
		}
		return true
	}, "no rescheduled notification reaches the provider")

	for _, id := range won {
		if g := s.get(t, id); g.Status != "scheduled" || reached(id) != 0 {
			t.Errorf("%s: rescheduled (200), but status %q and %d provider requests (want scheduled, 0)", id, g.Status, reached(id))
		}
	}
	for _, id := range lost {
		if c := reached(id); c != 1 {
			t.Errorf("%s: refused (409), but %d provider requests (want exactly 1)", id, c)
		}
	}
	if len(won) == 0 || len(lost) == 0 {
		t.Errorf("the race was not exercised: %d rescheduled, %d refused (want both > 0)", len(won), len(lost))
	}
}

// B2.2: One clock: the 30-day horizon and due_at both use the database clock.
// (A create and a PATCH one minute inside the horizon are accepted, one
// minute past it refused; a schedule that means "now" gets the database's
// now as dueAt.) The test's times come from the host clock; the host and the
// MySQL container agree to well under a minute (checked first), so the
// one-minute margins decide on the server's horizon, not on skew.
func TestB22HorizonUsesTheDatabaseClock(t *testing.T) {
	s := startStack(t, stackOptions{})
	if d := time.Since(s.rsDBNow(t)).Abs(); d > 10*time.Second {
		t.Fatalf("precondition: host and database clocks differ by %s; the one-minute margins below need them within seconds", d)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	inside := now.Add(notification.ScheduleHorizon - time.Minute)
	outside := now.Add(notification.ScheduleHorizon + time.Minute)
	scheduled := func(at time.Time) apiclient.NotificationCreate {
		b := msg("sms", "B2.2 horizon")
		b.ScheduledAt = &at
		return b
	}

	t.Run("create one minute inside the horizon: accepted", func(t *testing.T) {
		n := s.create(t, scheduled(inside))
		if n.Status != "scheduled" || !rsTimeEq(n.ScheduledAt, inside) {
			t.Errorf("want status scheduled, scheduledAt %s; got %q %v", inside.Format(time.RFC3339Nano), n.Status, n.ScheduledAt)
		}
	})

	t.Run("create one minute past the horizon: 400 naming scheduledAt", func(t *testing.T) {
		wantError(t, s.postCreate(t, scheduled(outside)), http.StatusBadRequest, "invalid_request", "scheduledAt")
	})

	t.Run("PATCH one minute inside the horizon: accepted", func(t *testing.T) {
		n := s.create(t, scheduled(now.Add(time.Hour)))
		m := s.rsMoved(t, n.Id, &inside)
		if m.Status != "scheduled" || !rsTimeEq(m.ScheduledAt, inside) || !m.DueAt.Equal(inside) {
			t.Errorf("want status scheduled, scheduledAt and dueAt %s; got %q %v %s", inside.Format(time.RFC3339Nano), m.Status, m.ScheduledAt, m.DueAt)
		}
	})

	t.Run("PATCH one minute past the horizon: 400 naming scheduledAt", func(t *testing.T) {
		n := s.create(t, scheduled(now.Add(time.Hour)))
		wantError(t, s.rsPatch(t, n.Id, &outside), http.StatusBadRequest, "invalid_request", "scheduledAt")
		if g := s.get(t, n.Id); !rsTimeEq(g.ScheduledAt, *n.ScheduledAt) {
			t.Errorf("after a refused PATCH: want scheduledAt %s, got %v", n.ScheduledAt, g.ScheduledAt)
		}
	})

	// dueAt of "now" lies between two readings of the database clock taken
	// around the request (both at millisecond precision, as due_at is stored).
	bracket := func(t *testing.T, what string, call func() time.Time) {
		t.Helper()
		lo := s.rsDBNow(t)
		due := call()
		hi := s.rsDBNow(t)
		if due.Before(lo) || due.After(hi) {
			t.Errorf("%s: dueAt %s is not the database's now [%s, %s]", what,
				due.Format(time.RFC3339Nano), lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano))
		}
	}

	t.Run("PATCH to null: dueAt is the database's now", func(t *testing.T) {
		n := s.create(t, scheduled(now.Add(time.Hour)))
		bracket(t, "PATCH null", func() time.Time { return s.rsMoved(t, n.Id, nil).DueAt })
	})

	t.Run("PATCH to a past time: dueAt is the database's now", func(t *testing.T) {
		n := s.create(t, scheduled(now.Add(time.Hour)))
		past := now.Add(-time.Hour)
		bracket(t, "PATCH past", func() time.Time { return s.rsMoved(t, n.Id, &past).DueAt })
	})

	t.Run("create with a past time: dueAt is the database's now", func(t *testing.T) {
		bracket(t, "create past", func() time.Time { return s.create(t, scheduled(now.Add(-time.Hour))).DueAt })
	})
}
```
