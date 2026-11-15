//go:build integration

package acceptance

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// dlRetrySlack is what a retry gap may exceed its backoff bound by: the
// worker's claim polling, the claim transaction and the limiter wait.
const dlRetrySlack = time.Second

// R12: Delivery & Retry Logic
// (transient provider failures are retried and end as sent; permanent ones fail
// after one attempt; exhausted ones are kept and can be replayed; decision 0007).
// Dials: APP_RETRY_BASE=100ms, APP_RETRY_CAP=1s, APP_MAX_ATTEMPTS=3,
// APP_PROVIDER_TIMEOUT=500ms; breaker defaults (10 calls before it counts), so
// the handful of failures here never opens it.
func TestDeliveryR12Retry(t *testing.T) {
	const base, maxAttempts = 100 * time.Millisecond, 3
	s := startStack(t, stackOptions{Workers: 1, Env: dlFastEnv(map[string]string{"APP_MAX_ATTEMPTS": "3"})})

	waitStatus := func(t *testing.T, id uuid.UUID, want string) apiclient.Notification {
		t.Helper()
		dlEventually(t, 10*time.Second, "notification "+id.String()+" stored as "+want, func() (bool, string) {
			st := s.storedStatus(t, id)
			return st == want, "stored status " + st
		})
		return s.get(t, id)
	}
	outcomes := func(n apiclient.Notification) []string {
		var out []string
		for _, a := range history(n) {
			st := 0
			if a.HttpStatus != nil {
				st = *a.HttpStatus
			}
			out = append(out, fmt.Sprintf("%s/%d", a.Outcome, st))
		}
		return out
	}

	t.Run("503 twice then accepted: sent after 3 attempts, gaps within the jitter bound", func(t *testing.T) {
		s.dlFaults(t, fakeprovider.Fault{Channel: "sms", Mode: fakeprovider.ModeStatus, Status: 503, Count: 2})
		t.Cleanup(func() { _ = s.Provider.SetFaults(nil) })
		n := s.create(t, msg("sms", "R12 transient then ok"))
		got := waitStatus(t, n.Id, "sent")
		if got.Attempts != 3 {
			t.Errorf("attempts: want 3, got %d", got.Attempts)
		}
		want := []string{"transient/503", "transient/503", "accepted/202"}
		if o := outcomes(got); !slices.Equal(o, want) {
			t.Errorf("deliveryAttempts: want %v, got %v", want, o)
		}
		rs := s.dlRequests(fakeprovider.Filter{Key: n.Id.String()})
		if len(rs) != 3 {
			t.Fatalf("requests at the double: want 3, got %d", len(rs))
		}
		// Full jitter: the wait after attempt k is random(0, min(cap, base*2^k)),
		// so gaps need not grow; each stays under its bound plus slack.
		for k := 1; k < len(rs); k++ {
			gap := rs[k].ReceivedAt.Sub(rs[k-1].ReceivedAt)
			bound := min(time.Second, base<<k)
			t.Logf("gap after attempt %d: %s (bound %s + slack %s)", k, gap.Round(time.Millisecond), bound, dlRetrySlack)
			if gap > bound+dlRetrySlack {
				t.Errorf("gap after attempt %d: %s, want at most %s", k, gap, bound+dlRetrySlack)
			}
		}
	})

	t.Run("400: failed as permanent after exactly one attempt", func(t *testing.T) {
		s.dlFaults(t, fakeprovider.Fault{Channel: "sms", Mode: fakeprovider.ModeStatus, Status: 400, Count: 1})
		t.Cleanup(func() { _ = s.Provider.SetFaults(nil) })
		n := s.create(t, msg("sms", "R12 permanent"))
		got := waitStatus(t, n.Id, "failed")
		if got.Status != "failed" || deref(got.FailureKind) != "permanent" {
			t.Errorf("want failed/permanent, got %s/%s", got.Status, deref(got.FailureKind))
		}
		if got.Attempts != 1 {
			t.Errorf("attempts: want 1, got %d", got.Attempts)
		}
		if o := outcomes(got); !slices.Equal(o, []string{"permanent/400"}) {
			t.Errorf("deliveryAttempts: want [permanent/400], got %v", o)
		}
		consistently(t, time.Second, func() bool {
			return len(s.Provider.Requests(fakeprovider.Filter{Key: n.Id.String()})) == 1
		}, "a permanent failure is never retried")
	})

	t.Run("always 503: exhausted after APP_MAX_ATTEMPTS, listed as failed, replayable", func(t *testing.T) {
		s.dlFaults(t, fakeprovider.Fault{Channel: "email", Mode: fakeprovider.ModeStatus, Status: 503})
		t.Cleanup(func() { _ = s.Provider.SetFaults(nil) })
		n := s.create(t, msg("email", "R12 exhausted"))
		got := waitStatus(t, n.Id, "failed")
		if deref(got.FailureKind) != "exhausted" {
			t.Errorf("failureKind: want exhausted, got %q", deref(got.FailureKind))
		}
		if got.Attempts != maxAttempts {
			t.Errorf("attempts: want %d, got %d", maxAttempts, got.Attempts)
		}
		if deref(got.LastError) == "" {
			t.Error("lastError is empty on an exhausted notification")
		}
		if o := outcomes(got); !slices.Equal(o, []string{"transient/503", "transient/503", "transient/503"}) {
			t.Errorf("deliveryAttempts: want 3 x transient/503, got %v", o)
		}
		if rs := s.Provider.Requests(fakeprovider.Filter{Key: n.Id.String()}); len(rs) != maxAttempts {
			t.Errorf("requests at the double: want %d, got %d", maxAttempts, len(rs))
		}
		failed := s.listAll(t, apiclient.ListNotificationsParams{Status: ptr(apiclient.Failed)})
		if !slices.Contains(ids(failed), n.Id) {
			t.Errorf("GET /v1/notifications?status=failed does not list %s", n.Id)
		}

		_ = s.Provider.SetFaults(nil)
		r, err := s.API.RetryNotificationWithResponse(t.Context(), n.Id)
		if err != nil {
			t.Fatalf("retry of a failed notification: %v", err)
		}
		if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
			t.Fatalf("retry of a failed notification: want 200, got %s", show(r))
		}
		replayed := waitStatus(t, n.Id, "sent")
		if deref(replayed.FailureKind) != "" {
			t.Errorf("failureKind after a successful replay: want none, got %q", deref(replayed.FailureKind))
		}
		o := outcomes(replayed)
		if len(o) != maxAttempts+1 || o[len(o)-1] != "accepted/202" {
			t.Errorf("deliveryAttempts after replay: want the 3 failures kept plus accepted/202, got %v", o)
		}
	})

	t.Run("retry of a notification that is not failed: 409 not_retryable", func(t *testing.T) {
		// email: no fault rule is left on it, so this one never consumes another subtest's fault.
		n := s.create(t, msg("email", "R12 not failed"))
		wantError(t, answered(t)(s.API.RetryNotificationWithResponse(t.Context(), n.Id)), http.StatusConflict, "not_retryable")
	})

	t.Run("429 with Retry-After: 2 waits pending with a future due time, then retries no earlier than 2 s", func(t *testing.T) {
		s.dlFaults(t, fakeprovider.Fault{Channel: "push", Mode: fakeprovider.ModeStatus, Status: 429, RetryAfterSeconds: 2, Count: 1})
		t.Cleanup(func() { _ = s.Provider.SetFaults(nil) })
		n := s.create(t, msg("push", "R12 retry after"))
		dlEventually(t, 10*time.Second, "the first request reaches the double", func() (bool, string) {
			return len(s.Provider.Requests(fakeprovider.Filter{Key: n.Id.String()})) > 0, "no request"
		})
		// While it waits to retry, no worker holds it: stored pending, due in the
		// future on the database clock.
		dlEventually(t, 1500*time.Millisecond, "stored as pending with a future due time and 1 attempt", func() (bool, string) {
			var st string
			var future bool
			var attempts int
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := s.DB.QueryRowContext(ctx, "SELECT status, due_at > NOW(3), attempts FROM notifications WHERE id = ?", n.Id.String()).Scan(&st, &future, &attempts)
			if err != nil {
				return false, err.Error()
			}
			return st == "pending" && future && attempts == 1, fmt.Sprintf("status %s, due in future %v, attempts %d", st, future, attempts)
		})
		if v := s.get(t, n.Id); v.Status != "pending" || !v.DueAt.After(v.UpdatedAt) {
			t.Errorf("GET while waiting to retry: want status pending with dueAt after updatedAt, got %s dueAt=%s updatedAt=%s", v.Status, v.DueAt, v.UpdatedAt)
		}
		waitStatus(t, n.Id, "sent")
		rs := s.dlRequests(fakeprovider.Filter{Key: n.Id.String()})
		if len(rs) != 2 {
			t.Fatalf("requests at the double: want 2, got %d", len(rs))
		}
		gap := rs[1].ReceivedAt.Sub(rs[0].ReceivedAt)
		t.Logf("Retry-After 2: retried %s after the first request", gap.Round(time.Millisecond))
		if gap < 2*time.Second {
			t.Errorf("retried %s after a 429 with Retry-After: 2, want at least 2s", gap)
		}
	})
}

// B1: Bonus: Failure Handling
// (provider outage: the channel pauses, no attempts are burned, sending
// resumes; decision 0008).
// Dials: APP_BREAKER_WINDOW=2s, APP_BREAKER_MIN_REQUESTS=5,
// APP_BREAKER_COOLDOWN=1s, APP_MAX_ATTEMPTS=3, APP_RETRY_BASE=100ms,
// APP_RETRY_CAP=1s; rate 100/s with 5 % headroom (dlPacedRate, 95/s).
func TestDeliveryB1BreakerPausesChannel(t *testing.T) {
	const perChannel = 300
	const outage = 6 * time.Second
	chans := []string{"sms", "email", "push"}
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(map[string]string{
		"APP_BREAKER_WINDOW":       "2s",
		"APP_BREAKER_MIN_REQUESTS": "5",
		"APP_BREAKER_COOLDOWN":     "1s",
		"APP_MAX_ATTEMPTS":         "3",
	})})
	s.dlCreateMany(t, perChannel*3, func(i int) apiclient.NotificationCreate {
		return msg(chans[i%3], fmt.Sprintf("B1 backlog %d", i))
	})
	s.dlFaults(t, fakeprovider.Fault{Channel: "sms", Mode: fakeprovider.ModeStatus, Status: 503})
	for range 3 {
		s.StartWorker()
	}

	var start time.Time
	dlEventually(t, 15*time.Second, "the first sms request reaches the double", func() (bool, string) {
		rs := s.dlRequests(fakeprovider.Filter{Channel: "sms"})
		if len(rs) == 0 {
			return false, "no sms request"
		}
		start = rs[0].ReceivedAt
		return true, ""
	})
	// The outage lasts until start+outage; through it no sms notification may fail.
	consistently(t, time.Until(start.Add(outage)), func() bool {
		return s.count(t, "SELECT COUNT(*) FROM notifications WHERE channel = 'sms' AND status = 'failed'") == 0
	}, "no sms notification is failed during the outage")
	dlWaitUntil(start.Add(outage))
	s.dlFaults(t)
	cleared := time.Now()

	during := dlReceivedIn(s.dlRequests(fakeprovider.Filter{Channel: "sms"}), start, cleared)
	limit := 0.25 * dlPacedRate * cleared.Sub(start).Seconds()
	t.Logf("sms requests during the %s outage: %d (limiter would allow ~%.0f; bound %.0f)", cleared.Sub(start).Round(time.Millisecond), during, dlPacedRate*cleared.Sub(start).Seconds(), limit)
	if float64(during) >= limit {
		t.Errorf("sms requests during the outage: %d, want fewer than %.0f (25%% of the limiter's rate): the channel did not pause", during, limit)
	}

	minFastRate := dlRateFloor(t, "B1 fast channels during the sms outage", dlPacedRate-7) // 88/s
	for _, ch := range []string{"email", "push"} {
		s.dlAllSent(t, 20*time.Second, perChannel, "channel = ?", ch)
		n, span, rate := dlRate(s.dlRequests(fakeprovider.Filter{Channel: ch}))
		t.Logf("%s during the sms outage: %d requests over %s = %.2f/s", ch, n, span.Round(time.Millisecond), rate)
		if rate < minFastRate {
			t.Errorf("%s drained at %.2f/s during the sms outage, want at least %.2f (other channels keep their rate)", ch, rate, minFastRate)
		}
	}
	s.dlAllSent(t, 30*time.Second, perChannel, "channel = 'sms'")
}
