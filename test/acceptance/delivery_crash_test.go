//go:build integration

package acceptance

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// R11 (delivery half): Idempotency support to prevent duplicate sends
// (one notification reaches the provider once under worker crashes and
// unknown outcomes; decisions 0006, 0018).
func TestDeliveryR11OnceUnderTimeoutsAndCrashes(t *testing.T) {
	const maxAttempts = 3

	// Dials: APP_PROVIDER_TIMEOUT=500ms (the hang is 1500 ms, so every hung
	// call times out), APP_MAX_ATTEMPTS=3, APP_RETRY_BASE=100ms,
	// APP_BREAKER_FAILURES=1000 (the timeouts must not declare a provider
	// outage: with no outage every failure counts against the message, so
	// calls made per key stay bounded by APP_MAX_ATTEMPTS).
	timeoutEnv := dlFastEnv(map[string]string{
		"APP_MAX_ATTEMPTS":     fmt.Sprint(maxAttempts),
		"APP_BREAKER_FAILURES": "1000",
	})
	hangThenSend := func(t *testing.T, s *stack, channel string, n int) []uuid.UUID {
		t.Helper()
		// The first n requests on the channel are accepted (counted as delivered)
		// and then held past the sender's timeout: an unknown outcome.
		s.dlFaults(t, fakeprovider.Fault{Channel: channel, Mode: fakeprovider.ModeAcceptThenHang, DelayMS: 1500, Count: n})
		ids := s.dlCreateMany(t, n, func(i int) apiclient.NotificationCreate {
			return msg(channel, fmt.Sprintf("R11 hang %d", i))
		})
		s.dlAllSent(t, 30*time.Second, n, "channel = ?", channel)
		return ids
	}

	t.Run("accepted then hung past the timeout, provider dedupes: delivered once", func(t *testing.T) {
		s := startStack(t, stackOptions{Workers: 2, Env: timeoutEnv})
		hangThenSend(t, s, "sms", 10)
		st := s.Provider.Stats()
		t.Logf("dedupe on: maxRequestsPerKey %d, maxDeliveriesPerKey %d", st.MaxRequestsPerKey, st.MaxDeliveriesPerKey)
		if st.MaxDeliveriesPerKey != 1 {
			t.Errorf("maxDeliveriesPerKey: want 1, got %d", st.MaxDeliveriesPerKey)
		}
		if st.MaxRequestsPerKey > maxAttempts {
			t.Errorf("maxRequestsPerKey: %d, want at most APP_MAX_ATTEMPTS=%d", st.MaxRequestsPerKey, maxAttempts)
		}
		if st.MaxRequestsPerKey < 2 {
			t.Errorf("maxRequestsPerKey: %d, want at least 2 (a timed-out call must be retried)", st.MaxRequestsPerKey)
		}
	})

	t.Run("accepted then hung past the timeout, provider without dedupe: duplicates bounded by attempts", func(t *testing.T) {
		s := startStack(t, stackOptions{Workers: 2, Env: timeoutEnv, Provider: []fakeprovider.Option{fakeprovider.WithDedupe(false)}})
		hangThenSend(t, s, "email", 10)
		st := s.Provider.Stats()
		t.Logf("dedupe off: %d keys delivered more than once; maxDeliveriesPerKey %d, maxRequestsPerKey %d",
			st.KeysDeliveredMoreThanOnce, st.MaxDeliveriesPerKey, st.MaxRequestsPerKey)
		if st.MaxDeliveriesPerKey > maxAttempts {
			t.Errorf("maxDeliveriesPerKey: %d, want at most APP_MAX_ATTEMPTS=%d", st.MaxDeliveriesPerKey, maxAttempts)
		}
		if st.MaxRequestsPerKey > maxAttempts {
			t.Errorf("maxRequestsPerKey: %d, want at most APP_MAX_ATTEMPTS=%d", st.MaxRequestsPerKey, maxAttempts)
		}
	})

	// Dials: APP_PROVIDER_TIMEOUT=5s so the 3 s delay holds the sends instead
	// of timing them out; APP_LEASE_DURATION=8s, the smallest whole second
	// config.Load accepts with that timeout (decision 0018: floor 7.35 s; the
	// 2 s lease used before is refused); APP_REAP_INTERVAL=500ms, so the
	// crashed worker's claims return after ~8.5 s.
	// It also carries the R11 history assertion of decision 0005 ("attempts
	// written before the call"): the call whose worker died stays in the
	// history as unknown, followed by the accepted retry.
	t.Run("worker killed while its sends are held: another worker finishes them, delivered once", func(t *testing.T) {
		const n = 20
		s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(map[string]string{
			"APP_PROVIDER_TIMEOUT": "5s",
			"APP_LEASE_DURATION":   "8s",
			"APP_MAX_ATTEMPTS":     fmt.Sprint(maxAttempts),
		})})
		ids := s.dlCreateMany(t, n, func(i int) apiclient.NotificationCreate {
			return msg("sms", fmt.Sprintf("R11 crash %d", i))
		})
		s.dlFaults(t, fakeprovider.Fault{Mode: fakeprovider.ModeDelay, DelayMS: 3000})
		w := s.StartWorker()
		dlEventually(t, 15*time.Second, "worker-1 has all its sends in flight", func() (bool, string) {
			r := s.Provider.Stats().Channels["sms"].Received
			return r == n, fmt.Sprintf("%d of %d received", r, n)
		})
		s.KillWorker(w)
		s.dlFaults(t)
		s.StartWorker()

		s.dlAllSent(t, 30*time.Second, n, "1=1")
		if p := s.count(t, "SELECT COUNT(*) FROM notifications WHERE status = 'processing'"); p != 0 {
			t.Errorf("%d notifications still processing", p)
		}
		delivered := s.dlDeliveredKeys()
		for _, id := range ids {
			if !delivered[id.String()] {
				t.Errorf("%s never delivered at the provider", id)
			}
		}
		st := s.Provider.Stats()
		t.Logf("after the crash: maxRequestsPerKey %d, maxDeliveriesPerKey %d", st.MaxRequestsPerKey, st.MaxDeliveriesPerKey)
		if st.MaxDeliveriesPerKey != 1 {
			t.Errorf("maxDeliveriesPerKey: want 1, got %d", st.MaxDeliveriesPerKey)
		}
		if st.MaxRequestsPerKey > maxAttempts {
			t.Errorf("maxRequestsPerKey: %d, want at most APP_MAX_ATTEMPTS=%d", st.MaxRequestsPerKey, maxAttempts)
		}
		// R11, decision 0005 ("attempts written before the call"): the crashed
		// call stays in the history as unknown, read through the API as an
		// operator would: GET /v1/notifications/{id} lists `unknown` attempts
		// (owner item 0015).
		const wantHistory = "1 unknown, 2 accepted"
		for _, id := range ids {
			as := history(s.get(t, id))
			got := make([]string, len(as))
			for i, a := range as {
				got[i] = fmt.Sprintf("%d %s", a.AttemptNo, a.Outcome)
			}
			if h := strings.Join(got, ", "); h != wantHistory {
				t.Errorf("%s: GET attempt history %q, want %q; full history %s", id, h, wantHistory, attemptsString(as))
			}
		}
	})
}

// attemptHistory reads a notification's attempt rows with SQL as
// "1 unknown, 2 accepted" (kept for TestR13MetricsUnknownAttemptsCountedApart).
func (s *stack) attemptHistory(t *testing.T, id uuid.UUID) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := s.DB.QueryContext(ctx, "SELECT attempt_no, outcome FROM delivery_attempts WHERE notification_id = ? ORDER BY attempt_no", id.String())
	if err != nil {
		t.Fatalf("attempt history of %s: %v", id, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var no int
		var outcome string
		if err := rows.Scan(&no, &outcome); err != nil {
			t.Fatalf("attempt history of %s: %v", id, err)
		}
		out = append(out, fmt.Sprintf("%d %s", no, outcome))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("attempt history of %s: %v", id, err)
	}
	return strings.Join(out, ", ")
}

// R5 (delivery half): Cancel pending notifications
// (cancel before sending: never reaches the provider; cancel after sending:
// refused; decision 0005: a cancel racing a claim has exactly one winner).
func TestDeliveryR5CancelRacesClaim(t *testing.T) {
	const n = 90
	s := startStack(t, stackOptions{Workers: 3, Env: dlFastEnv(nil)})

	// Baseline: the workers deliver; without it an all-cancelled run proves nothing.
	base := s.create(t, msg("sms", "R5 baseline"))
	dlEventually(t, 15*time.Second, "the baseline notification is sent", func() (bool, string) {
		st := s.storedStatus(t, base.Id)
		return st == "sent", "stored status " + st
	})

	type result struct {
		id     uuid.UUID
		cancel int
		err    error
	}
	results := make([]result, n)
	chans := []string{"sms", "email", "push"}
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			r, err := s.API.CreateNotificationWithResponse(t.Context(), nil, msg(chans[i%3], fmt.Sprintf("R5 race %d", i)))
			if err != nil {
				results[i].err = fmt.Errorf("create: %w", err)
				return
			}
			if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
				results[i].err = fmt.Errorf("create: %s", show(r))
				return
			}
			results[i].id = r.JSON201.Id
			// Spread the cancels from immediately to ~0.9 s after create so
			// both outcomes of the race occur.
			if d := time.Duration(i%10) * 100 * time.Millisecond; d > 0 {
				timer := time.NewTimer(d)
				<-timer.C
			}
			c, err := s.API.CancelNotificationWithResponse(t.Context(), results[i].id)
			if err != nil {
				results[i].err = err
				return
			}
			results[i].cancel = c.StatusCode()
		})
	}
	wg.Wait()
	for _, r := range results {
		if r.err != nil {
			t.Fatalf("race setup: %v", r.err)
		}
		if r.cancel != http.StatusOK && r.cancel != http.StatusConflict {
			t.Fatalf("cancel of %s: want 200 or 409, got %d", r.id, r.cancel)
		}
	}

	dlEventually(t, 20*time.Second, "every raced notification is sent or cancelled", func() (bool, string) {
		c := s.dlCounts(t, "1=1")
		return c["sent"]+c["cancelled"] == n+1, fmt.Sprintf("%v", c)
	})
	sent, cancelled := 0, 0
	for _, r := range results {
		st := s.storedStatus(t, r.id)
		reached := len(s.Provider.Requests(fakeprovider.Filter{Key: r.id.String()})) > 0
		switch st {
		case "sent":
			sent++
			if r.cancel != http.StatusConflict {
				t.Errorf("%s: sent, but its cancel answered %d", r.id, r.cancel)
			}
		case "cancelled":
			cancelled++
			if r.cancel != http.StatusOK {
				t.Errorf("%s: cancelled, but its cancel answered %d", r.id, r.cancel)
			}
		default:
			t.Errorf("%s: final status %q, want sent or cancelled", r.id, st)
		}
		if reached != (st == "sent") {
			t.Errorf("%s: status %s but reached the provider = %v", r.id, st, reached)
		}
	}
	t.Logf("race outcome: %d sent, %d cancelled of %d", sent, cancelled, n)
	if sent == 0 || cancelled == 0 {
		t.Errorf("the race was not exercised: %d sent, %d cancelled (want both > 0)", sent, cancelled)
	}
}
