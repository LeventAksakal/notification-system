package delivery

import (
	"net"
	"slices"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

// refusedProvider is a real provider client pointed at a port nobody listens
// on: every call fails its dial with "connection refused".
func refusedProvider(t *testing.T) *provider.Client {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	// 1 s: far above a local refusal (well under a millisecond), so no call
	// ends in the timeout instead.
	return provider.New(map[notification.Channel]string{notification.ChannelSMS: "http://" + addr + "/send"}, time.Second, 4)
}

// A provider that refuses connections is a provider fault, not a call that
// never left: each refusal is a recorded attempt, so budgets run out and the
// outage opens, and no job is given back to be claimed again at once
// (verifier finding 1 on 25176b4, 16 Nov 2026: refusals were released and the
// worker re-claimed them in a loop that never tripped the breaker).
func TestARefusingProviderExhaustsBudgetsAndOpensTheOutage(t *testing.T) {
	t.Run("a job's attempts are recorded until its budget is exhausted", func(t *testing.T) {
		q := newMemQueue()
		q.add(1, notification.ChannelSMS, notification.PriorityNormal)
		id := q.order[0]
		// Failures far above MaxAttempts: the breaker stays closed, so the
		// budget alone decides.
		st := health.NewMemory(health.Config{Failures: 1000, Cooldown: time.Minute}, time.Now)
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: refusedProvider(t), Health: st}))
		waitFor(t, "the job dead-lettered", func() bool { return q.row(id).status == notification.StatusFailed })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if k := q.row(id).kind; k != notification.FailureExhausted {
			t.Errorf("failure kind %q, want %q", k, notification.FailureExhausted)
		}
		q.mu.Lock()
		results, released, claims := q.results[id], q.released, q.claims[notification.PriorityNormal]
		q.mu.Unlock()
		want := slices.Repeat([]notification.AttemptOutcome{notification.OutcomeTransient}, testConfig().MaxAttempts)
		if !slices.Equal(results, want) {
			t.Errorf("recorded attempts %v, want %v", results, want)
		}
		if released != 0 || claims != testConfig().MaxAttempts {
			t.Errorf("%d releases, %d claims; want 0 releases and one claim per attempt (%d)", released, claims, testConfig().MaxAttempts)
		}
	})

	t.Run("the outage opens and the worker does not spin on the backlog", func(t *testing.T) {
		const failures, rows = 3, 30
		const cooldown = 50 * time.Millisecond
		q := newMemQueue()
		q.add(rows, notification.ChannelSMS, notification.PriorityNormal)
		st := &openCounter{State: health.NewMemory(health.Config{Failures: failures, Cooldown: cooldown}, time.Now)}
		send := &countingSender{fn: refusedProvider(t).Send}
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: send, Health: st}))
		outage := 20 * cooldown // 1 s, as in TestAnOutageSpendsNoBudgetBeyondItsDetection
		// A bounded measurement, not a wait for a state: the sleep is the
		// window the calls are counted over, and the bounds are upper bounds
		// that scale with it (one probe per cool-down). A loaded machine makes
		// fewer calls in the window, not more, and three refused dials, which
		// open the outage, take milliseconds. A fake clock would have to drive
		// the limiter's grid, the idle timers and the health cool-down at
		// once; the worker reads real time in all three.
		time.Sleep(outage)
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if st.opened.Load() == 0 {
			t.Error("the outage never opened")
		}
		q.mu.Lock()
		released, claims := q.released, q.claims[notification.PriorityNormal]
		q.mu.Unlock()
		// Some releases are expected: jobs claimed when the outage closed the
		// gate, and jobs in hand at shutdown, go back without a call. A
		// release loop shows in the claims, bounded below.
		t.Logf("%d calls, %d claims, %d releases in %s", send.calls.Load(), claims, released, outage)
		// The same bound as TestAnOutageSpendsNoBudgetBeyondItsDetection:
		// detection, the grant in flight when it was declared (5 slots), one
		// probe per cool-down and one at the edge. A release loop makes
		// thousands in a second.
		limit := failures + 5 + int(outage/cooldown) + 1
		if calls := int(send.calls.Load()); calls > limit {
			t.Errorf("%d provider calls in %s, want at most %d (about one probe per cool-down)", calls, outage, limit)
		}
		// Each row is claimed once before the outage is declared, then once
		// per call at most; the release loop of 25176b4 re-claimed thousands.
		if claims > rows+limit {
			t.Errorf("%d claims in %s, want at most %d", claims, outage, rows+limit)
		}
	})
}
