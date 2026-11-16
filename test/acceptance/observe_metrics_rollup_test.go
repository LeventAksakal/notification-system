//go:build integration

package acceptance

// R13 metrics read from per-minute rollups (decision 0014, "As built"): whole
// minutes come from the rollups and the partial first minute from raw rows,
// so the numbers must be exact whatever second of the minute they are read at.

import (
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// R13: Real-time metrics endpoint (queue depth, success/failure rates,
// latency) - "The endpoint's numbers match what a test just produced", at
// every window length and on both sides of a minute boundary.
func TestR13MetricsCountsWhatAClientJustDid(t *testing.T) {
	const n = 20
	const ch = "push"
	// covered windows must start this far before the first send: the window
	// is cut on the database clock, generatedAt is the API's.
	const margin = time.Second
	windows := []int{1, 29, 30, 31, 59, 60, 61, 3600}
	s := startStack(t, stackOptions{Workers: 1, Env: dlFastEnv(nil)})

	base := map[int]apiclient.DeliveryStats{}
	for _, w := range windows {
		base[w] = deliveryByChannel(t, s.metrics(t, w))[ch]
	}
	ids := s.dlCreateMany(t, n, func(i int) apiclient.NotificationCreate { return msg(ch, fmt.Sprintf("R13 rollup %d", i)) })
	s.dlAllSent(t, 30*time.Second, n, "channel = ?", ch)
	var firstFinish time.Time
	for _, id := range ids {
		g := s.get(t, id)
		if g.FinishedAt == nil {
			t.Fatalf("%s is sent but has no finishedAt", id)
		}
		if firstFinish.IsZero() || g.FinishedAt.Before(firstFinish) {
			firstFinish = *g.FinishedAt
		}
	}

	read := func(t *testing.T) time.Time {
		t.Helper()
		var at time.Time
		for _, w := range windows {
			m := s.metrics(t, w)
			at = m.GeneratedAt
			d := deliveryByChannel(t, m)[ch]
			for name, v := range map[string]int{
				"sent": d.Sent, "failed": d.Failed, "attempts": d.Attempts, "failedAttempts": d.FailedAttempts,
				"delivery.sent": m.Delivery.Sent, "delivery.attempts": m.Delivery.Attempts,
			} {
				if v < 0 {
					t.Errorf("windowSeconds=%d: %s is negative: %d", w, name, v)
				}
			}
			sent, attempts := d.Sent-base[w].Sent, d.Attempts-base[w].Attempts
			covers := m.GeneratedAt.Add(-time.Duration(w) * time.Second).Before(firstFinish.Add(-margin))
			switch {
			case covers && (sent != n || attempts != n):
				t.Errorf("windowSeconds=%d (covers every send; generatedAt %s, first send %s): want %d more sent and %d more attempts, got %d and %d",
					w, m.GeneratedAt.Format(time.RFC3339Nano), firstFinish.Format(time.RFC3339Nano), n, n, sent, attempts)
			case !covers && (sent > n || attempts > n):
				t.Errorf("windowSeconds=%d (shorter than the time since the first send): want at most %d more sent and attempts, got %d and %d", w, n, sent, attempts)
			}
			t.Logf("windowSeconds=%d covers=%v: +%d sent, +%d attempts", w, covers, sent, attempts)
		}
		return at
	}
	var at time.Time
	t.Run("right after the sends", func(t *testing.T) { at = read(t) })

	// Cross the next minute boundary on the server's clock.
	next := at.Truncate(time.Minute).Add(time.Minute)
	dlWaitUntil(time.Now().Add(next.Sub(at) + 300*time.Millisecond))
	if !poll(10*time.Second, func() bool { return !s.metrics(t, 3600).GeneratedAt.Before(next) }) {
		t.Fatalf("the server's clock did not pass %s", next.Format(time.RFC3339))
	}
	t.Run("after the next minute boundary", func(t *testing.T) { read(t) })
}

// R13: Real-time metrics endpoint - latency. The endpoint's percentiles are
// estimates from buckets at most 12.5% wide (decision 0014 "As built"):
// the estimate lies in the same bucket as the exact nearest-rank value.
// 10 notifications scheduled 2 s ahead on a channel no other send uses.
func TestR13MetricsLatencyWithinBucketError(t *testing.T) {
	const n = 10
	const ch = "push"
	s := startStack(t, stackOptions{Workers: 1, Env: dlFastEnv(nil)})
	at := time.Now().UTC().Add(2 * time.Second).Truncate(time.Millisecond)
	ids := s.dlCreateMany(t, n, func(i int) apiclient.NotificationCreate {
		b := msg(ch, fmt.Sprintf("R13 latency %d", i))
		b.ScheduledAt = &at
		return b
	})
	s.dlAllSent(t, 30*time.Second, n, "channel = ?", ch)

	lat := make([]int, 0, n)
	for _, id := range ids {
		g := s.get(t, id)
		due := g.CreatedAt
		if g.ScheduledAt != nil && g.ScheduledAt.After(due) {
			due = *g.ScheduledAt
		}
		if g.FinishedAt == nil {
			t.Fatalf("%s is sent but has no finishedAt", id)
		}
		lat = append(lat, int(math.Ceil(float64(g.FinishedAt.Sub(due).Microseconds())/1000)))
	}
	slices.Sort(lat)
	exact := lat[int(math.Ceil(0.5*n))-1] // nearest rank
	got := deliveryByChannel(t, s.metrics(t, 60))[ch].LatencyMs.P50
	// One bucket is at most 1/8 of its lower bound wide (exact up to 16 ms);
	// both values lie in it. 1 ms for rounding.
	tol := max(exact/8, 0) + 1
	t.Logf("latencies %v ms: exact p50 %d ms, endpoint p50 %d ms (tolerance %d ms)", lat, exact, got, tol)
	if got < exact-tol || got > exact+tol {
		t.Errorf("latency p50: endpoint %d ms, exact %d ms; want within one bucket (%d ms)", got, exact, tol)
	}
}

// R13: Real-time metrics endpoint ("success/failure rates"): a call whose
// result never came (its worker died; owner item 0015) is not counted as an
// attempt nor as a failed attempt. The contract has no unknownAttempts field
// yet (owner item 0015, open), so only this half is observable.
// Dials: APP_PROVIDER_TIMEOUT=5s so the 3 s hold keeps the call open while the
// worker is killed; APP_LEASE_DURATION=8s (config.Load's floor with a 5 s
// timeout is 7.35 s, decision 0018); APP_REAP_INTERVAL=500ms.
func TestR13MetricsUnknownAttemptsCountedApart(t *testing.T) {
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(map[string]string{
		"APP_PROVIDER_TIMEOUT": "5s",
		"APP_LEASE_DURATION":   "8s",
	})})
	n := s.create(t, msg("sms", "R13 unknown attempt"))
	s.dlFaults(t, fakeprovider.Fault{Mode: fakeprovider.ModeDelay, DelayMS: 3000})
	w := s.StartWorker()
	dlEventually(t, 15*time.Second, "the call is in flight at the provider", func() (bool, string) {
		r := s.Provider.Stats().Channels["sms"].Received
		return r == 1, fmt.Sprintf("%d received", r)
	})
	s.KillWorker(w)
	s.dlFaults(t)
	s.StartWorker()
	s.waitStatus(t, n.Id, "sent", 30*time.Second)
	if h := s.attemptHistory(t, n.Id); h != "1 unknown, 2 accepted" {
		t.Fatalf("precondition: attempt history %q, want %q (the killed call reaped as unknown)", h, "1 unknown, 2 accepted")
	}
	d := deliveryByChannel(t, s.metrics(t, 3600))["sms"]
	if d.Sent != 1 || d.Attempts != 1 || d.FailedAttempts != 0 {
		t.Errorf("sms delivery: want sent 1, attempts 1 (the accepted call only), failedAttempts 0; got %+v", d)
	}
}
