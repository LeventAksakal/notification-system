//go:build integration

package delivery_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/delivery"
	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/delivery/limiter"
	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

// TestWorkersHoldTheRateAtTheProvider runs three workers in-process against
// real MySQL, Valkey and the provider double and checks the arrival shape the
// provider sees: at most the limit in any one-second window, and the limit used.
func TestWorkersHoldTheRateAtTheProvider(t *testing.T) {
	// limit and headroom are the deployed defaults: 100 per second, paced 5% under.
	const perChannel, workers, limit, headroom = 500, 3, 100, 5
	_, dsn := support.MySQL(t)
	vkAddr := support.Valkey(t)
	purl, prov := support.Provider(t)

	seed, err := store.Open(t.Context(), dsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seed.Close() }()
	to := map[notification.Channel]string{notification.ChannelSMS: "+905551234567", notification.ChannelEmail: "a@example.com", notification.ChannelPush: "device-1"}
	for _, ch := range notification.Channels {
		var b strings.Builder
		var args []any
		b.WriteString("INSERT INTO notifications (id, recipient, channel, priority, content, due_at, correlation_id) VALUES ")
		for i := range perChannel {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("(?, ?, ?, 'normal', 'hello', NOW(3), 'corr')")
			args = append(args, uuid.Must(uuid.NewV7()).String(), to[ch], ch)
		}
		if _, err := seed.ExecContext(t.Context(), b.String(), args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	start := time.Now()
	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, workers)
	urls := map[notification.Channel]string{}
	for _, ch := range notification.Channels {
		urls[ch] = purl + "/send"
	}
	for range workers {
		db, err := store.Open(t.Context(), dsn, 10)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		if err := store.Warm(t.Context(), db, 10); err != nil {
			t.Fatal(err)
		}
		vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{vkAddr}, DisableCache: true})
		if err != nil {
			t.Fatal(err)
		}
		defer vk.Close()
		hs := health.NewValkey(vk, health.Config{Failures: 5, Cooldown: 5 * time.Second}, "health:")
		w := delivery.New(delivery.Config{
			Concurrency: 32, MaxAttempts: 5, RetryBase: 100 * time.Millisecond, RetryCap: time.Second,
			Lease: 2 * time.Second, ReapInterval: 500 * time.Millisecond, ShutdownTimeout: 5 * time.Second,
			Weights: [3]int{70, 20, 10}, Horizon: 200 * time.Millisecond,
		}, delivery.Deps{
			Queue:   &timedQueue{Queue: store.NewQueue(db), start: start},
			Limiter: limiter.New(vk, limit, headroom, "rate:"),
			Sender:  provider.New(urls, 500*time.Millisecond, 32),
			Health:  hs,
		})
		go func() { errc <- w.Run(ctx) }()
		// As processes started one after another: the first drains alone for a while.
		time.Sleep(400 * time.Millisecond)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		var sent int
		if err := seed.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM notifications WHERE status = 'sent'").Scan(&sent); err != nil {
			t.Fatal(err)
		}
		if sent == perChannel*len(notification.Channels) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d sent within 60s", sent)
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	for range workers {
		if err := <-errc; err != nil {
			t.Errorf("worker: %v", err)
		}
	}

	claimMu.Lock()
	var early, late []time.Duration
	for _, c := range claimLog {
		if c[0] < 300*time.Millisecond {
			early = append(early, c[1])
		} else {
			late = append(late, c[1])
		}
	}
	claimMu.Unlock()
	pct := func(d []time.Duration) string {
		if len(d) == 0 {
			return "-"
		}
		slices.Sort(d)
		return fmt.Sprintf("n=%d p50=%s p90=%s p99=%s max=%s", len(d), d[len(d)/2], d[len(d)*9/10], d[len(d)*99/100], d[len(d)-1])
	}
	t.Logf("claim latency first 300ms: %s; later: %s", pct(early), pct(late))
	for _, ch := range notification.Channels {
		rs := prov.Requests(fakeprovider.Filter{Channel: string(ch)})
		times := make([]time.Time, len(rs))
		for i, r := range rs {
			times[i] = r.ReceivedAt
		}
		slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
		busiest, at := 0, 0
		for i := range times {
			j, _ := slices.BinarySearchFunc(times, times[i].Add(time.Second), func(a, b time.Time) int { return a.Compare(b) })
			if j-i > busiest {
				busiest, at = j-i, i
			}
		}
		avg := float64(len(times)-1) / times[len(times)-1].Sub(times[0]).Seconds()
		var buckets []string
		for k := 0; k <= int(times[len(times)-1].Sub(times[0])/(100*time.Millisecond)); k++ {
			from := times[0].Add(time.Duration(k) * 100 * time.Millisecond)
			n := 0
			for _, x := range times {
				if !x.Before(from) && x.Before(from.Add(100*time.Millisecond)) {
					n++
				}
			}
			buckets = append(buckets, fmt.Sprint(n))
		}
		t.Logf("%s: %d requests, average %.1f/s, busiest 1 s window %d starting %s after the first; per 100 ms: %s",
			ch, len(times), avg, busiest, times[at].Sub(times[0]).Round(time.Millisecond), strings.Join(buckets, " "))
		switch {
		case busiest <= limit:
		case raceDetector:
			// The provider double lives in this process. Under the race
			// detector on a small machine the process stalls for tens of
			// milliseconds, and requests that left on time are counted late,
			// in a clump: CI saw 8 then 12 in consecutive 100 ms on all three
			// channels at once, and 101 in a second on one (owner item 0013).
			// That measures the instrumented test process, so it is reported
			// here and judged in the run without the detector.
			t.Logf("%s: %d requests in one second, limit %d: not judged under the race detector", ch, busiest, limit)
		default:
			t.Errorf("%s: %d requests in one second, limit %d", ch, busiest, limit)
		}
		// A sanity floor only. This package's tests run beside the other
		// packages' database tests, and throughput follows the machine
		// (alone: close to the paced rate, limit*(100-headroom)/100 = 95 per
		// second; in a full parallel run with 3% headroom it was 56 to 66, owner
		// item 0011). The strict floors are the acceptance suite's
		// (TestDeliveryR8RateLimit), which runs on its own.
		if avg < 40 {
			t.Errorf("%s: average %.1f/s, want at least 40", ch, avg)
		}
	}
}

var (
	claimMu  sync.Mutex
	claimLog [][2]time.Duration // since start, duration
)

type timedQueue struct {
	notification.Queue
	start time.Time
}

func (q *timedQueue) Claim(ctx context.Context, ch notification.Channel, p notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	st := time.Now()
	jobs, err := q.Queue.Claim(ctx, ch, p, n, lease)
	claimMu.Lock()
	claimLog = append(claimLog, [2]time.Duration{st.Sub(q.start), time.Since(st)})
	claimMu.Unlock()
	return jobs, err
}
