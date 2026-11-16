//go:build integration

package delivery_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

// TestWorkersKeepTheRateWhileClaimsAreSlow is the scenario of owner item 0011
// (300 sms, 900 email, 900 push, 3 workers, the sms provider hanging) with
// claims held before they reach MySQL, as a busy database answered while
// another Docker stack started (claims up to 1.6 s). The fast channels must
// keep close to the paced rate (the send alignment follows the measured claim
// time, record 0009) and the provider must never see more than the limit in
// one second.
//
// In the first two cases every row is high priority and the weights give high
// everything, so one claim is one query and the claim time is exactly the
// delay. The third holds every query of the default mix (all rows normal,
// weights 70/20/10: about four queries per claim, about 6.5 s); it runs only
// with DELIVERY_SLOW_CLAIM_STRESS=1.
//
// The floors are sanity floors, as in TestWorkersHoldTheRateAtTheProvider:
// this package's tests share the machine with the other packages'. The fixed
// 200 ms alignment sent nothing in any case (0 of 1800 in 60 s); the numbers
// measured alone are in .agents/pr/adaptive-align.md and record 0009.
func TestWorkersKeepTheRateWhileClaimsAreSlow(t *testing.T) {
	_, dsn := support.MySQL(t)
	vkAddr := support.Valkey(t)
	seed, err := store.Open(t.Context(), dsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seed.Close() }()
	onlyHigh := [3]int{100, 0, 0}
	for i, c := range []slowClaimCase{
		{name: "every claim takes 1.6 s", priority: "high", weights: onlyHigh, floor: 60,
			delay: func() time.Duration { return 1600 * time.Millisecond }},
		{name: "claims take 0 to 3.2 s", priority: "high", weights: onlyHigh, floor: 60,
			delay: func() time.Duration { return time.Duration(rand.Int64N(int64(3200 * time.Millisecond))) }}, //nolint:gosec // test jitter
		{name: "every claim query of the default mix takes 1.6 s", priority: "normal", weights: [3]int{70, 20, 10}, floor: 40, stress: true,
			delay: func() time.Duration { return 1600 * time.Millisecond }},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.stress && os.Getenv("DELIVERY_SLOW_CLAIM_STRESS") != "1" { //nolint:forbidigo // test-run knob, not a service setting
				t.Skip("set DELIVERY_SLOW_CLAIM_STRESS=1 to run")
			}
			if _, err := seed.ExecContext(t.Context(), "DELETE FROM delivery_attempts"); err != nil {
				t.Fatal(err)
			}
			if _, err := seed.ExecContext(t.Context(), "DELETE FROM notifications"); err != nil {
				t.Fatal(err)
			}
			slowClaimRun(t, dsn, vkAddr, fmt.Sprintf("rate%d:", i), seed, c)
		})
	}
}

type slowClaimCase struct {
	name     string
	priority string
	weights  [3]int
	delay    func() time.Duration
	floor    float64 // per second, each fast channel
	stress   bool
}

func slowClaimRun(t *testing.T, dsn, vkAddr, prefix string, seed *sql.DB, c slowClaimCase) {
	// limit and headroom are the deployed defaults: 100 per second, paced 5% under.
	const perFast, perSMS, workers, limit, headroom = 900, 300, 3, 100, 5
	purl, prov := support.Provider(t)
	if err := prov.SetFaults([]fakeprovider.Fault{{Channel: "sms", Mode: fakeprovider.ModeDelay, DelayMS: 2000}}); err != nil {
		t.Fatal(err)
	}
	to := map[notification.Channel]string{notification.ChannelSMS: "+905551234567", notification.ChannelEmail: "a@example.com", notification.ChannelPush: "device-1"}
	for _, ch := range notification.Channels {
		n := perFast
		if ch == notification.ChannelSMS {
			n = perSMS
		}
		var b strings.Builder
		var args []any
		b.WriteString("INSERT INTO notifications (id, recipient, channel, priority, content, due_at, correlation_id) VALUES ")
		for i := range n {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("(?, ?, ?, ?, 'hello', NOW(3), 'corr')")
			args = append(args, uuid.Must(uuid.NewV7()).String(), to[ch], ch, c.priority)
		}
		if _, err := seed.ExecContext(t.Context(), b.String(), args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// DELIVERY_SLOW_CLAIM_LOG=<path prefix> writes the workers' debug log of
	// each case (every reservation, claimed group and request with its slot)
	// to <prefix><case>.log, and the provider's arrivals per channel (key,
	// microseconds) to <prefix>arrivals-<channel>.csv: together they tell a
	// grid denser than the limit from requests bunched in transit.
	if dir := os.Getenv("DELIVERY_SLOW_CLAIM_LOG"); dir != "" { //nolint:forbidigo // test-run knob, not a service setting
		f, err := os.Create(dir + strings.TrimSuffix(prefix, ":") + ".log") //nolint:gosec // a path the person running the test chose
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(f, &slog.HandlerOptions{Level: slog.LevelDebug})))
		defer slog.SetDefault(prev)
	}
	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, workers)
	urls := map[notification.Channel]string{}
	for _, ch := range notification.Channels {
		urls[ch] = purl + "/send"
	}
	var released atomic.Int64
	stats, realStats := &claimStats{}, &claimStats{}
	var dbs []*sql.DB
	for range workers {
		db, err := store.Open(t.Context(), dsn, 10)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		dbs = append(dbs, db)
		if err := store.Warm(t.Context(), db, 10); err != nil {
			t.Fatal(err)
		}
		vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{vkAddr}, DisableCache: true})
		if err != nil {
			t.Fatal(err)
		}
		defer vk.Close()
		hs := health.NewValkey(vk, health.Config{Failures: 5, Cooldown: time.Second}, "health:"+prefix)
		w := delivery.New(delivery.Config{
			Concurrency: 32, MaxAttempts: 5, RetryBase: 100 * time.Millisecond, RetryCap: time.Second,
			Lease: 30 * time.Second, ReapInterval: 500 * time.Millisecond, ShutdownTimeout: 5 * time.Second,
			Weights: c.weights, Horizon: 200 * time.Millisecond,
		}, delivery.Deps{
			Queue:   &slowQueue{Queue: store.NewQueue(db), delay: c.delay, stats: stats, inDB: realStats, released: &released},
			Limiter: limiter.New(vk, limit, headroom, prefix),
			Sender:  provider.New(urls, 500*time.Millisecond, 32),
			Health:  hs,
		})
		go func() { errc <- w.Run(ctx) }()
	}
	// 900 at 95 per second take 9.5 s; 60 s is enough at a sixth of that.
	deadline := time.Now().Add(60 * time.Second)
	for {
		var sent int
		if err := seed.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM notifications WHERE status = 'sent' AND channel <> 'sms'").Scan(&sent); err != nil {
			t.Fatal(err)
		}
		if sent == 2*perFast {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("only %d of %d email and push sent within 60s", sent, 2*perFast)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	for range workers {
		if err := <-errc; err != nil {
			t.Errorf("worker: %v", err)
		}
	}

	t.Logf("%d claimed rows released unsent; claim query time %s, of it in the database %s", released.Load(), stats.summary(), realStats.summary())
	for i, db := range dbs {
		st := db.Stats()
		t.Logf("worker %d pool: %d waits for a connection, %s waiting in all", i, st.WaitCount, st.WaitDuration.Round(time.Millisecond))
	}
	for _, ch := range []notification.Channel{notification.ChannelEmail, notification.ChannelPush} {
		rs := prov.Requests(fakeprovider.Filter{Channel: string(ch)})
		times := make([]time.Time, len(rs))
		for i, r := range rs {
			times[i] = r.ReceivedAt
		}
		if len(times) < 2 {
			t.Errorf("%s: %d requests", ch, len(times))
			continue
		}
		if dir := os.Getenv("DELIVERY_SLOW_CLAIM_LOG"); dir != "" { //nolint:forbidigo // test-run knob, not a service setting
			var b strings.Builder
			for _, r := range rs {
				fmt.Fprintln(&b, r.IdempotencyKey+","+fmt.Sprint(r.ReceivedAt.UnixMicro()))
			}
			_ = os.WriteFile(dir+"arrivals-"+string(ch)+".csv", []byte(b.String()), 0o600) //nolint:gosec // a path the person running the test chose
		}
		slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
		busiest := 0
		for i := range times {
			j, _ := slices.BinarySearchFunc(times, times[i].Add(time.Second), func(a, b time.Time) int { return a.Compare(b) })
			busiest = max(busiest, j-i)
		}
		avg := float64(len(times)-1) / times[len(times)-1].Sub(times[0]).Seconds()
		// Steady state as the acceptance suite has it: the first and the
		// last second dropped.
		from, to := times[0].Add(time.Second), times[len(times)-1].Add(-time.Second)
		var mid []time.Time
		for _, x := range times {
			if !x.Before(from) && !x.After(to) {
				mid = append(mid, x)
			}
		}
		steady := 0.0
		if len(mid) > 1 {
			steady = float64(len(mid)-1) / mid[len(mid)-1].Sub(mid[0]).Seconds()
		}
		buckets := make([]int, int(times[len(times)-1].Sub(times[0])/time.Second)+1)
		for _, x := range times {
			buckets[int(x.Sub(times[0])/time.Second)]++
		}
		t.Logf("%s: %d requests, average %.1f/s from the first to the last, %.1f/s in steady state, busiest 1 s window %d; per second: %v",
			ch, len(times), avg, steady, busiest, buckets)
		if busiest > limit && !raceDetector {
			t.Errorf("%s: %d requests in one second, limit %d", ch, busiest, limit)
		}
		if avg < c.floor {
			t.Errorf("%s: average %.1f/s, want at least %.0f", ch, avg, c.floor)
		}
	}
}

// slowQueue holds every Claim for delay before it reaches the database, and
// counts the rows released unsent.
type slowQueue struct {
	notification.Queue
	delay    func() time.Duration
	stats    *claimStats
	inDB     *claimStats
	released *atomic.Int64
}

func (q *slowQueue) Claim(ctx context.Context, ch notification.Channel, p notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	st := time.Now()
	t := time.NewTimer(q.delay())
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.C:
	}
	began := time.Now()
	jobs, err := q.Queue.Claim(ctx, ch, p, n, lease)
	q.inDB.add(time.Since(began))
	q.stats.add(time.Since(st))
	return jobs, err
}

func (q *slowQueue) Release(ctx context.Context, job notification.Job, dueAt time.Time) error {
	q.released.Add(1)
	return q.Queue.Release(ctx, job, dueAt)
}

type claimStats struct {
	mu sync.Mutex
	d  []time.Duration
}

func (s *claimStats) add(d time.Duration) {
	s.mu.Lock()
	s.d = append(s.d, d)
	s.mu.Unlock()
}

func (s *claimStats) summary() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.d) == 0 {
		return "-"
	}
	d := slices.Clone(s.d)
	slices.Sort(d)
	return fmt.Sprintf("n=%d p50=%s p99=%s max=%s", len(d), d[len(d)/2], d[len(d)*99/100], d[len(d)-1])
}
