//go:build integration

package delivery_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/LeventAksakal/notification-system/internal/delivery"
	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/delivery/limiter"
	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

// measure turns on TestMeasureAttemptWriteCost (decision 0005, "Attempts
// written before the call"); it takes about a minute and only logs numbers:
//
//	go test -tags integration ./internal/delivery -run TestMeasure -measure -v
//
// This file uses nothing the attempt-first change added, so the same file
// measures the tree before it.
var measure = flag.Bool("measure", false, "run the attempt-write cost measurement (about a minute)")

// claimTimer times Claim: after the attempt-first change the claim also
// writes the in-flight attempts, so its latency carries the extra write.
type claimTimer struct {
	notification.Queue
	mu    *sync.Mutex
	times *[]time.Duration
}

func (q claimTimer) Claim(ctx context.Context, ch notification.Channel, p notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	st := time.Now()
	jobs, err := q.Queue.Claim(ctx, ch, p, n, lease)
	if len(jobs) > 0 {
		q.mu.Lock()
		*q.times = append(*q.times, time.Since(st))
		q.mu.Unlock()
	}
	return jobs, err
}

// timedClaims returns a wrap for rig.worker and a report of the claims that
// returned jobs.
func timedClaims() (func(notification.Queue) notification.Queue, func() string) {
	var mu sync.Mutex
	var times []time.Duration
	return func(q notification.Queue) notification.Queue { return claimTimer{Queue: q, mu: &mu, times: &times} },
		func() string { mu.Lock(); defer mu.Unlock(); return quantiles(times) }
}

// rig is one database, one Valkey prefix and one provider double for workers
// started in-process.
type rig struct {
	dsn, vk, url, prefix string
	prov                 *fakeprovider.Server
	db                   *sql.DB
}

func newRig(t *testing.T, opts ...fakeprovider.Option) *rig {
	t.Helper()
	_, dsn := support.MySQL(t)
	r := &rig{dsn: dsn, vk: support.Valkey(t), prefix: "rate:" + uuid.NewString() + ":"}
	r.url, r.prov = support.Provider(t, opts...)
	db, err := store.Open(t.Context(), dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r.db = db
	return r
}

var recipientOf = map[notification.Channel]string{notification.ChannelSMS: "+905551234567", notification.ChannelEmail: "a@example.com", notification.ChannelPush: "device-1"}

// seed inserts n pending notifications of ch due now and returns their ids.
func (r *rig) seed(t *testing.T, ch notification.Channel, n int) []string {
	t.Helper()
	var b strings.Builder
	var args []any
	ids := make([]string, n)
	b.WriteString("INSERT INTO notifications (id, recipient, channel, priority, content, due_at, correlation_id) VALUES ")
	for i := range n {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("(?, ?, ?, 'normal', 'hello', NOW(3), 'corr')")
		ids[i] = uuid.Must(uuid.NewV7()).String()
		args = append(args, ids[i], recipientOf[ch], ch)
	}
	if _, err := r.db.ExecContext(t.Context(), b.String(), args...); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return ids
}

func (r *rig) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := r.db.QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// worker starts one worker with the deployed rate defaults (100 per second,
// 5% headroom) and the given lease; wrap, when set, stands between it and the
// store. stop shuts it down and waits for it.
func (r *rig) worker(t *testing.T, mp *sdkmetric.MeterProvider, lease time.Duration, wrap func(notification.Queue) notification.Queue) (stop func()) {
	t.Helper()
	db, err := store.Open(t.Context(), r.dsn, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Warm(t.Context(), db, 10); err != nil {
		t.Fatal(err)
	}
	vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{r.vk}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	urls := map[notification.Channel]string{}
	for _, ch := range notification.Channels {
		urls[ch] = r.url + "/send"
	}
	var q notification.Queue = store.NewQueue(db)
	if wrap != nil {
		q = wrap(q)
	}
	deps := delivery.Deps{
		Queue:   q,
		Limiter: limiter.New(vk, 100, 5, r.prefix),
		Sender:  provider.New(urls, 500*time.Millisecond, 32),
		Health:  health.NewValkey(vk, health.Config{Failures: 5, Cooldown: 5 * time.Second}, r.prefix+"health:"),
	}
	if mp != nil {
		deps.Meter = mp
	}
	w := delivery.New(delivery.Config{
		Concurrency: 32, MaxAttempts: 5, RetryBase: 100 * time.Millisecond, RetryCap: time.Second,
		Lease: lease, ReapInterval: 200 * time.Millisecond, ShutdownTimeout: 5 * time.Second,
		Weights: [3]int{70, 20, 10}, Horizon: 200 * time.Millisecond,
	}, deps)
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-done; err != nil {
			t.Errorf("worker: %v", err)
		}
		vk.Close()
		_ = db.Close()
	}
	t.Cleanup(stop)
	return stop
}

// waitFor polls cond every 20 ms until it holds or within passes.
func waitFor(t *testing.T, within time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		ok, state := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s (%s)", what, within, state)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func quantiles(ds []time.Duration) string {
	if len(ds) == 0 {
		return "n=0"
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	at := func(p float64) time.Duration { return s[min(len(s)-1, int(float64(len(s))*p))] }
	return fmt.Sprintf("n=%d p50=%s p99=%s max=%s", len(s), at(0.5).Round(100*time.Microsecond), at(0.99).Round(100*time.Microsecond), s[len(s)-1].Round(100*time.Microsecond))
}

func TestMeasureAttemptWriteCost(t *testing.T) {
	if !*measure {
		t.Skip("pass -measure")
	}
	t.Run("three workers at the rate limit", func(t *testing.T) {
		const perChannel, workers = 500, 3
		r := newRig(t)
		for _, ch := range notification.Channels {
			r.seed(t, ch, perChannel)
		}
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		start := time.Now()
		wrap, claims := timedClaims()
		var stops []func()
		for range workers {
			stops = append(stops, r.worker(t, mp, 2*time.Second, wrap))
			time.Sleep(400 * time.Millisecond)
		}
		total := perChannel * len(notification.Channels)
		waitFor(t, 90*time.Second, "all sent", func() (bool, string) {
			n := r.count(t, "SELECT COUNT(*) FROM notifications WHERE status = 'sent'")
			return n == total, fmt.Sprintf("%d of %d", n, total)
		})
		took := time.Since(start)
		for _, stop := range stops {
			stop()
		}
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(t.Context(), &rm); err != nil {
			t.Fatal(err)
		}
		givenUp := map[string]int64{}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != "notifier.slots.given_up" {
					continue
				}
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					reason, _ := dp.Attributes.Value("reason")
					givenUp[reason.AsString()] += dp.Value
				}
			}
		}
		t.Logf("%d sent in %s; slots given up by reason: %v", total, took.Round(time.Millisecond), givenUp)
		for _, ch := range notification.Channels {
			rs := r.prov.Requests(fakeprovider.Filter{Channel: string(ch)})
			times := make([]time.Time, len(rs))
			for i, x := range rs {
				times[i] = x.ReceivedAt
			}
			slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
			busiest := 0
			gaps := make([]time.Duration, 0, len(times))
			for i := range times {
				j, _ := slices.BinarySearchFunc(times, times[i].Add(time.Second), func(a, b time.Time) int { return a.Compare(b) })
				busiest = max(busiest, j-i)
				if i > 0 {
					gaps = append(gaps, times[i].Sub(times[i-1]))
				}
			}
			avg := float64(len(times)-1) / times[len(times)-1].Sub(times[0]).Seconds()
			t.Logf("%s: %d requests, %.1f/s, busiest 1 s window %d; gap between arrivals %s", ch, len(times), avg, busiest, quantiles(gaps))
		}
		t.Logf("claims that returned jobs: %s", claims())
	})

	t.Run("one idle worker, one notification at a time", func(t *testing.T) {
		r := newRig(t)
		wrap, claims := timedClaims()
		r.worker(t, nil, 2*time.Second, wrap)
		var lat []time.Duration
		for range 60 {
			id := r.seed(t, notification.ChannelSMS, 1)[0]
			st := time.Now()
			waitFor(t, 10*time.Second, "the provider gets it", func() (bool, string) {
				return len(r.prov.Requests(fakeprovider.Filter{Key: id})) > 0, ""
			})
			lat = append(lat, r.prov.Requests(fakeprovider.Filter{Key: id})[0].ReceivedAt.Sub(st))
			time.Sleep(150 * time.Millisecond)
		}
		t.Logf("insert to provider arrival, idle: %s; claims: %s", quantiles(lat), claims())
	})
}
