//go:build integration

package delivery_test

import (
	"context"
	"encoding/json"
	"os"
	"slices"
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

// measureVariant is what differs between the code before decision 0026 and
// after it: the health dependency, the default budget, and the column that
// holds the budget spent. Everything else in the measurement is shared, so
// the same file runs on main with only this block changed.
var measureVariant = struct {
	name        string
	maxAttempts int    // the default APP_MAX_ATTEMPTS of that code
	budgetCol   string // the column the budget is counted in
	cooldown    time.Duration
	deps        func(vk valkey.Client) delivery.Deps
	config      func(*delivery.Config) // the dials that code adds, as cmd/app sets them
}{
	name: "after (0026: shared health, budget_spent)", maxAttempts: 10, budgetCol: "budget_spent", cooldown: 5 * time.Second,
	deps: func(vk valkey.Client) delivery.Deps {
		return delivery.Deps{Health: health.NewValkey(vk, health.Config{Failures: 5, Cooldown: 5 * time.Second}, "health:")}
	},
	config: func(c *delivery.Config) { c.ProviderTimeout, c.HealthCooldown = 5*time.Second, 5*time.Second },
}

// callLog records each worker's provider calls.
type callLog struct {
	mu    sync.Mutex
	calls map[int][]call
}

type call struct {
	at time.Time
	ok bool
}

type loggingSender struct {
	next   delivery.Sender
	worker int
	log    *callLog
}

func (s loggingSender) Send(ctx context.Context, m provider.Message) (provider.Response, error) {
	res, err := s.next.Send(ctx, m)
	if m.Channel == notification.ChannelSMS {
		s.log.mu.Lock()
		s.log.calls[s.worker] = append(s.log.calls[s.worker], call{at: time.Now(), ok: err == nil && res.Status/100 == 2})
		s.log.mu.Unlock()
	}
	return res, err
}

// TestMeasureProviderHealth measures what an sms provider outage costs, with
// the deployed defaults: 3 workers, a steady 10 sms per second from 5 s
// before the outage to its end, an outage of MEASURE_OUTAGE (default 10m)
// answering 503. Skipped unless MEASURE_PROVIDER_HEALTH=1; it prints one JSON
// line per metric. Rate 100 per second with 5% headroom, as deployed.
func TestMeasureProviderHealth(t *testing.T) {
	if os.Getenv("MEASURE_PROVIDER_HEALTH") != "1" { //nolint:forbidigo // test-run knob, not a service setting
		t.Skip("set MEASURE_PROVIDER_HEALTH=1 to measure")
	}
	outage := 10 * time.Minute
	if v := os.Getenv("MEASURE_OUTAGE"); v != "" { //nolint:forbidigo // test-run knob, not a service setting
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
		outage = d
	}
	const workers, streamEvery, lead = 3, 100 * time.Millisecond, 5 * time.Second
	_, dsn := support.MySQL(t)
	vkAddr := support.Valkey(t)
	purl, prov := support.Provider(t)
	seed, err := store.Open(t.Context(), dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seed.Close() }()

	log := &callLog{calls: map[int][]call{}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errc := make(chan error, workers)
	urls := map[notification.Channel]string{}
	for _, ch := range notification.Channels {
		urls[ch] = purl + "/send"
	}
	for i := range workers {
		db, err := store.Open(t.Context(), dsn, 10)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{vkAddr}, DisableCache: true})
		if err != nil {
			t.Fatal(err)
		}
		defer vk.Close()
		deps := measureVariant.deps(vk)
		deps.Queue = store.NewQueue(db)
		deps.Limiter = limiter.New(vk, 100, 5, "rate:")
		deps.Sender = loggingSender{next: provider.New(urls, 5*time.Second, 32), worker: i, log: log}
		cfg := delivery.Config{
			Concurrency: 32, MaxAttempts: measureVariant.maxAttempts, RetryBase: time.Second, RetryCap: 5 * time.Minute,
			Lease: 30 * time.Second, ReapInterval: 5 * time.Second, ShutdownTimeout: 5 * time.Second,
			Weights: [3]int{70, 20, 10}, Horizon: 200 * time.Millisecond,
		}
		measureVariant.config(&cfg)
		w := delivery.New(cfg, deps)
		go func() { errc <- w.Run(ctx) }()
	}

	// The stream: one sms every 100 ms until the outage ends.
	streamCtx, stopStream := context.WithCancel(ctx)
	var streamWG sync.WaitGroup
	streamWG.Go(func() {
		tk := time.NewTicker(streamEvery)
		defer tk.Stop()
		for {
			select {
			case <-streamCtx.Done():
				return
			case <-tk.C:
			}
			_, err := seed.ExecContext(ctx, "INSERT INTO notifications (id, recipient, channel, priority, content, due_at, correlation_id) VALUES (?, '+905551234567', 'sms', 'normal', 'hello', NOW(3), 'measure')",
				uuid.Must(uuid.NewV7()).String())
			if err != nil && ctx.Err() == nil {
				t.Errorf("stream insert: %v", err)
				return
			}
		}
	})
	time.Sleep(lead)
	outageStart := time.Now()
	if err := prov.SetFaults([]fakeprovider.Fault{{Channel: "sms", Mode: fakeprovider.ModeStatus, Status: 503}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(outage)
	if err := prov.SetFaults(nil); err != nil {
		t.Fatal(err)
	}
	recovery := time.Now()
	stopStream()
	streamWG.Wait()

	deadline := time.Now().Add(10 * time.Minute)
	var drained time.Time
	for {
		var left int
		if err := seed.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM notifications WHERE status IN ('pending','processing')").Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left == 0 {
			drained = time.Now()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d notifications still pending 10 min after recovery", left)
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	for range workers {
		if err := <-errc; err != nil {
			t.Errorf("worker: %v", err)
		}
	}

	emit := func(metric string, v any) {
		b, _ := json.Marshal(map[string]any{"variant": measureVariant.name, "outage": outage.String(), "metric": metric, "value": v})
		t.Log(string(b))
	}

	// (1) Failed calls per worker before that worker stopped sending
	// continuously: its failed calls after the outage started, up to its
	// first gap of at least half a cool-down (a stopped worker only probes).
	log.mu.Lock()
	perWorker := map[int]int{}
	for wk, cs := range log.calls {
		n := 0
		var prev time.Time
		for _, c := range cs {
			if c.at.Before(outageStart) || c.at.After(recovery) {
				continue
			}
			if !prev.IsZero() && c.at.Sub(prev) >= measureVariant.cooldown/2 {
				break
			}
			if !c.ok {
				n++
			}
			prev = c.at
		}
		perWorker[wk] = n
	}
	// (2) Calls per cool-down in the steady part: from 30 s into the outage.
	steadyFrom := outageStart.Add(30 * time.Second)
	steady := 0
	for _, cs := range log.calls {
		for _, c := range cs {
			if c.at.After(steadyFrom) && c.at.Before(recovery) {
				steady++
			}
		}
	}
	log.mu.Unlock()
	emit("failed calls per worker before it stopped", perWorker)
	if span := recovery.Sub(steadyFrom); span > 0 {
		emit("sms calls per cool-down in the steady outage", float64(steady)/(span.Seconds()/measureVariant.cooldown.Seconds()))
	}

	// (3) Budget spent and calls made per notification that met the outage.
	var maxB, maxA, failed, n int
	var meanB, meanA float64
	q := "SELECT COALESCE(MAX(" + measureVariant.budgetCol + "),0), COALESCE(AVG(" + measureVariant.budgetCol + "),0), COALESCE(MAX(attempts),0), COALESCE(AVG(attempts),0), " +
		"COALESCE(SUM(status = 'failed'),0), COUNT(*) FROM notifications WHERE channel = 'sms' AND created_at < ?"
	if err := seed.QueryRowContext(t.Context(), q, recovery.UTC()).Scan(&maxB, &meanB, &maxA, &meanA, &failed, &n); err != nil {
		t.Fatal(err)
	}
	emit("notifications that met the outage", n)
	emit("budget spent per notification (max, mean)", []any{maxB, meanB})
	emit("calls made per notification (max, mean)", []any{maxA, meanA})
	emit("notifications dead-lettered", failed)

	// (4) Recovery to the first accepted sms request; (5) the drain.
	rs := prov.Requests(fakeprovider.Filter{Channel: "sms"})
	var after []time.Time
	for _, r := range rs {
		if r.ReceivedAt.After(recovery) && r.Status/100 == 2 {
			after = append(after, r.ReceivedAt)
		}
	}
	slices.SortFunc(after, func(a, b time.Time) int { return a.Compare(b) })
	if len(after) > 0 {
		emit("recovery to first accepted sms (ms)", after[0].Sub(recovery).Milliseconds())
		busiest := 0
		for i := range after {
			j, _ := slices.BinarySearchFunc(after, after[i].Add(time.Second), func(a, b time.Time) int { return a.Compare(b) })
			busiest = max(busiest, j-i)
		}
		emit("drain busiest second", busiest)
		emit("drain mean per second", float64(len(after))/drained.Sub(after[0]).Seconds())
		emit("drain duration (s)", drained.Sub(recovery).Seconds())
	}
}
