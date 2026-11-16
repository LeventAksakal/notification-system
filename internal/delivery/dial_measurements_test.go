//go:build integration && measure

package delivery_test

// The measurements the delivery dials are derived from (decision 0018, "As
// built"). Not a test of behaviour: it prints numbers and fails only when the
// rig itself breaks. It runs the dials before and after a change back to back,
// alternating which goes first, so drift on a shared machine falls on both.
//
//	go test -tags "integration measure" ./internal/delivery -run TestMeasureDials -v -timeout 40m -cpu 2
//
// MEASURE_REPEATS (default 3) sets the number of rounds. MEASURE_AFTER
// overrides the "after" set, e.g. "concurrency=72,lease=9s,reap=1s,pool=24",
// to try a candidate before it becomes the default. MEASURE_SCENARIOS picks
// scenarios (default "drain,slow,open,recovery"). MEASURE_RELAX_FSYNC=1 turns
// off MySQL's per-commit flush, so the database stops being the bottleneck
// and the scenario measures the dial alone (used for the concurrency).

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/config"
	"github.com/LeventAksakal/notification-system/internal/delivery"
	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/delivery/limiter"
	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

// dialSet is the dials a round runs with.
type dialSet struct {
	name        string
	concurrency int
	lease, reap time.Duration
	pool        int
	timeout     time.Duration
}

// before is the defaults until 16 Nov 2026.
var before = dialSet{name: "before", concurrency: 32, lease: 30 * time.Second, reap: 5 * time.Second, pool: 10, timeout: 5 * time.Second}

// after is the current config defaults.
func after(t *testing.T) dialSet {
	c, err := config.Load(func(k string) string {
		if k == "APP_DATABASE_DSN" {
			return "u:p@tcp(db:3306)/app"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	s := dialSet{name: "after", concurrency: c.WorkerConcurrency, lease: c.LeaseDuration, reap: c.ReapInterval,
		pool: c.DBMaxOpenConns, timeout: c.ProviderTimeout}
	for kv := range strings.SplitSeq(os.Getenv("MEASURE_AFTER"), ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		var err error
		switch k {
		case "concurrency":
			s.concurrency, err = strconv.Atoi(v)
		case "pool":
			s.pool, err = strconv.Atoi(v)
		case "lease":
			s.lease, err = time.ParseDuration(v)
		case "reap":
			s.reap, err = time.ParseDuration(v)
		default:
			err = fmt.Errorf("unknown key %q", k)
		}
		if err != nil {
			t.Fatalf("MEASURE_AFTER %s: %v", kv, err)
		}
	}
	return s
}

// slowProviderLatency is the provider latency the concurrency is derived for
// (the assumed p99, decision 0018): every call takes this long.
const slowProviderLatency = 500 * time.Millisecond

func TestMeasureDials(t *testing.T) {
	repeats := 3
	if s := os.Getenv("MEASURE_REPEATS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		repeats = n
	}
	r := newDialRig(t)
	if os.Getenv("MEASURE_RELAX_FSYNC") == "1" {
		// support.MySQL's root password is its app password (testcontainers' mysql module).
		root, err := store.Open(t.Context(), strings.Replace(r.dsn, "app:test@", "root:test@", 1), 1)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = root.Close() }()
		for _, q := range []string{"SET GLOBAL innodb_flush_log_at_trx_commit = 2", "SET GLOBAL sync_binlog = 0"} {
			if _, err := root.ExecContext(t.Context(), q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		t.Log("MEASURE MySQL per-commit flush off")
	}
	scenarios := os.Getenv("MEASURE_SCENARIOS")
	if scenarios == "" {
		scenarios = "drain,slow,open,recovery"
	}
	on := func(name string) bool { return slices.Contains(strings.Split(scenarios, ","), name) }
	r.writeServiceTime(t)
	sets := []dialSet{before, after(t)}
	t.Logf("sets: %+v", sets)
	for rep := range repeats {
		order := sets
		if rep%2 == 1 {
			order = []dialSet{sets[1], sets[0]}
		}
		for _, s := range order {
			tag := fmt.Sprintf("round %d %s", rep+1, s.name)
			if on("drain") {
				r.drain(t, tag, s)
			}
			if on("slow") {
				r.slowProviderOneWorkerLost(t, tag, s)
			}
			if on("open") {
				r.openLoop(t, tag, s)
			}
			if on("recovery") {
				r.recovery(t, tag, s)
			}
		}
	}
}

// rig is one MySQL, one Valkey and one provider double for every round.
type dialRig struct {
	dsn, vk, purl string
	prov          *fakeprovider.Server
	admin         *sql.DB
	run           int
}

func newDialRig(t *testing.T) *dialRig {
	_, dsn := support.MySQL(t)
	vk := support.Valkey(t)
	purl, prov := support.Provider(t)
	admin, err := store.Open(t.Context(), dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	return &dialRig{dsn: dsn, vk: vk, purl: purl + "/send", prov: prov, admin: admin}
}

func (r *dialRig) reset(t *testing.T) {
	t.Helper()
	for _, q := range []string{"DELETE FROM delivery_attempts", "DELETE FROM notifications"} {
		if _, err := r.admin.ExecContext(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	r.prov.Reset()
	if err := r.prov.SetFaults(nil); err != nil {
		t.Fatal(err)
	}
	r.run++
}

var measureRecipients = map[notification.Channel]string{notification.ChannelSMS: "+905551234567", notification.ChannelEmail: "a@example.com", notification.ChannelPush: "device-1"}

// seed inserts n due notifications on ch and returns their ids with the local
// time the insert committed.
func (r *dialRig) seed(t *testing.T, ch notification.Channel, n int) map[string]time.Time {
	t.Helper()
	ids := make(map[string]time.Time, n)
	for start := 0; start < n; start += 500 {
		end := min(start+500, n)
		var b strings.Builder
		var args []any
		var batch []string
		b.WriteString("INSERT INTO notifications (id, recipient, channel, priority, content, due_at, correlation_id) VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				b.WriteString(",")
			}
			id := uuid.Must(uuid.NewV7()).String()
			batch = append(batch, id)
			b.WriteString("(?, ?, ?, 'normal', 'hello', NOW(3), 'corr')")
			args = append(args, id, measureRecipients[ch], ch)
		}
		if _, err := r.admin.ExecContext(t.Context(), b.String(), args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
		now := time.Now()
		for _, id := range batch {
			ids[id] = now
		}
	}
	return ids
}

// probe collects the timings of one round.
type probe struct {
	mu                   sync.Mutex
	claims, writes, call []time.Duration
}

func (p *probe) add(to *[]time.Duration, d time.Duration) {
	p.mu.Lock()
	*to = append(*to, d)
	p.mu.Unlock()
}

type measuredQueue struct {
	notification.Queue
	p *probe
}

func (q measuredQueue) Claim(ctx context.Context, ch notification.Channel, pr notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	st := time.Now()
	jobs, err := q.Queue.Claim(ctx, ch, pr, n, lease)
	q.p.add(&q.p.claims, time.Since(st))
	return jobs, err
}

func (q measuredQueue) MarkSent(ctx context.Context, job notification.Job, a notification.Attempt) error {
	st := time.Now()
	err := q.Queue.MarkSent(ctx, job, a)
	q.p.add(&q.p.writes, time.Since(st))
	return err
}

func (q measuredQueue) Reschedule(ctx context.Context, job notification.Job, a notification.Attempt, due time.Time) error {
	st := time.Now()
	err := q.Queue.Reschedule(ctx, job, a, due)
	q.p.add(&q.p.writes, time.Since(st))
	return err
}

type measuredSender struct {
	delivery.Sender
	p *probe
}

func (s measuredSender) Send(ctx context.Context, m provider.Message) (provider.Response, error) {
	st := time.Now()
	res, err := s.Sender.Send(ctx, m)
	s.p.add(&s.p.call, time.Since(st))
	return res, err
}

// fleet is the workers of one round.
type fleet struct {
	cancel  context.CancelFunc
	errc    chan error
	n       int
	dbs     []*sql.DB
	maxUse  []int
	stopUse chan struct{}
	useWG   sync.WaitGroup
}

func (r *dialRig) startWorkers(t *testing.T, s dialSet, n int, p *probe) *fleet {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	f := &fleet{cancel: cancel, errc: make(chan error, n), n: n, maxUse: make([]int, n), stopUse: make(chan struct{})}
	urls := map[notification.Channel]string{}
	for _, ch := range notification.Channels {
		urls[ch] = r.purl
	}
	for i := range n {
		db, err := store.Open(t.Context(), r.dsn, s.pool)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Warm(t.Context(), db, s.pool); err != nil {
			t.Fatal(err)
		}
		f.dbs = append(f.dbs, db)
		vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{r.vk}, DisableCache: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(vk.Close)
		hs := health.NewValkey(vk, health.Config{Failures: 5, Cooldown: 5 * time.Second}, fmt.Sprintf("mh%d:", r.run))
		w := delivery.New(delivery.Config{
			Concurrency: s.concurrency, MaxAttempts: 12, RetryBase: time.Second, RetryCap: 5 * time.Minute,
			Lease: s.lease, ReapInterval: s.reap, ShutdownTimeout: 10 * time.Second,
			Weights: [3]int{70, 20, 10}, Horizon: 200 * time.Millisecond,
		}, delivery.Deps{
			Queue:   measuredQueue{Queue: store.NewQueue(db), p: p},
			Limiter: limiter.New(vk, 100, 5, fmt.Sprintf("m%d:", r.run)),
			Sender:  measuredSender{Sender: provider.New(urls, s.timeout, s.concurrency), p: p},
			Health:  hs,
		})
		go func() { f.errc <- w.Run(ctx) }()
		f.useWG.Add(1)
		go func() {
			defer f.useWG.Done()
			tk := time.NewTicker(2 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-f.stopUse:
					return
				case <-tk.C:
					if u := db.Stats().InUse; u > f.maxUse[i] {
						f.maxUse[i] = u
					}
				}
			}
		}()
	}
	return f
}

// stop ends the workers and returns the pool's wait count, wait time and
// highest in-use count over all of them.
func (f *fleet) stop(t *testing.T) (waits int64, waited time.Duration, maxUse int) {
	t.Helper()
	f.cancel()
	for range f.n {
		if err := <-f.errc; err != nil {
			t.Errorf("worker: %v", err)
		}
	}
	close(f.stopUse)
	f.useWG.Wait()
	for i, db := range f.dbs {
		st := db.Stats()
		waits += st.WaitCount
		waited += st.WaitDuration
		maxUse = max(maxUse, f.maxUse[i])
		_ = db.Close()
	}
	return waits, waited, maxUse
}

func (r *dialRig) waitSent(t *testing.T, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		var sent int
		if err := r.admin.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM notifications WHERE status = 'sent'").Scan(&sent); err != nil {
			t.Fatal(err)
		}
		if sent >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d sent within %s", sent, want, within)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func pct(d []time.Duration) string {
	if len(d) == 0 {
		return "n=0"
	}
	d = slices.Clone(d)
	slices.Sort(d)
	at := func(p int) time.Duration { return d[min(len(d)-1, len(d)*p/100)] }
	return fmt.Sprintf("n=%d p50=%s p99=%s max=%s", len(d), at(50).Round(10*time.Microsecond), at(99).Round(10*time.Microsecond), d[len(d)-1].Round(10*time.Microsecond))
}

// rate returns the arrival rate the provider saw on ch, first to last request.
func (r *dialRig) rate(ch notification.Channel) (float64, int) {
	rs := r.prov.Requests(fakeprovider.Filter{Channel: string(ch)})
	if len(rs) < 2 {
		return 0, len(rs)
	}
	first, last := rs[0].ReceivedAt, rs[0].ReceivedAt
	for _, x := range rs {
		first = earliest(first, x.ReceivedAt)
		last = latest(last, x.ReceivedAt)
	}
	return float64(len(rs)-1) / last.Sub(first).Seconds(), len(rs)
}

func earliest(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// drain: 3 workers, a backlog of 1000 per channel, an instant provider. The
// throughput per channel, the claim, result-write and provider-call times,
// and the pool's waits.
func (r *dialRig) drain(t *testing.T, tag string, s dialSet) {
	const perChannel = 500
	r.reset(t)
	for _, ch := range notification.Channels {
		r.seed(t, ch, perChannel)
	}
	p := &probe{}
	f := r.startWorkers(t, s, 3, p)
	r.waitSent(t, perChannel*len(notification.Channels), 5*time.Minute)
	waits, waited, maxUse := f.stop(t)
	var rates []string
	for _, ch := range notification.Channels {
		v, _ := r.rate(ch)
		rates = append(rates, fmt.Sprintf("%s %.1f/s", ch, v))
	}
	t.Logf("MEASURE %s drain: %s | claim %s | write %s | call %s | pool waits %d (%s), max in use %d of %d",
		tag, strings.Join(rates, ", "), pct(p.claims), pct(p.writes), pct(p.call), waits, waited, maxUse, s.pool)
}

// slowProviderOneWorkerLost: every call takes slowProviderLatency and one of
// three workers is gone; the total throughput is what the concurrency allows.
func (r *dialRig) slowProviderOneWorkerLost(t *testing.T, tag string, s dialSet) {
	const perChannel = 600
	r.reset(t)
	if err := r.prov.SetFaults([]fakeprovider.Fault{{Mode: fakeprovider.ModeDelay, DelayMS: int(slowProviderLatency / time.Millisecond)}}); err != nil {
		t.Fatal(err)
	}
	for _, ch := range notification.Channels {
		r.seed(t, ch, perChannel)
	}
	p := &probe{}
	f := r.startWorkers(t, s, 2, p)
	r.waitSent(t, perChannel*len(notification.Channels), 5*time.Minute)
	waits, waited, maxUse := f.stop(t)
	total := 0.0
	var rates []string
	for _, ch := range notification.Channels {
		v, _ := r.rate(ch)
		total += v
		rates = append(rates, fmt.Sprintf("%s %.1f/s", ch, v))
	}
	t.Logf("MEASURE %s slow provider (%s), 2 workers: total %.1f/s (%s) | call %s | write %s | pool waits %d (%s), max in use %d",
		tag, slowProviderLatency, total, strings.Join(rates, ", "), pct(p.call), pct(p.writes), waits, waited, maxUse)
}

// openLoop: 3 workers, 40 per second per channel arriving for 10 s (under
// half the paced 95, so a loaded machine still keeps up), an instant provider. Accept (insert committed) to provider
// arrival, per notification.
func (r *dialRig) openLoop(t *testing.T, tag string, s dialSet) {
	const perTick, ticks = 4, 100 // 4 per channel every 100 ms for 10 s
	r.reset(t)
	p := &probe{}
	f := r.startWorkers(t, s, 3, p)
	accepted := map[string]time.Time{}
	tk := time.NewTicker(100 * time.Millisecond)
	for range ticks {
		<-tk.C
		for _, ch := range notification.Channels {
			for id, at := range r.seed(t, ch, perTick) {
				accepted[id] = at
			}
		}
	}
	tk.Stop()
	r.waitSent(t, len(accepted), 3*time.Minute)
	waits, waited, maxUse := f.stop(t)
	var lat []time.Duration
	for _, ch := range notification.Channels {
		for _, x := range r.prov.Requests(fakeprovider.Filter{Channel: string(ch)}) {
			if at, ok := accepted[x.IdempotencyKey]; ok {
				lat = append(lat, x.ReceivedAt.Sub(at))
			}
		}
	}
	t.Logf("MEASURE %s open loop 40/s per channel: accept-to-provider %s | write %s | pool waits %d (%s), max in use %d",
		tag, pct(lat), pct(p.writes), waits, waited, maxUse)
}

// recovery: a worker claims 30 notifications and dies at once (its claims
// are never written); one live worker is running. The time from the claim to
// each notification's arrival at the provider; the worst case is lease +
// reap interval.
func (r *dialRig) recovery(t *testing.T, tag string, s dialSet) {
	const n = 30
	r.reset(t)
	r.seed(t, notification.ChannelSMS, n)
	dead := store.NewQueue(r.admin)
	claimed := time.Now()
	jobs, err := dead.Claim(t.Context(), notification.ChannelSMS, notification.PriorityNormal, n, s.lease)
	if err != nil || len(jobs) != n {
		t.Fatalf("dead worker's claim: %d jobs, %v", len(jobs), err)
	}
	p := &probe{}
	f := r.startWorkers(t, s, 1, p)
	r.waitSent(t, n, s.lease+s.reap+30*time.Second)
	_, _, _ = f.stop(t)
	var rec []time.Duration
	for _, x := range r.prov.Requests(fakeprovider.Filter{Channel: string(notification.ChannelSMS)}) {
		rec = append(rec, x.ReceivedAt.Sub(claimed))
	}
	t.Logf("MEASURE %s recovery after a crash (lease %s, reap %s): claim-to-provider %s", tag, s.lease, s.reap, pct(rec))
}

// writeServiceTime: the fenced result write alone, one at a time on one
// connection, so no pool wait and no other worker is in it: the database's
// and the driver's time for MarkSent. 300 claimed rows, written one by one.
func (r *dialRig) writeServiceTime(t *testing.T) {
	const n = 300
	r.reset(t)
	r.seed(t, notification.ChannelSMS, n)
	db, err := store.Open(t.Context(), r.dsn, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	q := store.NewQueue(db)
	jobs, err := q.Claim(t.Context(), notification.ChannelSMS, notification.PriorityNormal, n, time.Minute)
	if err != nil || len(jobs) != n {
		t.Fatalf("claim: %d jobs, %v", len(jobs), err)
	}
	var d []time.Duration
	for _, j := range jobs {
		st := time.Now()
		if err := q.MarkSent(t.Context(), j, notification.Attempt{StartedAt: st, Outcome: notification.OutcomeAccepted, HTTPStatus: 202, ProviderMessageID: "m"}); err != nil {
			t.Fatal(err)
		}
		d = append(d, time.Since(st))
	}
	t.Logf("MEASURE write service time (MarkSent alone, one connection): %s", pct(d))
}
