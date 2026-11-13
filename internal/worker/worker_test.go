package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// memQueue is an in-memory core.Queue with the same fencing rules.
type memQueue struct {
	mu       sync.Mutex
	rows     map[string]*memRow
	order    []string
	claims   map[core.Priority]int
	results  map[string][]core.AttemptOutcome
	released int
}

type memRow struct {
	job    core.Job
	status core.Status
	kind   core.FailureKind
	due    time.Time
}

func newMemQueue() *memQueue {
	return &memQueue{rows: map[string]*memRow{}, claims: map[core.Priority]int{}, results: map[string][]core.AttemptOutcome{}}
}

func (q *memQueue) add(n int, ch core.Channel, p core.Priority) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for range n {
		id := fmt.Sprintf("%s-%s-%d", ch, p, len(q.order))
		q.rows[id] = &memRow{job: core.Job{ID: id, Channel: ch, Priority: p, CorrelationID: "c-" + id}, status: core.StatusPending}
		q.order = append(q.order, id)
	}
}

func (q *memQueue) Claim(_ context.Context, ch core.Channel, p core.Priority, n int, lease time.Duration) ([]core.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []core.Job
	for _, id := range q.order {
		r := q.rows[id]
		if len(out) == n {
			break
		}
		if r.status == core.StatusPending && r.job.Channel == ch && r.job.Priority == p && !time.Now().Before(r.due) {
			r.status = core.StatusProcessing
			r.job.ClaimVersion++
			r.job.LeaseUntil = time.Now().Add(lease)
			out = append(out, r.job)
			q.claims[p]++
		}
	}
	return out, nil
}

func (q *memQueue) finish(job core.Job, a *core.Attempt, to core.Status, kind core.FailureKind, due time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	r := q.rows[job.ID]
	if r == nil || r.status != core.StatusProcessing || r.job.ClaimVersion != job.ClaimVersion {
		return core.ErrLeaseLost
	}
	r.status, r.kind, r.due = to, kind, due
	if a != nil {
		r.job.Attempts++
		q.results[job.ID] = append(q.results[job.ID], a.Outcome)
	} else {
		q.released++
	}
	return nil
}

func (q *memQueue) MarkSent(_ context.Context, j core.Job, a core.Attempt) error {
	return q.finish(j, &a, core.StatusSent, "", time.Time{})
}
func (q *memQueue) Reschedule(_ context.Context, j core.Job, a core.Attempt, due time.Time) error {
	return q.finish(j, &a, core.StatusPending, "", due)
}
func (q *memQueue) MarkFailed(_ context.Context, j core.Job, a core.Attempt, k core.FailureKind) error {
	return q.finish(j, &a, core.StatusFailed, k, time.Time{})
}
func (q *memQueue) Release(_ context.Context, j core.Job, due time.Time) error {
	return q.finish(j, nil, core.StatusPending, "", due)
}
func (q *memQueue) ReapExpired(context.Context) (int, error) { return 0, nil }

func (q *memQueue) count(st core.Status) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, r := range q.rows {
		if r.status == st {
			n++
		}
	}
	return n
}

func (q *memQueue) row(id string) memRow {
	q.mu.Lock()
	defer q.mu.Unlock()
	return *q.rows[id]
}

// openLimiter grants every permit; failing makes it unreachable.
type openLimiter struct {
	mu    sync.Mutex
	err   error
	takes int
}

func (l *openLimiter) Take(context.Context, core.Channel, int) (int, time.Duration, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.takes++
	if l.err != nil {
		return 0, 0, l.err
	}
	return 1, 0, nil
}

// scriptSender answers by function.
type scriptSender struct {
	fn func(ctx context.Context, m provider.Message) (provider.Response, error)
}

func (s scriptSender) Send(ctx context.Context, m provider.Message) (provider.Response, error) {
	return s.fn(ctx, m)
}

func accept(context.Context, provider.Message) (provider.Response, error) {
	return provider.Response{Status: 202, MessageID: "m"}, nil
}

type recPublisher struct {
	mu     sync.Mutex
	events []core.Event
}

func (p *recPublisher) Publish(_ context.Context, e core.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
	return nil
}

func (p *recPublisher) statuses(id string) []core.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []core.Status
	for _, e := range p.events {
		if e.NotificationID == id {
			out = append(out, e.Status)
		}
	}
	return out
}

func testConfig() Config {
	return Config{
		Concurrency: 4, MaxAttempts: 3, RetryBase: time.Millisecond, RetryCap: 5 * time.Millisecond,
		Lease: time.Minute, ReapInterval: 10 * time.Millisecond, ShutdownTimeout: time.Second,
		Weights: [3]int{70, 20, 10}, IdleMin: time.Millisecond, IdleMax: 5 * time.Millisecond,
	}
}

// run starts w and returns a stop func that cancels it and returns Run's error.
func run(t *testing.T, w *Worker) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	return func() error { cancel(); return <-errc }
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("not within 5s: %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestWorkerDelivers(t *testing.T) {
	q := newMemQueue()
	q.add(20, core.ChannelSMS, core.PriorityNormal)
	q.add(20, core.ChannelEmail, core.PriorityLow)
	pub := &recPublisher{}
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: &openLimiter{}, Sender: scriptSender{accept}, Publisher: pub}))
	waitFor(t, "all 40 sent", func() bool { return q.count(core.StatusSent) == 40 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := pub.statuses("sms-normal-0"); fmt.Sprint(got) != "[processing sent]" {
		t.Errorf("events: want [processing sent], got %v", got)
	}
}

func TestWorkerRetryOutcomes(t *testing.T) {
	cases := []struct {
		name     string
		answers  []int // status per call; 0 = network error
		status   core.Status
		kind     core.FailureKind
		outcomes string
	}{
		{"transient twice then accepted", []int{503, 0, 202}, core.StatusSent, "", "[transient transient accepted]"},
		{"permanent after one call", []int{400}, core.StatusFailed, core.FailurePermanent, "[permanent]"},
		{"exhausted after max attempts", []int{503, 503, 503}, core.StatusFailed, core.FailureExhausted, "[transient transient transient]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := newMemQueue()
			q.add(1, core.ChannelPush, core.PriorityHigh)
			var mu sync.Mutex
			calls := 0
			send := func(context.Context, provider.Message) (provider.Response, error) {
				mu.Lock()
				defer mu.Unlock()
				st := c.answers[min(calls, len(c.answers)-1)]
				calls++
				if st == 0 {
					return provider.Response{}, errors.New("connection reset")
				}
				return provider.Response{Status: st, Header: http.Header{}}, nil
			}
			stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: &openLimiter{}, Sender: scriptSender{send}}))
			id := "push-high-0"
			waitFor(t, "a final status", func() bool { st := q.row(id).status; return st == core.StatusSent || st == core.StatusFailed })
			if err := stop(); err != nil {
				t.Fatal(err)
			}
			r := q.row(id)
			q.mu.Lock()
			got := fmt.Sprint(q.results[id])
			q.mu.Unlock()
			if r.status != c.status || r.kind != c.kind || got != c.outcomes {
				t.Errorf("got %s/%s %s, want %s/%s %s", r.status, r.kind, got, c.status, c.kind, c.outcomes)
			}
		})
	}
}

func TestWorkerSendsNothingWhenTheLimiterIsDown(t *testing.T) {
	q := newMemQueue()
	q.add(5, core.ChannelSMS, core.PriorityHigh)
	lim := &openLimiter{err: errors.New("valkey down")}
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: lim, Sender: scriptSender{accept}}))
	waitFor(t, "the limiter is asked", func() bool { lim.mu.Lock(); defer lim.mu.Unlock(); return lim.takes > 3 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if n := q.count(core.StatusPending); n != 5 {
		t.Errorf("want all 5 pending (nothing claimed), got %d", n)
	}
}

func TestWorkerShutdownReleasesUnsentWork(t *testing.T) {
	q := newMemQueue()
	q.add(3, core.ChannelEmail, core.PriorityNormal)
	started := make(chan struct{}, 3)
	send := func(ctx context.Context, _ provider.Message) (provider.Response, error) {
		started <- struct{}{}
		<-ctx.Done() // a provider that never answers
		return provider.Response{}, ctx.Err()
	}
	cfg := testConfig()
	cfg.ShutdownTimeout = 20 * time.Millisecond
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: &openLimiter{}, Sender: scriptSender{send}}))
	for range 3 {
		<-started
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	released, results := q.released, len(q.results)
	q.mu.Unlock()
	if released != 3 || results != 0 || q.count(core.StatusPending) != 3 {
		t.Errorf("want 3 released, no attempts, 3 pending; got released %d, attempts on %d, pending %d", released, results, q.count(core.StatusPending))
	}
}

func TestWorkerWeightsAndBorrowing(t *testing.T) {
	t.Run("all priorities busy: shares follow the weights", func(t *testing.T) {
		q := newMemQueue()
		for _, p := range core.Priorities {
			q.add(400, core.ChannelSMS, p)
		}
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: &openLimiter{}, Sender: scriptSender{accept}}))
		waitFor(t, "400 sent", func() bool { return q.count(core.StatusSent) >= 400 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		h, n, l := q.claims[core.PriorityHigh], q.claims[core.PriorityNormal], q.claims[core.PriorityLow]
		q.mu.Unlock()
		total := float64(h + n + l)
		if hs := float64(h) / total; hs < 0.65 || hs > 0.75 {
			t.Errorf("high share %.2f, want about 0.70 (h=%d n=%d l=%d)", hs, h, n, l)
		}
		if ls := float64(l) / total; ls < 0.07 || ls > 0.13 {
			t.Errorf("low share %.2f, want about 0.10 (h=%d n=%d l=%d)", ls, h, n, l)
		}
	})

	t.Run("normal empty: its share is lent down to low", func(t *testing.T) {
		q := newMemQueue()
		q.add(400, core.ChannelSMS, core.PriorityHigh)
		q.add(400, core.ChannelSMS, core.PriorityLow)
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: &openLimiter{}, Sender: scriptSender{accept}}))
		waitFor(t, "300 sent", func() bool { return q.count(core.StatusSent) >= 300 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		h, l := q.claims[core.PriorityHigh], q.claims[core.PriorityLow]
		q.mu.Unlock()
		if ls := float64(l) / float64(h+l); ls < 0.25 || ls > 0.35 {
			t.Errorf("low share %.2f, want about 0.30 (low's 10 plus normal's 20)", ls)
		}
	})
}

func TestSelectorOrder(t *testing.T) {
	s := newSelector([3]int{70, 20, 10})
	counts := map[int]int{}
	for range 100 {
		counts[s.next()]++
	}
	if counts[0] != 70 || counts[1] != 20 || counts[2] != 10 {
		t.Errorf("100 turns: got %v, want exactly 70/20/10", counts)
	}
	now := time.Now()
	s = newSelector([3]int{0, 1, 0}) // always picks normal first
	if got := fmt.Sprint(s.order(now)); got != "[normal low high]" {
		t.Errorf("order: want down then up, got %s", got)
	}
	s.markEmpty(core.PriorityLow, now.Add(time.Second))
	if got := fmt.Sprint(s.order(now)); got != "[normal high]" {
		t.Errorf("order with low empty: got %s", got)
	}
}

func TestBackoff(t *testing.T) {
	top := func(n int64) int64 { return n }
	cases := []struct {
		attempt    int
		retryAfter time.Duration
		want       time.Duration
	}{
		{1, 0, 200 * time.Millisecond},
		{2, 0, 400 * time.Millisecond},
		{4, 0, time.Second}, // capped
		{70, 0, time.Second},
		{1, 2 * time.Second, 2 * time.Second}, // never shorter than Retry-After
	}
	for _, c := range cases {
		if got := Backoff(c.attempt, 100*time.Millisecond, time.Second, c.retryAfter, top); got != c.want {
			t.Errorf("Backoff(%d, retryAfter %s) upper = %s, want %s", c.attempt, c.retryAfter, got, c.want)
		}
	}
	if got := Backoff(3, 100*time.Millisecond, time.Second, 0, func(int64) int64 { return 0 }); got != 0 {
		t.Errorf("full jitter lower bound: got %s", got)
	}
}
