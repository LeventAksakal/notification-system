package delivery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

// The Docker client of the integration tests (testcontainers on Windows)
// starts one process-wide IO completion goroutine that never ends, and the
// MySQL server shared by the process (test/support) keeps its Ryuk
// connection until the process exits.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/Microsoft/go-winio.ioCompletionProcessor"),
		goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"))
}

// memQueue is an in-memory notification.Queue with the same fencing rules.
type memQueue struct {
	mu       sync.Mutex
	rows     map[string]*memRow
	order    []string
	claims   map[notification.Priority]int
	results  map[string][]notification.AttemptOutcome
	released int
	lost     int // result writes refused because the lease had run out
	peeks    int // NextDue calls
	// claimCalls counts Claim calls, empty ones included; asks counts them
	// per priority.
	claimCalls int
	asks       map[notification.Priority]int
}

type memRow struct {
	job    notification.Job
	status notification.Status
	kind   notification.FailureKind
	due    time.Time
}

func newMemQueue() *memQueue {
	return &memQueue{rows: map[string]*memRow{}, claims: map[notification.Priority]int{}, asks: map[notification.Priority]int{}, results: map[string][]notification.AttemptOutcome{}}
}

func (q *memQueue) add(n int, ch notification.Channel, p notification.Priority) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for range n {
		id := fmt.Sprintf("%s-%s-%d", ch, p, len(q.order))
		q.rows[id] = &memRow{job: notification.Job{ID: id, Channel: ch, Priority: p, CorrelationID: "c-" + id}, status: notification.StatusPending}
		q.order = append(q.order, id)
	}
}

func (q *memQueue) Claim(_ context.Context, ch notification.Channel, p notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.claimCalls++
	q.asks[p]++
	// Due rows of this lane, oldest due first, as the store's claim (ORDER BY
	// due_at); ties in insertion order.
	now := time.Now()
	var due []*memRow
	for _, id := range q.order {
		r := q.rows[id]
		if r.status == notification.StatusPending && r.job.Channel == ch && r.job.Priority == p && !now.Before(r.due) {
			due = append(due, r)
		}
	}
	slices.SortStableFunc(due, func(a, b *memRow) int { return a.due.Compare(b.due) })
	out := make([]notification.Job, 0, min(n, len(due)))
	for _, r := range due[:min(n, len(due))] {
		r.status = notification.StatusProcessing
		r.job.ClaimVersion++
		r.job.LeaseUntil = now.Add(lease)
		out = append(out, r.job)
		q.claims[p]++
	}
	return out, nil
}

func (q *memQueue) finish(job notification.Job, a *notification.Attempt, to notification.Status, kind notification.FailureKind, due time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	r := q.rows[job.ID]
	if r == nil || r.status != notification.StatusProcessing || r.job.ClaimVersion != job.ClaimVersion {
		return notification.ErrLeaseLost
	}
	if time.Now().After(r.job.LeaseUntil) {
		// As if the reaper had taken it back: the claimer's write is fenced off.
		q.lost++
		return notification.ErrLeaseLost
	}
	r.status, r.kind, r.due = to, kind, due
	switch {
	case a == nil:
		q.released++
	case a.Outcome == "":
		// Failed without a call (the budget was spent): counts unchanged.
	default:
		r.job.Attempts++
		if to == notification.StatusFailed || a.Counted && to == notification.StatusPending {
			r.job.BudgetSpent++ // as the store: a failure counts when Counted, a final failure always
		}
		q.results[job.ID] = append(q.results[job.ID], a.Outcome)
	}
	return nil
}

func (q *memQueue) MarkSent(_ context.Context, j notification.Job, a notification.Attempt) error {
	return q.finish(j, &a, notification.StatusSent, "", time.Time{})
}
func (q *memQueue) Reschedule(_ context.Context, j notification.Job, a notification.Attempt, due time.Time) error {
	return q.finish(j, &a, notification.StatusPending, "", due)
}
func (q *memQueue) MarkFailed(_ context.Context, j notification.Job, a notification.Attempt, k notification.FailureKind) error {
	return q.finish(j, &a, notification.StatusFailed, k, time.Time{})
}
func (q *memQueue) Release(_ context.Context, j notification.Job, due time.Time) error {
	return q.finish(j, nil, notification.StatusPending, "", due)
}
func (q *memQueue) ReapExpired(context.Context) ([]notification.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []notification.Job
	for _, id := range q.order {
		r := q.rows[id]
		if r.status == notification.StatusProcessing && time.Now().After(r.job.LeaseUntil) {
			r.status = notification.StatusPending
			out = append(out, notification.Job{ID: r.job.ID, BatchID: r.job.BatchID, Channel: r.job.Channel,
				Attempts: r.job.Attempts, CorrelationID: r.job.CorrelationID})
		}
	}
	return out, nil
}

func (q *memQueue) NextDue(_ context.Context, ch notification.Channel) (map[notification.Priority]time.Duration, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.peeks++
	out := map[notification.Priority]time.Duration{}
	now := time.Now()
	for _, r := range q.rows {
		if r.status != notification.StatusPending || r.job.Channel != ch {
			continue
		}
		w := r.due.Sub(now)
		if cur, ok := out[r.job.Priority]; !ok || w < cur {
			out[r.job.Priority] = w
		}
	}
	return out, nil
}

func (q *memQueue) count(st notification.Status) int {
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

// gridLimiter is an in-memory GCRA with burst 1 per channel, like the Valkey
// limiter: slots exactly interval apart, at most horizon past the lead (the
// smaller of the request's and its own). Its clock is
// a "remote" one, hours away from the local wall clock and running on the
// local monotonic clock: Grant.At is on it, as Valkey's TIME is.
type gridLimiter struct {
	mu                  sync.Mutex
	err                 error
	takes               int
	reserves            int // Takes that asked for slots (n > 0)
	givenBack, gaveBack int // GiveBack calls; those that moved the next free slot back
	interval            time.Duration
	horizon             time.Duration
	next                map[notification.Channel]time.Time
	start               time.Time // local, monotonic
}

// remoteBase is the gridLimiter's clock at its start.
var remoteBase = time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

func newGridLimiter(interval, horizon time.Duration) *gridLimiter {
	return &gridLimiter{interval: interval, horizon: horizon, next: map[notification.Channel]time.Time{}, start: time.Now()}
}

// remote is local moment t on the limiter's clock.
func (l *gridLimiter) remote(t time.Time) time.Time { return remoteBase.Add(t.Sub(l.start)) }

// local is limiter moment at on the local monotonic clock.
func (l *gridLimiter) local(at time.Time) time.Time { return l.start.Add(at.Sub(remoteBase)) }

func (l *gridLimiter) Take(_ context.Context, ch notification.Channel, r notification.SlotRequest) (notification.Grant, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.takes++
	if l.err != nil {
		return notification.Grant{}, l.err
	}
	if r.N > 0 {
		l.reserves++
	}
	now := time.Now()
	k, first, wait, tat, idle := gcraTake(l.next[ch], now, l.interval, r, l.horizon)
	if k > 0 { // as the Valkey script: a Take that reserves nothing writes nothing
		l.next[ch] = tat
	}
	return notification.Grant{N: k, At: l.remote(now), First: first, Interval: l.interval, Wait: wait, Idle: idle}, nil
}

// gcraTake is the Valkey limiter's script on time.Time: from the next free
// slot tat (zero: no key) it reserves up to r.N slots, starting a free grid
// (tat not after now) at r.Lead, and none later than r.Lead plus the smaller
// of r.Horizon and horizon (the fake's own cap; 0 for none) after now. It
// returns the slots reserved, the first one's distance from now, the wait
// before asking again can reserve something, the new next free slot and how
// long the grid had been free (Grant.Idle).
func gcraTake(tat, now time.Time, interval time.Duration, r notification.SlotRequest, horizon time.Duration) (int, time.Duration, time.Duration, time.Time, time.Duration) {
	idle := time.Duration(0) // as the Valkey limiter reports it
	switch {
	case tat.IsZero():
		idle = time.Second // the least a missing key proves (limiter.keyLinger)
	case tat.Before(now):
		idle = now.Sub(tat)
	}
	if !tat.After(now) {
		tat = now.Add(r.Lead)
	}
	h := r.Horizon
	if horizon > 0 {
		h = min(h, horizon)
	}
	last := r.Lead + h
	first := tat.Sub(now)
	k := 0
	if first <= last {
		k = min(int((last-first)/interval)+1, r.N)
	}
	tat = tat.Add(time.Duration(k) * interval)
	var wait time.Duration
	if k < r.N {
		wait = max(tat.Sub(now)-last, 0)
	}
	return k, first, wait, tat, idle
}

// GiveBack moves the channel's next free slot back to from if it is still
// g's end, as the Valkey limiter's script does.
func (l *gridLimiter) GiveBack(_ context.Context, ch notification.Channel, g notification.Grant, from time.Time) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.givenBack++
	if l.err != nil {
		return false, l.err
	}
	end := l.local(g.At.Add(g.First + time.Duration(g.N)*g.Interval))
	if g.N <= 0 || !l.local(from).Before(end) || !l.next[ch].Equal(end) {
		return false, nil
	}
	l.next[ch] = l.local(from)
	l.gaveBack++
	return true, nil
}

func testLimiter() *gridLimiter { return newGridLimiter(5*time.Millisecond, 20*time.Millisecond) }

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
	events []notification.Event
}

func (p *recPublisher) Publish(_ context.Context, e notification.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
	return nil
}

func (p *recPublisher) statuses(id string) []notification.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []notification.Status
	for _, e := range p.events {
		if e.NotificationID == id {
			out = append(out, e.Status)
		}
	}
	return out
}

func testConfig() Config {
	return Config{
		Concurrency: 30, MaxAttempts: 3, RetryBase: time.Millisecond, RetryCap: 5 * time.Millisecond,
		Lease: time.Minute, ReapInterval: 10 * time.Millisecond, ShutdownTimeout: time.Second,
		Weights: [3]int{70, 20, 10}, IdleMin: time.Millisecond, IdleMax: 5 * time.Millisecond,
		AlignFloor: time.Microsecond, AlignCeiling: time.Microsecond,
		Horizon: 20 * time.Millisecond,
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
	q.add(20, notification.ChannelSMS, notification.PriorityNormal)
	q.add(20, notification.ChannelEmail, notification.PriorityLow)
	pub := &recPublisher{}
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}, Publisher: pub}))
	waitFor(t, "all 40 sent", func() bool { return q.count(notification.StatusSent) == 40 })
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
		status   notification.Status
		kind     notification.FailureKind
		outcomes string
	}{
		{"transient twice then accepted", []int{503, 0, 202}, notification.StatusSent, "", "[transient transient accepted]"},
		{"permanent after one call", []int{400}, notification.StatusFailed, notification.FailurePermanent, "[permanent]"},
		{"exhausted after max attempts", []int{503, 503, 503}, notification.StatusFailed, notification.FailureExhausted, "[transient transient transient]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := newMemQueue()
			q.add(1, notification.ChannelPush, notification.PriorityHigh)
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
			stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{send}}))
			id := "push-high-0"
			waitFor(t, "a final status", func() bool {
				st := q.row(id).status
				return st == notification.StatusSent || st == notification.StatusFailed
			})
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
	q.add(5, notification.ChannelSMS, notification.PriorityHigh)
	lim := testLimiter()
	lim.err = errors.New("valkey down")
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: lim, Sender: scriptSender{accept}}))
	waitFor(t, "the limiter is asked", func() bool { lim.mu.Lock(); defer lim.mu.Unlock(); return lim.takes > 3 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if n := q.count(notification.StatusPending); n != 5 {
		t.Errorf("want all 5 pending (nothing claimed), got %d", n)
	}
}

func TestWorkerShutdownReleasesUnsentWork(t *testing.T) {
	q := newMemQueue()
	q.add(3, notification.ChannelEmail, notification.PriorityNormal)
	started := make(chan struct{}, 3)
	send := func(ctx context.Context, _ provider.Message) (provider.Response, error) {
		started <- struct{}{}
		<-ctx.Done() // a provider that never answers
		return provider.Response{}, ctx.Err()
	}
	cfg := testConfig()
	cfg.ShutdownTimeout = 20 * time.Millisecond
	cfg.Horizon = 100 * time.Millisecond
	// Slots 25 ms apart: a send goroutine woken a few ms late on a loaded
	// machine (5.5 ms seen) would miss a 5 ms slot and add a release this
	// test does not count.
	lim := newGridLimiter(25*time.Millisecond, 100*time.Millisecond)
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: scriptSender{send}}))
	for range 3 {
		<-started
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	// The requests had left: each call is an unknown attempt (decision 0005),
	// not counted against the budget (0026), and the notification goes back
	// to pending for a retry.
	q.mu.Lock()
	defer q.mu.Unlock()
	for id, outcomes := range q.results {
		if fmt.Sprint(outcomes) != "[unknown]" {
			t.Errorf("%s: outcomes %v, want [unknown]", id, outcomes)
		}
	}
	pending := 0
	for _, r := range q.rows {
		if r.status == notification.StatusPending && r.job.Attempts == 1 && r.job.BudgetSpent == 0 {
			pending++
		}
	}
	// A retry claimed again before the loop stopped is released unsent,
	// which changes neither its attempts nor its outcomes.
	if len(q.results) != 3 || pending != 3 {
		t.Errorf("want 3 unknown attempts and 3 pending after 1 uncounted attempt; got attempts on %d, pending %d", len(q.results), pending)
	}
}

func TestWorkerFailsANotificationWhoseBudgetUnknownCallsSpent(t *testing.T) {
	q := newMemQueue()
	q.add(1, notification.ChannelPush, notification.PriorityHigh)
	id := "push-high-0"
	budget := testConfig().MaxAttempts
	q.rows[id].job.Attempts, q.rows[id].job.BudgetSpent = budget+2, budget // the reaper counted unknown calls
	var mu sync.Mutex
	calls := 0
	send := func(context.Context, provider.Message) (provider.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return provider.Response{Status: 202, MessageID: "m"}, nil
	}
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{send}}))
	waitFor(t, "a final status", func() bool { return q.row(id).status == notification.StatusFailed })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if r := q.row(id); calls != 0 || r.kind != notification.FailureExhausted || r.job.Attempts != budget+2 || r.job.BudgetSpent != budget {
		t.Errorf("got %d calls, %s after %d attempts (%d counted); want no call, exhausted after %d (%d counted)",
			calls, r.kind, r.job.Attempts, r.job.BudgetSpent, budget+2, budget)
	}
}

func TestWorkerWeightsAndBorrowing(t *testing.T) {
	t.Run("all priorities busy: shares follow the weights", func(t *testing.T) {
		q := newMemQueue()
		for _, p := range notification.Priorities {
			q.add(400, notification.ChannelSMS, p)
		}
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}}))
		waitFor(t, "400 sent", func() bool { return q.count(notification.StatusSent) >= 400 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		h, n, l := q.claims[notification.PriorityHigh], q.claims[notification.PriorityNormal], q.claims[notification.PriorityLow]
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
		q.add(400, notification.ChannelSMS, notification.PriorityHigh)
		q.add(400, notification.ChannelSMS, notification.PriorityLow)
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}}))
		waitFor(t, "300 sent", func() bool { return q.count(notification.StatusSent) >= 300 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		h, l := q.claims[notification.PriorityHigh], q.claims[notification.PriorityLow]
		q.mu.Unlock()
		if ls := float64(l) / float64(h+l); ls < 0.25 || ls > 0.35 {
			t.Errorf("low share %.2f, want about 0.30 (low's 10 plus normal's 20)", ls)
		}
	})

	// A claim that runs short lends its unused slots on, but not to a
	// priority never answered for: each such ask is one more claim query in
	// the same claim, which tripled the claim time while the database was
	// slow (owner item 0011). NextDue answers those priorities instead.
	onlyHigh := [3]int{100, 0, 0}
	t.Run("a short claim does not query priorities never answered for", func(t *testing.T) {
		q := newMemQueue()
		q.add(5, notification.ChannelSMS, notification.PriorityHigh)
		cfg := testConfig()
		cfg.Weights = onlyHigh
		stop := run(t, New(cfg, Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}}))
		waitFor(t, "5 sent", func() bool { return q.count(notification.StatusSent) == 5 })
		waitFor(t, "the priorities without weight looked up", func() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.peeks > 0 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		n, l := q.asks[notification.PriorityNormal], q.asks[notification.PriorityLow]
		q.mu.Unlock()
		if n != 0 || l != 0 {
			t.Errorf("claim queries for normal %d, low %d with nothing due there; want none", n, l)
		}
	})

	t.Run("rows of a priority without weight are still sent", func(t *testing.T) {
		q := newMemQueue()
		q.add(5, notification.ChannelSMS, notification.PriorityHigh)
		q.add(20, notification.ChannelSMS, notification.PriorityNormal)
		cfg := testConfig()
		cfg.Weights = onlyHigh
		stop := run(t, New(cfg, Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}}))
		waitFor(t, "25 sent", func() bool { return q.count(notification.StatusSent) == 25 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSelectorPlan(t *testing.T) {
	now := time.Now()
	s := newSelector([3]int{70, 20, 10})
	if got := s.plan(100, now); got != [3]int{70, 20, 10} {
		t.Errorf("100 slots: got %v, want exactly 70/20/10", got)
	}
	s = newSelector([3]int{70, 20, 10})
	s.settle(1, now.Add(time.Second), true, s.beginRound())
	if got := s.plan(100, now); got != [3]int{70, 0, 30} {
		t.Errorf("normal empty: got %v, want its share lent down to low", got)
	}
	s = newSelector([3]int{70, 20, 10})
	s.settle(2, now.Add(time.Second), true, s.beginRound())
	if got := s.plan(100, now); got != [3]int{70, 30, 0} {
		t.Errorf("low empty: got %v, want its share lent up to normal", got)
	}
	s = newSelector([3]int{70, 20, 0})
	if got := s.plan(100, now); got[2] != 0 {
		t.Errorf("weight 0: low got %d slots of its own, want none", got[2])
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

// gatedPublisher holds every batch call (the processing events) until open is
// called, like a Valkey that does not answer, and records the batch's events
// when its call is over. Single events (the results) go out at once, so a
// result not held back for its processing event would overtake it.
type gatedPublisher struct {
	recPublisher
	gate chan struct{}
	once sync.Once
}

func newGatedPublisher() *gatedPublisher { return &gatedPublisher{gate: make(chan struct{})} }

func (p *gatedPublisher) open() { p.once.Do(func() { close(p.gate) }) }

func (p *gatedPublisher) PublishAll(ctx context.Context, es []notification.Event) error {
	select {
	case <-p.gate:
	case <-ctx.Done():
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, es...)
	return nil
}

// The processing publish does not return until every job has been sent: were
// it on the claim path, nothing would be sent at all, whatever the timing. A
// timer woken late on a busy machine may still give a slot up and release a
// job (correct, and not what this test is about); the job is then claimed and
// sent again, so the test does not count releases. Each result must wait for
// its own processing event: nothing is published while that is held, and once
// it is let through no job has more results than processing events at any
// point of its event sequence. (A released job's pending event and its next
// claim's events wait for different publishes, so their mutual order is not
// asserted.)
func TestWorkerSlowPublishCostsNoSlotAndKeepsEventOrder(t *testing.T) {
	q := newMemQueue()
	q.add(12, notification.ChannelSMS, notification.PriorityHigh)
	pub := newGatedPublisher()
	defer pub.open()
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}, Publisher: pub}))
	waitFor(t, "all 12 sent while their processing publish does not return", func() bool {
		return q.count(notification.StatusSent) == 12
	})
	pub.mu.Lock()
	early := fmt.Sprint(pub.events)
	n := len(pub.events)
	pub.mu.Unlock()
	if n != 0 {
		t.Errorf("published before the processing publish returned: %s, want nothing", early)
	}
	pub.open()
	ids := make([]string, 12)
	for i := range ids {
		ids[i] = fmt.Sprintf("sms-high-%d", i)
	}
	// balance is how many processing events of id have no result yet; -1 when
	// a result came before its processing event.
	balance := func(got []notification.Status) int {
		n := 0
		for _, st := range got {
			if st == notification.StatusProcessing {
				n++
			} else if n--; n < 0 {
				return -1
			}
		}
		return n
	}
	waitFor(t, "every result published", func() bool {
		for _, id := range ids {
			got := pub.statuses(id)
			if !slices.Contains(got, notification.StatusSent) || balance(got) > 0 {
				return false
			}
		}
		return true
	})
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if got := pub.statuses(id); balance(got) != 0 || got[0] != notification.StatusProcessing {
			t.Errorf("%s events: %v, want each result after a processing event of its own", id, got)
		}
	}
}

// slowFirstClaim makes the first claim take delay, longer than the group's
// slots last, so its rows come back with no slot left.
type slowFirstClaim struct {
	*memQueue
	delay time.Duration
	once  sync.Once
}

func (q *slowFirstClaim) Claim(ctx context.Context, ch notification.Channel, p notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	q.once.Do(func() { sleep(ctx, q.delay) })
	return q.memQueue.Claim(ctx, ch, p, n, lease)
}

// Rows a claim brought back too late for its slots are released off the claim
// path: their release event waits for the processing publish, the channel's
// next claim does not, so the rows are claimed again and sent while that
// publish still has not returned.
func TestWorkerReleaseOfAnOutlastedClaimDoesNotWaitForThePublish(t *testing.T) {
	mq := newMemQueue()
	mq.add(12, notification.ChannelSMS, notification.PriorityHigh)
	q := &slowFirstClaim{memQueue: mq, delay: 100 * time.Millisecond} // slots span 20 ms
	pub := newGatedPublisher()
	defer pub.open()
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}, Publisher: pub}))
	waitFor(t, "all 12 sent while the processing publish does not return", func() bool {
		return mq.count(notification.StatusSent) == 12
	})
	mq.mu.Lock()
	released := mq.released
	mq.mu.Unlock()
	if released == 0 {
		t.Error("no row released: the first claim did not outlast its slots, the test proves nothing")
	}
	pub.open()
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}
