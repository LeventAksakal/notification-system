package delivery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

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
