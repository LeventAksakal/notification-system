package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/breaker"
	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

// countingSender counts calls and answers with fn.
type countingSender struct {
	calls atomic.Int64
	fn    func(ctx context.Context, m provider.Message) (provider.Response, error)
}

func (s *countingSender) Send(ctx context.Context, m provider.Message) (provider.Response, error) {
	s.calls.Add(1)
	return s.fn(ctx, m)
}

func TestWorkerLeaseSafety(t *testing.T) {
	t.Run("a job reaching the sender after its lease ended produces no provider request", func(t *testing.T) {
		q := newMemQueue()
		q.add(5, core.ChannelPush, core.PriorityHigh)
		snd := &countingSender{fn: accept}
		// The worker's clock runs two minutes ahead: every one-minute lease
		// has ended by the time its job is handed to the sender, as for a
		// worker paused past its leases.
		later := func() time.Time { return time.Now().Add(2 * time.Minute) }
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: snd, Now: later}))
		waitFor(t, "jobs claimed and released", func() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.released >= 5 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		defer q.mu.Unlock()
		if n := snd.calls.Load(); n != 0 || len(q.results) != 0 {
			t.Errorf("want 0 provider requests and no attempts, got %d requests, attempts on %d jobs", n, len(q.results))
		}
	})

	t.Run("a call that would outlive the lease is cut and recorded as a transient attempt while the lease holds", func(t *testing.T) {
		q := newMemQueue()
		q.add(1, core.ChannelSMS, core.PriorityNormal)
		var mu sync.Mutex
		var deadlines []time.Duration
		snd := &countingSender{fn: func(ctx context.Context, _ provider.Message) (provider.Response, error) {
			dl, _ := ctx.Deadline()
			mu.Lock()
			deadlines = append(deadlines, time.Until(dl))
			mu.Unlock()
			<-ctx.Done() // a provider that never answers
			return provider.Response{}, ctx.Err()
		}}
		cfg := testConfig()
		cfg.Lease = 600 * time.Millisecond
		cfg.MaxAttempts = 1
		stop := run(t, New(cfg, Deps{Queue: q, Limiter: testLimiter(), Sender: snd}))
		id := "sms-normal-0"
		waitFor(t, "a final status", func() bool { return q.row(id).status == core.StatusFailed })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		defer q.mu.Unlock()
		if got := fmt.Sprint(q.results[id]); got != "[transient]" || q.lost != 0 {
			t.Errorf("want one transient attempt recorded and no write fenced off, got %s (lost %d)", got, q.lost)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(deadlines) != 1 || deadlines[0] > cfg.Lease-leaseMargin {
			t.Errorf("call deadlines %v: want one call ending at least %s before the %s lease", deadlines, leaseMargin, cfg.Lease)
		}
	})
}

func TestWorkerMissedSlotIsReleasedNotSentLate(t *testing.T) {
	q := newMemQueue()
	q.add(1, core.ChannelEmail, core.PriorityHigh)
	jobs, _ := q.Claim(t.Context(), core.ChannelEmail, core.PriorityHigh, 1, time.Minute)
	snd := &countingSender{fn: accept}
	w := New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: snd})
	cr := w.newChannelRun(core.ChannelEmail, testPlaces(1, 0))
	tk, _, _ := cr.br.Allow()
	tolerance := 5 * time.Millisecond
	// The job reaches its send step three intervals after its slot (a slow
	// claim, a timer late under load).
	w.sendAt(t.Context(), t.Context(), cr, jobs[0], nil, time.Now().Add(-3*tolerance), tk, tolerance, 0)
	q.mu.Lock()
	defer q.mu.Unlock()
	if snd.calls.Load() != 0 || q.released != 1 || q.rows[jobs[0].ID].status != core.StatusPending {
		t.Errorf("want no request and the job released to pending; got %d requests, %d released, status %s",
			snd.calls.Load(), q.released, q.rows[jobs[0].ID].status)
	}
}

func TestWorkerHungChannelDoesNotStallTheOthers(t *testing.T) {
	q := newMemQueue()
	q.add(500, core.ChannelSMS, core.PriorityNormal)
	q.add(500, core.ChannelEmail, core.PriorityNormal)
	var smsNow, smsMax atomic.Int64
	snd := &countingSender{fn: func(ctx context.Context, m provider.Message) (provider.Response, error) {
		if m.Channel != core.ChannelSMS {
			return accept(ctx, m)
		}
		n := smsNow.Add(1)
		defer smsNow.Add(-1)
		for {
			old := smsMax.Load()
			if n <= old || smsMax.CompareAndSwap(old, n) {
				break
			}
		}
		<-ctx.Done() // the sms provider hangs
		return provider.Response{}, ctx.Err()
	}}
	cfg := testConfig()
	cfg.Channels = []core.Channel{core.ChannelSMS, core.ChannelEmail}
	cfg.Concurrency = 8 // 2 places of its own per channel, 4 shared
	lim := newGridLimiter(10*time.Millisecond, 50*time.Millisecond)
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: snd}))
	waitFor(t, "the sms calls hang", func() bool { return smsNow.Load() == 6 })
	before := q.countSent(core.ChannelEmail)
	time.Sleep(time.Second)
	got := q.countSent(core.ChannelEmail) - before
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	t.Logf("email sent in 1 s while sms hangs: %d (limit 100/s); sms calls in flight at most %d", got, smsMax.Load())
	if got < 80 {
		t.Errorf("email sent %d in 1 s while sms hangs, want at least 80 of its 100", got)
	}
	if smsMax.Load() > 6 {
		t.Errorf("sms held %d calls; its own 2 and the 4 shared are all it may take", smsMax.Load())
	}
}

func TestWorkerPublishesReapedLeases(t *testing.T) {
	q := newMemQueue()
	q.add(1, core.ChannelPush, core.PriorityLow)
	// A claim by a worker that died: its lease ran out a second ago.
	jobs, _ := q.Claim(t.Context(), core.ChannelPush, core.PriorityLow, 1, -time.Second)
	pub := &recPublisher{}
	lim := testLimiter()
	lim.err = errors.New("valkey down") // this worker claims nothing itself
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: lim, Sender: scriptSender{accept}, Publisher: pub}))
	waitFor(t, "the reaped job is published as pending", func() bool {
		return fmt.Sprint(pub.statuses(jobs[0].ID)) == "[pending]"
	})
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	for _, e := range pub.events {
		if e.NotificationID == jobs[0].ID && e.CorrelationID != jobs[0].CorrelationID {
			t.Errorf("reaped event correlation id %q, want %q", e.CorrelationID, jobs[0].CorrelationID)
		}
	}
}

func TestWorkerDoesNotSendWhenTheBreakerOpenedAfterTheClaim(t *testing.T) {
	q := newMemQueue()
	q.add(1, core.ChannelSMS, core.PriorityHigh)
	jobs, _ := q.Claim(t.Context(), core.ChannelSMS, core.PriorityHigh, 1, time.Minute)
	br := breaker.New(breaker.Config{Window: time.Minute, MinRequests: 1, FailureRatio: 0.5, Cooldown: time.Minute}, time.Now)
	snd := &countingSender{fn: accept}
	w := New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: snd, Breakers: map[core.Channel]*breaker.Breaker{core.ChannelSMS: br}})
	cr := w.newChannelRun(core.ChannelSMS, testPlaces(1, 0))
	tk, _, _ := br.Allow() // admitted while closed
	other, _, _ := br.Allow()
	br.Done(other, false) // another call fails: the breaker opens
	w.sendAt(t.Context(), t.Context(), cr, jobs[0], nil, time.Now(), tk, 5*time.Millisecond, 0)
	q.mu.Lock()
	defer q.mu.Unlock()
	if snd.calls.Load() != 0 || q.released != 1 {
		t.Errorf("want no request and the job released; got %d requests, %d released", snd.calls.Load(), q.released)
	}
}

func TestWorkerDoesNotSendASlotPlacedBeforeAClockStep(t *testing.T) {
	q := newMemQueue()
	q.add(1, core.ChannelSMS, core.PriorityHigh)
	jobs, _ := q.Claim(t.Context(), core.ChannelSMS, core.PriorityHigh, 1, time.Minute)
	snd := &countingSender{fn: accept}
	w := New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: snd})
	cr := w.newChannelRun(core.ChannelSMS, testPlaces(1, 0))
	tk, _, _ := cr.br.Allow()
	// The slot is placed under the mapping's generation; then a Take shows
	// the limiter's clock 5 s ahead of where the mapping had it.
	now, remote := time.Now(), time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	_, _, gen, _ := w.clock.observe(now, now.Add(time.Millisecond), remote, 5*time.Millisecond)
	if _, _, _, r := w.clock.observe(now.Add(time.Second), now.Add(time.Second+time.Millisecond), remote.Add(6*time.Second), 5*time.Millisecond); !r.voids {
		t.Fatal("a 5 s step did not void the mapping's generation")
	}
	w.sendAt(t.Context(), t.Context(), cr, jobs[0], nil, time.Now(), tk, 5*time.Millisecond, gen)
	q.mu.Lock()
	defer q.mu.Unlock()
	if snd.calls.Load() != 0 || q.released != 1 {
		t.Errorf("want no request and the job released; got %d requests, %d released", snd.calls.Load(), q.released)
	}
}

func (q *memQueue) countSent(ch core.Channel) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, r := range q.rows {
		if r.status == core.StatusSent && r.job.Channel == ch {
			n++
		}
	}
	return n
}

func testPlaces(own, shared int) callPlaces {
	return callPlaces{own: make(chan struct{}, own), shared: make(chan struct{}, shared)}
}
