package delivery

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

// idleConfig is testConfig with a safety poll long enough that only a hint or
// a due time can explain a prompt send.
func idleConfig() Config {
	cfg := testConfig()
	cfg.SafetyPoll = 30 * time.Second
	return cfg
}

// fakeWakes is a wake.Listener without Valkey: the test calls its callbacks.
type fakeWakes struct {
	mu           sync.Mutex
	onHint       func(notification.Wake)
	onSubscribed func()
	ready        chan struct{}
}

func newFakeWakes() *fakeWakes { return &fakeWakes{ready: make(chan struct{})} }

func (f *fakeWakes) Run(ctx context.Context, _ []notification.Channel, onHint func(notification.Wake), onSubscribed func()) error {
	f.mu.Lock()
	f.onHint, f.onSubscribed = onHint, onSubscribed
	f.mu.Unlock()
	onSubscribed()
	close(f.ready)
	<-ctx.Done()
	return nil
}

func (f *fakeWakes) hint(h notification.Wake) {
	<-f.ready
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onHint(h)
}

// sentLog records when each notification reached the sender.
type sentLog struct {
	mu sync.Mutex
	at map[string]time.Time
}

func newSentLog() *sentLog { return &sentLog{at: map[string]time.Time{}} }

func (l *sentLog) Send(_ context.Context, m provider.Message) (provider.Response, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.at[m.Key]; !ok {
		l.at[m.Key] = time.Now()
	}
	return provider.Response{Status: 202, MessageID: "m"}, nil
}

func (l *sentLog) when(key string) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t, ok := l.at[key]
	return t, ok
}

// addDue adds one pending row of ch and p, due at due, and returns its id.
func (q *memQueue) addDue(ch notification.Channel, p notification.Priority, due time.Time) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	id := fmt.Sprintf("%s-%s-%d", ch, p, len(q.order))
	q.rows[id] = &memRow{job: notification.Job{ID: id, Channel: ch, Priority: p, CorrelationID: "c-" + id}, status: notification.StatusPending, due: due}
	q.order = append(q.order, id)
	return id
}

func (q *memQueue) stats() (peeks, claimCalls int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.peeks, q.claimCalls
}

func TestAnIdleWorkerClaimsAtOnceWhenAHintArrives(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent()) // the listener stops with the worker
	q := newMemQueue()
	wakes := newFakeWakes()
	sent := newSentLog()
	stop := run(t, New(idleConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: sent, Wakes: wakes}))
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	// Every channel has found nothing and gone to sleep for the safety poll.
	waitFor(t, "every channel idle", func() bool { p, _ := q.stats(); return p >= len(notification.Channels) })

	id := q.addDue(notification.ChannelSMS, notification.PriorityNormal, time.Time{})
	start := time.Now()
	wakes.hint(notification.Wake{Channel: notification.ChannelSMS, Priority: notification.PriorityNormal, Due: start})
	waitFor(t, "the hinted notification sent", func() bool { _, ok := sent.when(id); return ok })
	if at, _ := sent.when(id); at.Sub(start) > time.Second {
		t.Fatalf("sent %s after the hint; want well under the 30 s safety poll", at.Sub(start))
	}
}

// raceQueue runs hook once, inside the first NextDue that has an answer
// ready: as if a notification were committed and hinted after the database
// answered, but before the worker acted on the answer.
type raceQueue struct {
	*memQueue
	once sync.Once
	hook func()
}

func (q *raceQueue) NextDue(ctx context.Context, ch notification.Channel) (map[notification.Priority]time.Duration, error) {
	next, err := q.memQueue.NextDue(ctx, ch)
	if ch == notification.ChannelPush {
		q.once.Do(q.hook)
	}
	return next, err
}

func TestAHintThatArrivesDuringARoundIsNotLostToIt(t *testing.T) {
	q := &raceQueue{memQueue: newMemQueue()}
	sent := newSentLog()
	w := New(idleConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: sent})
	var mu sync.Mutex
	var id string
	var hinted time.Time
	q.hook = func() {
		mu.Lock()
		id = q.addDue(notification.ChannelPush, notification.PriorityHigh, time.Time{})
		hinted = time.Now()
		mu.Unlock()
		w.onHint(notification.Wake{Channel: notification.ChannelPush, Priority: notification.PriorityHigh, Due: hinted})
	}
	got := func() (string, time.Time) {
		mu.Lock()
		defer mu.Unlock()
		return id, hinted
	}
	stop := run(t, w)
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	waitFor(t, "the notification hinted during the round sent", func() bool {
		id, _ := got()
		if id == "" {
			return false
		}
		_, ok := sent.when(id)
		return ok
	})
	sentID, hintedAt := got()
	if at, _ := sent.when(sentID); at.Sub(hintedAt) > time.Second {
		t.Fatalf("sent %s after its hint; the round's \"nothing pending\" must not outrank it (safety poll 30 s)", at.Sub(hintedAt))
	}
}

func TestSelectorKeepsAHintReceivedDuringARound(t *testing.T) {
	now := time.Now()
	later := now.Add(time.Second)
	cases := []struct {
		name      string
		hintFirst bool // the hint came before the round began: the round saw its row
		want      time.Time
		wantCheck bool
	}{
		{"a hint during the round wins over its settle", false, now, false},
		{"a hint before the round is the round's to judge", true, later, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newSelector([3]int{1, 1, 1})
			if c.hintFirst {
				s.hint(0, now)
			}
			r := s.beginRound()
			if !c.hintFirst {
				s.hint(0, now)
			}
			s.settle(0, later, true, r)
			if !s.emptyUntil[0].Equal(c.want) || s.check[0] != c.wantCheck {
				t.Fatalf("emptyUntil %s check %v, want %s check %v", s.emptyUntil[0], s.check[0], c.want, c.wantCheck)
			}
		})
	}
}

func TestAnEmptyChannelSleepsUntilItsNextDueTime(t *testing.T) {
	q := newMemQueue()
	sent := newSentLog()
	due := time.Now().Add(300 * time.Millisecond)
	id := q.addDue(notification.ChannelEmail, notification.PriorityLow, due)
	stop := run(t, New(idleConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: sent}))
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	waitFor(t, "the scheduled notification sent", func() bool { _, ok := sent.when(id); return ok })
	at, _ := sent.when(id)
	if at.Before(due) {
		t.Fatalf("sent %s before it was due", due.Sub(at))
	}
	// The worker sleeps until the due time, then claims: not the 30 s safety poll.
	if late := at.Sub(due); late > 150*time.Millisecond {
		t.Fatalf("sent %s after it was due; want a claim at the due time", late)
	}
}

func TestASafetyPollAsksTheDatabaseInsteadOfClaiming(t *testing.T) {
	q := newMemQueue()
	lim := testLimiter()
	cfg := testConfig()
	cfg.SafetyPoll = 20 * time.Millisecond
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: newSentLog()}))
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	// The first round of every channel reserves and claims; after that the
	// channels are idle.
	waitFor(t, "every channel idle", func() bool { p, _ := q.stats(); return p >= len(notification.Channels) })
	peeks0, claims0 := q.stats()
	lim.mu.Lock()
	reserves0 := lim.reserves
	lim.mu.Unlock()
	waitFor(t, "ten more safety polls", func() bool { p, _ := q.stats(); return p >= peeks0+10 })
	_, claims := q.stats()
	lim.mu.Lock()
	reserves := lim.reserves
	lim.mu.Unlock()
	if claims != claims0 || reserves != reserves0 {
		t.Fatalf("idle safety polls claimed %d times and reserved slots %d times; want neither (NextDue only)",
			claims-claims0, reserves-reserves0)
	}
}

func TestAGroupAfterAnIdleGridLeavesAsSoonAsItIsClaimed(t *testing.T) {
	const align = 300 * time.Millisecond
	cases := []struct {
		name string
		busy bool // another worker holds slots just ahead
	}{
		{"idle grid: sent right after the claim", false},
		{"busy grid: sent SendAlign after its slot", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := newMemQueue()
			lim := testLimiter()
			sent := newSentLog()
			cfg := idleConfig()
			cfg.SendAlign = align
			id := q.addDue(notification.ChannelSMS, notification.PriorityHigh, time.Time{})
			start := time.Now()
			if c.busy {
				if _, err := lim.Take(t.Context(), notification.ChannelSMS, 2); err != nil {
					t.Fatal(err)
				}
			}
			stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: sent}))
			defer func() {
				if err := stop(); err != nil {
					t.Error(err)
				}
			}()
			waitFor(t, "sent", func() bool { _, ok := sent.when(id); return ok })
			at, _ := sent.when(id)
			took := at.Sub(start)
			if c.busy && took < align {
				t.Fatalf("busy grid: sent after %s, want at least SendAlign (%s): an earlier slot's request may still be leaving", took, align)
			}
			if !c.busy && took >= align/2 {
				t.Fatalf("idle grid: sent after %s, want well under SendAlign (%s)", took, align)
			}
		})
	}
}

// lateIdleReply is a gridLimiter whose reply to a grant on an idle grid comes
// late (Valkey reply jitter), so a worker that reserved after it can hear
// back, and claim, first.
type lateIdleReply struct {
	*gridLimiter
	delay time.Duration
}

func (l lateIdleReply) Take(ctx context.Context, ch notification.Channel, n int) (notification.Grant, error) {
	g, err := l.gridLimiter.Take(ctx, ch, n)
	if err == nil && g.N > 0 && g.Idle > 0 {
		sleep(ctx, l.delay)
	}
	return g, err
}

func TestWorkersWokenTogetherLeaveTheHintedRowToTheEarlyGroup(t *testing.T) {
	// Three workers share one queue and one limiter, as in the stack. A hint
	// wakes them all; the first to reserve sees an idle grid and may send at
	// once, the others land on a busy grid SendAlign further out. The reply
	// to the first comes late, so a later worker hears back first. The
	// hinted row must still go with the early group.
	const align = 300 * time.Millisecond
	for attempt := range 5 {
		t.Run(fmt.Sprintf("hint %d", attempt), func(t *testing.T) {
			q := newMemQueue()
			// As in production, where the first taker's 19 slots of
			// 10.527 ms end 13 us past the 200 ms horizon and the next taker
			// still gets one slot: the worker asks for five slots of 5 ms
			// (horizon just under 25 ms) and the limiter's horizon admits a
			// sixth right after them.
			const horizon = 25*time.Millisecond - time.Microsecond
			lim := newGridLimiter(5*time.Millisecond, 25*time.Millisecond)
			// Under the 25 ms until that next slot, which the deferred claim
			// waits for.
			late := lateIdleReply{gridLimiter: lim, delay: 10 * time.Millisecond}
			sent := newSentLog()
			cfg := idleConfig()
			cfg.SendAlign = align
			cfg.Horizon = horizon
			var all []*fakeWakes
			var stops []func() error
			for range 3 {
				wk := newFakeWakes()
				all = append(all, wk)
				stops = append(stops, run(t, New(cfg, Deps{Queue: q, Limiter: late, Sender: sent, Wakes: wk})))
			}
			defer func() {
				for _, stop := range stops {
					if err := stop(); err != nil {
						t.Error(err)
					}
				}
			}()
			waitFor(t, "every worker idle", func() bool { p, _ := q.stats(); return p >= 3*len(notification.Channels) })
			// The start-up rounds reserved slots: let the grid fall idle.
			waitFor(t, "grid idle", func() bool {
				lim.mu.Lock()
				defer lim.mu.Unlock()
				return time.Since(lim.next[notification.ChannelPush]) > align+lim.interval
			})
			id := q.addDue(notification.ChannelPush, notification.PriorityNormal, time.Time{})
			start := time.Now()
			for _, wk := range all {
				wk.hint(notification.Wake{Channel: notification.ChannelPush, Priority: notification.PriorityNormal, Due: start})
			}
			waitFor(t, "sent", func() bool { _, ok := sent.when(id); return ok })
			if at, _ := sent.when(id); at.Sub(start) >= align/2 {
				t.Fatalf("sent %s after the hint: a later group took it; want well under SendAlign (%s)", at.Sub(start), align)
			}
		})
	}
}

// recWaker records the hints a worker publishes to the other workers, and when.
type recWaker struct {
	mu    sync.Mutex
	hints []notification.Wake
	at    []time.Time
}

func (r *recWaker) Wake(_ context.Context, hints ...notification.Wake) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, h := range hints {
		r.hints = append(r.hints, h)
		r.at = append(r.at, time.Now())
	}
	return nil
}

// of returns the hints for ch and p and when each was published.
func (r *recWaker) of(ch notification.Channel, p notification.Priority) (hints []notification.Wake, at []time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, h := range r.hints {
		if h.Channel == ch && h.Priority == p {
			hints, at = append(hints, h), append(at, r.at[i])
		}
	}
	return hints, at
}

// The other workers learn of every job this worker puts back to pending, so
// none of them waits for its safety poll to find it.
func TestTheWorkerHintsTheOthersWhenAJobGoesBackToPending(t *testing.T) {
	t.Run("rescheduled after a transient answer: due at its retry", func(t *testing.T) {
		q := newMemQueue()
		q.add(1, notification.ChannelPush, notification.PriorityHigh)
		var mu sync.Mutex
		calls := 0
		send := func(context.Context, provider.Message) (provider.Response, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls == 1 {
				return provider.Response{Status: 503, Header: http.Header{}}, nil
			}
			return provider.Response{Status: 202, MessageID: "m"}, nil
		}
		wk := &recWaker{}
		cfg := testConfig()
		stop := run(t, New(cfg, Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{send}, Waker: wk}))
		waitFor(t, "sent on the retry", func() bool { return q.count(notification.StatusSent) == 1 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		hints, at := wk.of(notification.ChannelPush, notification.PriorityHigh)
		if len(hints) != 1 || hints[0].Due.IsZero() || hints[0].Due.After(at[0].Add(cfg.RetryCap)) {
			t.Fatalf("want one hint for push/high due at the retry (within RetryCap %s of its publishing), got %+v at %v", cfg.RetryCap, hints, at)
		}
	})
	t.Run("released unsent at shutdown: due now", func(t *testing.T) {
		q := newMemQueue()
		q.add(3, notification.ChannelEmail, notification.PriorityNormal)
		started := make(chan struct{}, 3)
		send := func(ctx context.Context, _ provider.Message) (provider.Response, error) {
			started <- struct{}{}
			<-ctx.Done() // a provider that never answers
			return provider.Response{}, ctx.Err()
		}
		wk := &recWaker{}
		cfg := testConfig()
		cfg.ShutdownTimeout = 20 * time.Millisecond
		stop := run(t, New(cfg, Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{send}, Waker: wk}))
		for range 3 {
			<-started
		}
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		hints, at := wk.of(notification.ChannelEmail, notification.PriorityNormal)
		if len(hints) != 3 {
			t.Fatalf("want a hint per released job (3), got %d", len(hints))
		}
		for i, h := range hints {
			if h.Due.After(at[i]) {
				t.Fatalf("hint %d due %s after its publishing: a released job is due now", i, h.Due.Sub(at[i]))
			}
		}
	})
	t.Run("reaped after its lease ran out: every priority of its channel, due now", func(t *testing.T) {
		q := newMemQueue()
		q.add(1, notification.ChannelSMS, notification.PriorityLow)
		// A crashed worker's claim whose lease has already run out.
		if got, err := q.Claim(t.Context(), notification.ChannelSMS, notification.PriorityLow, 1, -time.Second); err != nil || len(got) != 1 {
			t.Fatalf("claim: %v, %d rows", err, len(got))
		}
		wk := &recWaker{}
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}, Waker: wk}))
		waitFor(t, "every priority of sms hinted", func() bool {
			for _, p := range notification.Priorities {
				if hints, _ := wk.of(notification.ChannelSMS, p); len(hints) == 0 {
					return false
				}
			}
			return true
		})
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		for _, p := range notification.Priorities {
			hints, at := wk.of(notification.ChannelSMS, p)
			if hints[0].Due.After(at[0]) {
				t.Fatalf("%s hint due %s after its publishing: a reaped job is due now", p, hints[0].Due.Sub(at[0]))
			}
		}
	})
}
