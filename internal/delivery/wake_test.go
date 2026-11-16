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
		{"busy grid: sent at its slot, after the slots ahead of it", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := newMemQueue()
			lim := testLimiter()
			sent := newSentLog()
			cfg := idleConfig()
			cfg.AlignFloor, cfg.AlignCeiling = align, align
			id := q.addDue(notification.ChannelSMS, notification.PriorityHigh, time.Time{})
			start := time.Now()
			if c.busy {
				if _, err := lim.Take(t.Context(), notification.ChannelSMS, notification.SlotRequest{N: 2, Horizon: time.Second}); err != nil {
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
			// The two slots taken ahead start now, an interval apart.
			if ahead := 2 * lim.interval; c.busy && took < ahead {
				t.Fatalf("busy grid: sent after %s, want at least %s (after the two slots ahead): an earlier slot's request may still be leaving", took, ahead)
			}
			if !c.busy && took >= align/2 {
				t.Fatalf("idle grid: sent after %s, want well under the lead (%s)", took, align)
			}
		})
	}
}

// The slots a lone row did not need go back to the limiter (giveBack): a
// second row right behind it leaves about an interval later, not behind the
// first group's unused slots, a lead plus a horizon out.
func TestARowRightAfterALoneSendIsNotHeldBehindUnusedSlots(t *testing.T) {
	const lead = 150 * time.Millisecond
	q := newMemQueue()
	lim := newGridLimiter(5*time.Millisecond, 100*time.Millisecond)
	sent := newSentLog()
	fw := newFakeWakes()
	cfg := idleConfig()
	cfg.Channels = []notification.Channel{notification.ChannelSMS}
	cfg.AlignFloor, cfg.AlignCeiling = lead, lead
	cfg.Horizon = 100 * time.Millisecond
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: sent, Wakes: fw}))
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	first := q.addDue(notification.ChannelSMS, notification.PriorityHigh, time.Time{})
	fw.hint(notification.Wake{Channel: notification.ChannelSMS, Priority: notification.PriorityHigh})
	waitFor(t, "the first row sent", func() bool { _, ok := sent.when(first); return ok })
	second := q.addDue(notification.ChannelSMS, notification.PriorityHigh, time.Time{})
	start := time.Now()
	fw.hint(notification.Wake{Channel: notification.ChannelSMS, Priority: notification.PriorityHigh})
	waitFor(t, "the second row sent", func() bool { _, ok := sent.when(second); return ok })
	at, _ := sent.when(second)
	if took := at.Sub(start); took >= lead {
		t.Fatalf("second row sent %s after its hint; want well under the lead (%s): it waited behind the first group's unused slots", took, lead)
	}
	lim.mu.Lock()
	defer lim.mu.Unlock()
	if lim.gaveBack == 0 {
		t.Fatalf("no slots given back (%d GiveBack calls, none took effect)", lim.givenBack)
	}
}

// The same with three workers, as in the stack: every hint wakes all three,
// the first to reserve gets the flex group and the others reserve right
// after it. Their slots must not keep the near groups' give-backs from
// taking effect, a far group must not claim the second row to send it at
// its far slot, and none of the workers may sit out its pause while the
// second row waits (TestGapDeliveryR7SecondRowAfterALoneSend: 385 ms there
// before these fixes; this test caught a 158 ms case the stack had not).
func TestASecondRowAfterALoneSendIsNotHeldBehindTheOtherWorkersSlots(t *testing.T) {
	const lead = 150 * time.Millisecond
	q := newMemQueue()
	lim := newGridLimiter(5*time.Millisecond, 100*time.Millisecond)
	sent := newSentLog()
	cfg := idleConfig()
	cfg.Channels = []notification.Channel{notification.ChannelSMS}
	cfg.AlignFloor, cfg.AlignCeiling = lead, lead
	cfg.Horizon = 100 * time.Millisecond
	var wakes []*fakeWakes
	for range 3 {
		fw := newFakeWakes()
		wakes = append(wakes, fw)
		stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: sent, Wakes: fw}))
		defer func() {
			if err := stop(); err != nil {
				t.Error(err)
			}
		}()
	}
	hint := func() {
		for _, fw := range wakes {
			fw.hint(notification.Wake{Channel: notification.ChannelSMS, Priority: notification.PriorityHigh})
		}
	}
	waitFor(t, "every worker idle", func() bool { p, _ := q.stats(); return p >= 3 })
	var worst time.Duration
	for range 5 {
		time.Sleep(3 * lead) // every reservation of the last pair has passed
		first := q.addDue(notification.ChannelSMS, notification.PriorityHigh, time.Time{})
		hint()
		waitFor(t, "the first row sent", func() bool { _, ok := sent.when(first); return ok })
		second := q.addDue(notification.ChannelSMS, notification.PriorityHigh, time.Time{})
		start := time.Now()
		hint()
		waitFor(t, "the second row sent", func() bool { _, ok := sent.when(second); return ok })
		at, _ := sent.when(second)
		t.Logf("second row sent %s after its hint", at.Sub(start))
		worst = max(worst, at.Sub(start))
	}
	// Half the lead: sent at once costs a Take and a claim on the fakes
	// (under 5 ms measured, race detector on two cores); every failure mode
	// costs a pause (the 50 ms refill) or a slot a lead or more out.
	if worst >= lead/2 {
		t.Fatalf("a second row was sent %s after its hint; want well under the lead (%s): it waited behind reserved slots or a pause", worst, lead)
	}
}

// wokenTogether puts the moments of a late reply in order with events, not a
// sleep. Workers woken by one hint each reserve on one grid; the reply to the
// Take that finds it idle (the early group's) is held until every other
// worker has heard back from its own Take and made its next call on what it
// heard: a GiveBack or a claim (counted once it returns) or another Take
// (counted as it is made). So the later workers hear back first and act
// first, whatever the scheduler does, and a row one of them claims on a grant
// made before the early group heard back is in its hands before the early
// group can ask. That holds for a deferred claim (group.deferClaim) too: the
// later grant here lies beyond the lead, which channelLoop gives back at
// once, and this test pins that.
//
// A Take a later worker makes after that waits until the hinted row is
// claimed. channelLoop makes it a pause after the first (Grant.Wait +
// refill(Horizon): 100 ms or more with the 200 ms Horizon, about 17.5 ms
// here, 5 ms + 25 ms/2), no longer woken, so it claims at once; the early
// group's claim is under way as soon as its reply is in, so the reply's
// jitter has that pause to fit in. That margin is the product's (channelLoop
// gives a woken grant beyond the lead back and claims on the next one); this
// test does not race it on the scheduler. The 10 ms sleep that stood in for
// the jitter left 7.5 ms of it, which the race detector on a loaded runner
// lost in about one run in eight (the row then left at the later worker's
// slot, about 600 ms out).
type wokenTogether struct {
	ch notification.Channel
	n  int // workers

	takes sync.Mutex // one woken Take at a time
	last  time.Time  // guarded by takes: when the last woken Take returned

	mu      sync.Mutex
	armed   bool
	early   int           // the worker whose Take found the grid idle; -1 before
	heard   map[int]bool  // workers that have heard back from a Take since armed
	acted   map[int]bool  // later workers that have made their next call since
	settled map[int]bool  // workers that have asked NextDue: idle until a hint
	release chan struct{} // closed once every later worker has acted
	claimed chan struct{} // closed once the hinted row is claimed
}

func newWokenTogether(ch notification.Channel, n int) *wokenTogether {
	return &wokenTogether{ch: ch, n: n, early: -1, heard: map[int]bool{}, acted: map[int]bool{},
		settled: map[int]bool{}, release: make(chan struct{}), claimed: make(chan struct{})}
}

// arm starts ordering: every reserving Take on ch from now on is a woken one.
func (o *wokenTogether) arm() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.armed = true
}

// idle reports whether every worker's start-up round is over, so only a hint
// wakes it. With no rows and no catch-up hint (quietWakes) start-up is one
// round: every priority starts due (newSelector), and a claim that finds
// nothing carries its slots on to the next priority, or looks it up, until
// all three are answered; its NextDue comes after its claims and give-back
// and settles them empty until the 30 s safety poll.
func (o *wokenTogether) idle() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.settled) == o.n
}

// earlyWorker is the worker whose Take found the grid idle, -1 if none did.
func (o *wokenTogether) earlyWorker() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.early
}

// act counts worker id's next call after its first reply; o.mu is held.
func (o *wokenTogether) act(id int) {
	if !o.armed || id == o.early || !o.heard[id] || o.acted[id] {
		return
	}
	o.acted[id] = true
	if len(o.acted) == o.n-1 {
		close(o.release)
	}
}

// wokenLimiter is worker id's view of the shared gridLimiter.
type wokenLimiter struct {
	*gridLimiter
	o  *wokenTogether
	id int
}

func (l wokenLimiter) Take(ctx context.Context, ch notification.Channel, r notification.SlotRequest) (notification.Grant, error) {
	o := l.o
	o.mu.Lock()
	ordered := o.armed && ch == o.ch && r.N > 0 // a Take of no slots only reads the clock
	again := ordered && o.heard[l.id]
	if again {
		o.act(l.id)
	}
	o.mu.Unlock()
	if !ordered {
		return l.gridLimiter.Take(ctx, ch, r)
	}
	if again {
		select {
		case <-o.claimed:
		case <-ctx.Done():
			return notification.Grant{}, ctx.Err()
		}
		return l.gridLimiter.Take(ctx, ch, r)
	}
	// One at a time, each on a later limiter now than the one before, as
	// Takes from processes a network apart are (leadLog): woken on one tick
	// of a coarse clock (Windows), the next taker's first free slot would be
	// 600 ms out, 1 us past the lead plus the cap (300 ms + 299.999 ms) it
	// may reserve to, and it would get none instead of the one right after
	// the early group.
	o.takes.Lock()
	for !time.Now().After(o.last) {
	}
	g, err := l.gridLimiter.Take(ctx, ch, r)
	o.last = time.Now()
	o.takes.Unlock()
	o.mu.Lock()
	o.heard[l.id] = true
	early := o.early < 0 && err == nil && g.N > 0 && g.Idle > 0
	if early {
		o.early = l.id
	}
	o.mu.Unlock()
	if early {
		select {
		case <-o.release:
		case <-ctx.Done():
			return notification.Grant{}, ctx.Err()
		}
	}
	return g, err
}

func (l wokenLimiter) GiveBack(ctx context.Context, ch notification.Channel, g notification.Grant, from time.Time) (bool, error) {
	ok, err := l.gridLimiter.GiveBack(ctx, ch, g, from)
	if ch == l.o.ch {
		l.o.mu.Lock()
		l.o.act(l.id)
		l.o.mu.Unlock()
	}
	return ok, err
}

// wokenQueue is worker id's view of the shared memQueue.
type wokenQueue struct {
	*memQueue
	o  *wokenTogether
	id int
}

func (q wokenQueue) Claim(ctx context.Context, ch notification.Channel, p notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	jobs, err := q.memQueue.Claim(ctx, ch, p, n, lease)
	if ch == q.o.ch {
		q.o.mu.Lock()
		if q.o.armed && len(jobs) > 0 {
			select {
			case <-q.o.claimed:
			default:
				close(q.o.claimed)
			}
		}
		q.o.act(q.id)
		q.o.mu.Unlock()
	}
	return jobs, err
}

func (q wokenQueue) NextDue(ctx context.Context, ch notification.Channel) (map[notification.Priority]time.Duration, error) {
	next, err := q.memQueue.NextDue(ctx, ch)
	if ch == q.o.ch {
		q.o.mu.Lock()
		q.o.settled[q.id] = true
		q.o.mu.Unlock()
	}
	return next, err
}

// quietWakes is a fakeWakes without its subscription's catch-up hint
// (onSubscribed). Every priority starts due anyway (newSelector), so the
// start-up round asks what that hint would; the second start-up round the
// hint can add (one landing on a round under way is kept for another) could
// still be running when the test adds its row and hints.
type quietWakes struct{ *fakeWakes }

func (q quietWakes) Run(ctx context.Context, chs []notification.Channel, onHint func(notification.Wake), _ func()) error {
	return q.fakeWakes.Run(ctx, chs, onHint, func() {})
}

func TestWorkersWokenTogetherLeaveTheHintedRowToTheEarlyGroup(t *testing.T) {
	// Three workers share one queue and one limiter, as in the stack. A hint
	// wakes them all; the first to reserve sees an idle grid and may send at
	// once, the others land on a busy grid a horizon further out. The reply
	// to the first comes late, so the later workers hear back first
	// (wokenTogether). The hinted row must still go with the early group.
	const align = 300 * time.Millisecond
	const workers = 3
	q := newMemQueue()
	// As in production, where the first taker's 19 slots of 10.527 ms end
	// 13 us past lead + 200 ms and the next taker still gets one slot right
	// after them: the worker's horizon is its lead (300 ms), the limiter's
	// cap 1 us under it, so the first taker gets 60 slots of 5 ms, not the
	// 61 it asks for, and the next one the 61st, about 600 ms out.
	lim := newGridLimiter(5*time.Millisecond, align-time.Microsecond)
	order := newWokenTogether(notification.ChannelPush, workers)
	cfg := idleConfig()
	cfg.Channels = []notification.Channel{notification.ChannelPush}
	cfg.AlignFloor, cfg.AlignCeiling = align, align
	cfg.Horizon = 25 * time.Millisecond
	var all []*fakeWakes
	var sent []*sentLog // one per worker: which one sent the row
	for i := range workers {
		wk := newFakeWakes()
		all = append(all, wk)
		sent = append(sent, newSentLog())
		stop := run(t, New(cfg, Deps{Queue: wokenQueue{memQueue: q, o: order, id: i},
			Limiter: wokenLimiter{gridLimiter: lim, o: order, id: i}, Sender: sent[i], Wakes: quietWakes{wk}}))
		defer func() {
			if err := stop(); err != nil {
				t.Error(err)
			}
		}()
	}
	waitFor(t, "every worker idle", order.idle)
	// The start-up rounds reserved slots: let the grid fall idle.
	waitFor(t, "grid idle", func() bool {
		lim.mu.Lock()
		defer lim.mu.Unlock()
		return time.Since(lim.next[notification.ChannelPush]) > align+lim.interval
	})
	id := q.addDue(notification.ChannelPush, notification.PriorityNormal, time.Time{})
	order.arm()
	for _, wk := range all {
		wk.hint(notification.Wake{Channel: notification.ChannelPush, Priority: notification.PriorityNormal, Due: time.Now()})
	}
	by := -1
	waitFor(t, "sent", func() bool {
		for i, s := range sent {
			if _, ok := s.when(id); ok {
				by = i
				return true
			}
		}
		return false
	})
	if early := order.earlyWorker(); by != early {
		t.Fatalf("the hinted row was sent by worker %d, the early group was worker %d's: a later group took it on a grant made before the early group heard back", by, early)
	}
}

// leadLog is a gridLimiter that records the lead of every Take that asked for
// slots, and when. Its Takes are one at a time and at least 50 us apart, as
// Takes from processes a network away are: the limiter's now then differs
// between workers woken together by more than the 1 us that decides whether
// the next taker gets a slot right after the first one's group.
type leadLog struct {
	*gridLimiter
	mu    sync.Mutex
	takes sync.Mutex
	at    []time.Time
	leads []time.Duration
}

func (l *leadLog) Take(ctx context.Context, ch notification.Channel, r notification.SlotRequest) (notification.Grant, error) {
	if r.N > 0 {
		l.mu.Lock()
		l.at, l.leads = append(l.at, time.Now()), append(l.leads, r.Lead)
		l.mu.Unlock()
	}
	l.takes.Lock()
	defer l.takes.Unlock()
	g, err := l.gridLimiter.Take(ctx, ch, r)
	for start := time.Now(); time.Since(start) < 50*time.Microsecond; {
	}
	return g, err
}

// leadsSince returns the leads of the Takes made after t.
func (l *leadLog) leadsSince(t time.Time) []time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []time.Duration
	for i, at := range l.at {
		if at.After(t) {
			out = append(out, l.leads[i])
		}
	}
	return out
}

// A deferred claim (group.deferClaim) waits for its slot before it claims;
// that wait is not claim time, so it must not lift the worker's lead. Three
// workers with the lead free to follow their claims (memQueue claims take
// microseconds, so it sits at the floor) are woken together by one hint after
// another; the second worker's one slot right after the first's group is
// deferred, as in production (TestWorkersWokenTogetherLeaveTheHintedRowToTheEarlyGroup).
func TestDeferredClaimsDoNotLiftTheLead(t *testing.T) {
	const floor = 20 * time.Millisecond
	q := newMemQueue()
	// The first taker's five slots of 5 ms end 1 us past its horizon, so
	// the next taker gets the slot right after them and defers its claim.
	lim := &leadLog{gridLimiter: newGridLimiter(5*time.Millisecond, 25*time.Millisecond)}
	sent := newSentLog()
	cfg := idleConfig()
	cfg.Channels = []notification.Channel{notification.ChannelPush}
	cfg.AlignFloor, cfg.AlignCeiling = floor, time.Second
	cfg.Horizon = 25*time.Millisecond - time.Microsecond
	var all []*fakeWakes
	for range 3 {
		wk := newFakeWakes()
		all = append(all, wk)
		stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: sent, Wakes: wk}))
		defer func() {
			if err := stop(); err != nil {
				t.Error(err)
			}
		}()
	}
	waitFor(t, "every worker idle", func() bool { p, _ := q.stats(); return p >= 3 })
	var settled time.Time
	for k := range 12 {
		waitFor(t, "grid idle", func() bool {
			g := lim.gridLimiter // leadLog's own mu guards only its log
			g.mu.Lock()
			defer g.mu.Unlock()
			return time.Since(g.next[notification.ChannelPush]) > 50*time.Millisecond
		})
		if k == 6 {
			settled = time.Now() // every worker has claimed enough to measure
		}
		id := q.addDue(notification.ChannelPush, notification.PriorityNormal, time.Time{})
		for _, wk := range all {
			wk.hint(notification.Wake{Channel: notification.ChannelPush, Priority: notification.PriorityNormal, Due: time.Now()})
		}
		waitFor(t, "sent", func() bool { _, ok := sent.when(id); return ok })
	}
	leads := lim.leadsSince(settled)
	if len(leads) == 0 {
		t.Fatal("no reservation after the first six hints")
	}
	for _, l := range leads {
		if l > floor {
			t.Fatalf("leads after six hints: %v; want every one at the floor (%s): a deferred claim's wait counted as claim time", leads, floor)
		}
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
