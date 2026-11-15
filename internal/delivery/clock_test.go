package delivery

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

func TestClockMapBounds(t *testing.T) {
	epoch := time.Now()
	remote0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	us := func(n int) time.Duration { return time.Duration(n) * time.Microsecond }
	// A sample is a Take: local us since epoch before and after, remote us
	// since remote0 at the reservation. Offsets are reported relative to
	// local epoch minus remote0, in ns. A sample of age a is widened by
	// a*200e-6 on each side (20 us per 100 ms).
	type sample struct{ sent, got, at int }
	cases := []struct {
		name         string
		samples      []sample
		lower, upper time.Duration
	}{
		{"one sample bounds the offset by its round trip",
			[]sample{{100_000, 110_000, 105_000}}, -5_000_000, 5_000_000},
		{"a faster round trip tightens the bounds",
			[]sample{{100_000, 110_000, 105_000}, {200_000, 202_000, 201_000}}, -1_000_000, 1_000_000},
		{"a slow round trip does not loosen them beyond the drift",
			[]sample{{200_000, 202_000, 201_000}, {300_000, 340_000, 301_000}}, -1_000_000, 1_027_600},
		{"bounds from different samples combine",
			[]sample{{100_000, 120_000, 119_000}, {200_000, 220_000, 201_000}}, -1_000_000, 1_020_000},
		{"samples older than the window expire",
			[]sample{{100_000, 102_000, 101_000}, {20_100_000, 20_140_000, 20_120_000}}, -20_000_000, 20_000_000},
		{"crossed bounds restart from the newest sample",
			[]sample{{100_000, 102_000, 101_000}, {300_000, 302_000, 250_000}}, 50_000_000, 52_000_000},
		// The offset was 1 ms at the first sample and is 1.5 ms 5 s later
		// (100 ppm): without widening the upper bound would stay at 1 ms and
		// place slots 0.5 ms early.
		{"an old sample widens by its age, so drift within the allowance is never early",
			[]sample{{0, 2_000, 1_000}, {5_000_000, 5_001_000, 4_999_200}}, 800_000, 1_800_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newClockMap(epoch, 10*time.Second, 1200*time.Millisecond)
			base := -time.Duration(remote0.UnixNano())
			var lower, upper time.Duration
			for _, s := range tc.samples {
				lower, upper, _, _ = c.observe(epoch.Add(us(s.sent)), epoch.Add(us(s.got)), remote0.Add(us(s.at)), 0)
			}
			if lower-base != tc.lower || upper-base != tc.upper {
				t.Fatalf("bounds [%s, %s], want [%s, %s]", lower-base, upper-base, tc.lower, tc.upper)
			}
			// A remote moment maps to local with the given offset.
			if got := c.toLocal(remote0.Add(us(7)), upper); got.Sub(epoch) != us(7)+tc.upper {
				t.Fatalf("toLocal: %s after epoch, want %s", got.Sub(epoch), us(7)+tc.upper)
			}
		})
	}
}

func TestClockMapGenerationMovesOnlyOnARestart(t *testing.T) {
	epoch := time.Now()
	remote0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	c := newClockMap(epoch, 10*time.Second, 1200*time.Millisecond)
	_, _, g0, _ := c.observe(epoch.Add(ms(100)), epoch.Add(ms(102)), remote0.Add(ms(101)), 10*time.Millisecond)
	_, _, g1, r1 := c.observe(epoch.Add(ms(200)), epoch.Add(ms(230)), remote0.Add(ms(215)), 10*time.Millisecond) // agrees, looser
	if g1 != g0 || r1.happened || !c.current(g0) {
		t.Fatalf("an agreeing sample moved the generation: %d -> %d (restarted %v)", g0, g1, r1)
	}
	_, _, g2, r2 := c.observe(epoch.Add(ms(300)), epoch.Add(ms(302)), remote0.Add(ms(5_301)), 10*time.Millisecond) // 5 s step
	if g2 == g0 || !r2.voids || c.current(g0) || !c.current(g2) {
		t.Fatalf("a 5 s step: generation %d -> %d (restarted %v); the old one must be void", g0, g2, r2)
	}
}

func TestClockMapVoidsOnlyForAForwardStepOfAnInterval(t *testing.T) {
	epoch := time.Now()
	remote0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	const interval = 10 * time.Millisecond
	// A step moves Valkey's clock by d at local moment after; every sample
	// is a 100 us round trip, one every 100 ms from 100 ms on.
	type step struct {
		after time.Duration
		d     time.Duration
	}
	cases := []struct {
		name     string
		interval time.Duration // passed with every sample
		steps    []step
		voids    []bool // per step, whether its restart voided
	}{
		{"a forward step under an interval restarts without voiding",
			interval, []step{{ms(1_000), ms(1)}}, []bool{false}},
		{"a forward step of just over an interval voids",
			interval, []step{{ms(1_000), interval + ms(1)}}, []bool{true}},
		{"a backward step voids nothing however large",
			interval, []step{{ms(1_000), -ms(500)}}, []bool{false}},
		{"small forward steps inside one second void once they add up to an interval",
			interval, []step{{ms(1_000), ms(4)}, {ms(1_300), ms(4)}, {ms(1_600), ms(4)}, {ms(1_900), ms(4)}}, []bool{false, false, true, false}},
		{"small forward steps further apart than the span never add up",
			interval, []step{{ms(1_000), ms(4)}, {ms(2_500), ms(4)}, {ms(4_000), ms(4)}, {ms(5_500), ms(4)}}, []bool{false, false, false, false}},
		{"with no interval known any forward step voids",
			0, []step{{ms(1_000), ms(1)}}, []bool{true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newClockMap(epoch, 10*time.Second, 1200*time.Millisecond)
			var shift time.Duration
			var got []bool
			var prev uint64
			k := 0
			for at := ms(100); at <= ms(7_000); at += ms(100) {
				if k < len(tc.steps) && at >= tc.steps[k].after {
					shift += tc.steps[k].d
					k++
				}
				_, _, gen, r := c.observe(epoch.Add(at), epoch.Add(at+100*time.Microsecond), remote0.Add(at+50*time.Microsecond+shift), tc.interval)
				if r.voids != (gen != prev) || r.voids != !c.current(prev) {
					t.Fatalf("restart voids %v, generation %d -> %d", r.voids, prev, gen)
				}
				prev = gen
				if r.happened {
					got = append(got, r.voids)
					if (tc.steps[k-1].d > 0) != (r.step > 0) || (r.step > 0 && r.step < tc.steps[k-1].d) {
						t.Fatalf("step estimate %s for a step of %s: it must bound the step from above", r.step, tc.steps[k-1].d)
					}
				}
			}
			if !slices.Equal(got, tc.voids) {
				t.Fatalf("restarts voided %v, want %v", got, tc.voids)
			}
		})
	}
}

func TestClockMapUncertainty(t *testing.T) {
	epoch := time.Now()
	remote0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	c := newClockMap(epoch, 10*time.Second, 1200*time.Millisecond)
	if u := c.uncertainty(epoch); u != unknown {
		t.Fatalf("no sample: uncertainty %s, want unknown", u)
	}
	c.observe(epoch.Add(ms(100)), epoch.Add(ms(102)), remote0.Add(ms(101)), 10*time.Millisecond)
	if u := c.uncertainty(epoch.Add(ms(102))); u != ms(2) {
		t.Fatalf("one fresh sample: uncertainty %s, want its round trip 2ms", u)
	}
	if u := c.uncertainty(epoch.Add(ms(5_102))); u != ms(4) {
		t.Fatalf("5 s later: uncertainty %s, want 2ms + 2 x 1ms of drift", u)
	}
	if u := c.uncertainty(epoch.Add(ms(10_200))); u != unknown {
		t.Fatalf("past the window: uncertainty %s, want unknown", u)
	}
}

// delayedLimiter is a gridLimiter whose replies arrive late: the grant is
// made (and At stamped) at once, then held for the next of delays. It
// records every slot it handed out on the local monotonic clock.
type delayedLimiter struct {
	grid   *gridLimiter
	delays []time.Duration
	// reportIdle passes the grid's Grant.Idle on; false reports a busy grid.
	reportIdle bool

	mu    sync.Mutex
	k     int
	slots []time.Time
}

func (l *delayedLimiter) Take(ctx context.Context, ch notification.Channel, n int) (notification.Grant, error) {
	g, err := l.grid.Take(ctx, ch, n)
	if err != nil {
		return g, err
	}
	l.mu.Lock()
	for i := range g.N {
		l.slots = append(l.slots, l.grid.local(g.At.Add(g.First+time.Duration(i)*g.Interval)))
	}
	d := l.delays[l.k%len(l.delays)]
	l.k++
	l.mu.Unlock()
	sleep(ctx, d)
	if err := ctx.Err(); err != nil {
		return notification.Grant{}, err // as the real limiter: a cut call has no reply
	}
	if !l.reportIdle {
		// A grid that is never idle keeps every group on the fixed SendAlign
		// these tests measure against; groups sent early after an idle grid
		// are tested on their own (TestAGroupSentEarlyAfterIdleKeepsSendsOneIntervalApart).
		g.Idle = 0
	}
	return g, nil
}

func (l *delayedLimiter) handedOut() []time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.slots)
}

// timedSender records when each request left.
type timedSender struct {
	mu  sync.Mutex
	at  []time.Time
	ids []string
}

func (s *timedSender) Send(_ context.Context, m provider.Message) (provider.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.at = append(s.at, time.Now())
	s.ids = append(s.ids, m.Key)
	return provider.Response{Status: 202, MessageID: "m"}, nil
}

func (s *timedSender) times() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.at)
}

// R8 under a late limiter reply: every request leaves at or after its slot
// (+ SendAlign) and at most one interval after it, however late the reply
// carrying the slot arrived.
func TestWorkerSendsWithinOneIntervalOfItsSlotWhateverTheReplyDelay(t *testing.T) {
	const interval, delay = 20 * time.Millisecond, 80 * time.Millisecond
	// The timer, the scheduler and the sender's lock between the send-time
	// check and the recorded moment; a loaded machine meets it.
	const margin = 10 * time.Millisecond
	q := newMemQueue()
	q.add(40, notification.ChannelSMS, notification.PriorityNormal)
	lim := &delayedLimiter{grid: newGridLimiter(interval, 100*time.Millisecond), delays: []time.Duration{0, delay}}
	snd := &timedSender{}
	cfg := testConfig()
	cfg.Channels = []notification.Channel{notification.ChannelSMS}
	cfg.Horizon = 100 * time.Millisecond
	cfg.SendAlign = 150 * time.Millisecond // above the delay: late groups lose no slot
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: snd}))
	waitFor(t, "all 40 sent", func() bool { return q.count(notification.StatusSent) == 40 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	sends, slots := snd.times(), lim.handedOut()
	slices.SortFunc(sends, func(a, b time.Time) int { return a.Compare(b) })
	slices.SortFunc(slots, func(a, b time.Time) int { return a.Compare(b) })
	// Match each send to the earliest unused slot whose window [slot+align,
	// slot+align+interval+margin] holds it; slots whose window passed unused
	// were given up. Equal windows make the greedy match exact.
	j := 0
	var worst time.Duration
	for n, s := range sends {
		for j < len(slots) && s.After(slots[j].Add(cfg.SendAlign+interval+margin)) {
			j++
		}
		if j == len(slots) || s.Before(slots[j].Add(cfg.SendAlign)) {
			t.Fatalf("send %d at %s has no slot: it left early or later than one interval (+%s) after every free slot",
				n, s.Sub(sends[0]), margin)
		}
		worst = max(worst, s.Sub(slots[j].Add(cfg.SendAlign)))
		j++
	}
	t.Logf("%d sends on %d slots; latest send %s after its slot + SendAlign (interval %s)", len(sends), len(slots), worst, interval)
}

// R8 with a group sent early after an idle grid (group.flex): no request
// leaves before its slot, and the requests are no denser than the grid allows,
// the early group and the fixed-alignment groups of the burst right behind it
// alike, whether the limiter's reply is prompt or late. The grid's guarantee
// is that each request leaves within one interval of its own moment, and the
// moments are an interval apart: so a request leaves more than one interval
// after the request two before it. Two neighbours may be close (a request
// late within its tolerance, then the next one on time), on a busy grid too.
func TestAGroupSentEarlyAfterIdleKeepsSendsOneIntervalApart(t *testing.T) {
	const interval = 20 * time.Millisecond
	const margin = 5 * time.Millisecond // timer and scheduler jitter between two recorded sends
	for _, delay := range []time.Duration{0, 30 * time.Millisecond} {
		t.Run(fmt.Sprintf("every other limiter reply %s late", delay), func(t *testing.T) {
			q := newMemQueue()
			q.add(3, notification.ChannelSMS, notification.PriorityNormal) // fewer than a group's 6 slots
			lim := &delayedLimiter{grid: newGridLimiter(interval, 100*time.Millisecond), delays: []time.Duration{0, delay}, reportIdle: true}
			snd := &timedSender{}
			fw := newFakeWakes()
			cfg := idleConfig()
			cfg.Channels = []notification.Channel{notification.ChannelSMS}
			cfg.Horizon = 100 * time.Millisecond
			cfg.SendAlign = 150 * time.Millisecond // above the delay: late groups lose no slot
			start := time.Now()
			stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: snd, Wakes: fw}))
			waitFor(t, "the first send", func() bool { return len(snd.times()) > 0 })
			q.add(27, notification.ChannelSMS, notification.PriorityNormal) // a burst right behind the early group
			fw.hint(notification.Wake{Channel: notification.ChannelSMS, Priority: notification.PriorityNormal})
			waitFor(t, "all 30 sent", func() bool { return q.count(notification.StatusSent) == 30 })
			if err := stop(); err != nil {
				t.Fatal(err)
			}
			sends, slots := snd.times(), lim.handedOut()
			slices.SortFunc(sends, func(a, b time.Time) int { return a.Compare(b) })
			slices.SortFunc(slots, func(a, b time.Time) int { return a.Compare(b) })
			if first := sends[0].Sub(start); first >= cfg.SendAlign {
				t.Fatalf("first send after %s: the idle grid's group should leave before SendAlign (%s)", first, cfg.SendAlign)
			}
			for i := 2; i < len(sends); i++ {
				if gap := sends[i].Sub(sends[i-2]); gap < interval-margin {
					t.Fatalf("sends %d and %d are %s apart, want more than one interval (%s, less %s jitter) between a send and the one two before it", i-2, i, gap, interval, margin)
				}
			}
			// Each send is matched to a distinct slot at or before it.
			j := 0
			for n, s := range sends {
				if j == len(slots) || s.Before(slots[j]) {
					t.Fatalf("send %d at %s: before its slot", n, s.Sub(start))
				}
				j++
			}
		})
	}
}

// R9 after an idle grid: the first group of a backlog finds a row for every
// slot, so it keeps its moments and the grid stays in use from it to the next
// group. Sent early, it would leave a hole of up to SendAlign before the next
// group, which keeps SendAlign (measured on the stack: low alone fell to 91.6
// requests per second against R9's floor of 93).
func TestABacklogAfterAnIdleGridLeavesNoSlotUnused(t *testing.T) {
	const interval = 20 * time.Millisecond
	// A slot missed under load leaves a gap of an interval or two; the hole
	// this test is about is SendAlign long.
	const hole = 150 * time.Millisecond / 2
	q := newMemQueue()
	q.add(30, notification.ChannelSMS, notification.PriorityLow)
	lim := &delayedLimiter{grid: newGridLimiter(interval, 100*time.Millisecond), delays: []time.Duration{0}, reportIdle: true}
	snd := &timedSender{}
	cfg := testConfig()
	cfg.Channels = []notification.Channel{notification.ChannelSMS}
	cfg.Horizon = 100 * time.Millisecond
	cfg.SendAlign = 150 * time.Millisecond
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: snd}))
	waitFor(t, "all 30 sent", func() bool { return q.count(notification.StatusSent) == 30 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	sends := snd.times()
	slices.SortFunc(sends, func(a, b time.Time) int { return a.Compare(b) })
	for i := 1; i < len(sends); i++ {
		if gap := sends[i].Sub(sends[i-1]); gap >= hole {
			t.Fatalf("sends %d and %d are %s apart (interval %s): a hole of half SendAlign or more, the slots between them went unused", i-1, i, gap, interval)
		}
	}
}

// collect reads every metric of a manual reader.
func collect(t *testing.T, r sdkmetric.Reader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

// counted sums the data points of counter name whose attributes include want.
func counted(rm metricdata.ResourceMetrics, name string, want ...attribute.KeyValue) int64 {
	var n int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if hasAll(dp.Attributes, want) {
					n += dp.Value
				}
			}
		}
	}
	return n
}

func observed(rm metricdata.ResourceMetrics, name string, want ...attribute.KeyValue) uint64 {
	var n uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			h, ok := m.Data.(metricdata.Histogram[float64])
			if m.Name != name || !ok {
				continue
			}
			for _, dp := range h.DataPoints {
				if hasAll(dp.Attributes, want) {
					n += dp.Count
				}
			}
		}
	}
	return n
}

func hasAll(set attribute.Set, want []attribute.KeyValue) bool {
	for _, kv := range want {
		if v, ok := set.Value(kv.Key); !ok || v != kv.Value {
			return false
		}
	}
	return true
}

func TestWorkerDeliveryMetrics(t *testing.T) {
	sms := attribute.String("channel", "sms")
	t.Run("a delivered job counts one attempt and one call duration", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		defer func() { _ = mp.Shutdown(context.Background()) }()
		q := newMemQueue()
		q.add(1, notification.ChannelSMS, notification.PriorityHigh)
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}, Meter: mp}))
		waitFor(t, "the job sent", func() bool { return q.count(notification.StatusSent) == 1 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		rm := collect(t, reader)
		if n := counted(rm, "notifier.delivery.attempts", sms, attribute.String("outcome", "accepted")); n != 1 {
			t.Errorf("attempts{channel=sms,outcome=accepted} = %d, want 1", n)
		}
		if n := observed(rm, "notifier.provider.call.duration", sms); n != 1 {
			t.Errorf("provider call durations recorded for sms = %d, want 1", n)
		}
	})
	t.Run("a slot placed before a limiter clock step is given up", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		defer func() { _ = mp.Shutdown(context.Background()) }()
		q := newMemQueue()
		q.add(1, notification.ChannelSMS, notification.PriorityHigh)
		jobs, _ := q.Claim(t.Context(), notification.ChannelSMS, notification.PriorityHigh, 1, time.Minute)
		snd := &countingSender{fn: accept}
		w := New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: snd, Meter: mp})
		cr := w.newChannelRun(notification.ChannelSMS, testPlaces(1, 0))
		tk, _, _ := cr.br.Allow()
		now, remote := time.Now(), time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		_, _, gen, _ := w.clock.observe(now, now.Add(time.Millisecond), remote, 5*time.Millisecond)
		w.clock.observe(now.Add(time.Second), now.Add(time.Second+time.Millisecond), remote.Add(6*time.Second), 5*time.Millisecond) // a 5 s step
		w.sendAt(t.Context(), t.Context(), cr, jobs[0], nil, time.Now(), tk, 5*time.Millisecond, gen)
		if n := counted(collect(t, reader), "notifier.slots.given_up", sms, attribute.String("reason", "clock_stepped")); n != 1 || snd.calls.Load() != 0 {
			t.Fatalf("given_up{reason=clock_stepped} = %d and %d calls, want 1 and 0", n, snd.calls.Load())
		}
	})
	t.Run("a lease too short to send gives the slot up", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		defer func() { _ = mp.Shutdown(context.Background()) }()
		q := newMemQueue()
		q.add(1, notification.ChannelSMS, notification.PriorityHigh)
		later := func() time.Time { return time.Now().Add(2 * time.Hour) } // every lease looks over
		snd := &countingSender{fn: accept}
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: snd, Now: later, Meter: mp}))
		waitFor(t, "a slot given up", func() bool {
			return counted(collect(t, reader), "notifier.slots.given_up", sms, attribute.String("reason", "lease_too_short")) >= 1
		})
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if n := snd.calls.Load(); n != 0 {
			t.Fatalf("%d provider calls, want 0", n)
		}
	})
}

// While the limiter's clock is not known to within one interval the worker
// reads it with Takes of no slots: it sends nothing and reserves nothing, so
// the shared rate stays with the workers that can use it.
func TestWorkerWithAnUncertainClockReservesNoSlot(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()
	q := newMemQueue()
	q.add(5, notification.ChannelSMS, notification.PriorityHigh)
	// Every reply takes longer than an interval.
	grid := newGridLimiter(10*time.Millisecond, 50*time.Millisecond)
	lim := &delayedLimiter{grid: grid, delays: []time.Duration{25 * time.Millisecond}}
	snd := &countingSender{fn: accept}
	cfg := testConfig()
	cfg.Channels = []notification.Channel{notification.ChannelSMS}
	cfg.IdleMin, cfg.IdleMax = time.Millisecond, 20*time.Millisecond
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: snd, Meter: mp}))
	waitFor(t, "five clock readings", func() bool {
		grid.mu.Lock()
		defer grid.mu.Unlock()
		return grid.takes >= 5
	})
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if n := snd.calls.Load(); n != 0 {
		t.Errorf("%d provider calls with the limiter's clock unknown to within an interval, want 0", n)
	}
	if n := len(lim.handedOut()); n != 0 {
		t.Errorf("%d slots reserved while the clock was uncertain, want 0", n)
	}
	if n := counted(collect(t, reader), "notifier.slots.given_up", attribute.String("channel", "sms")); n != 0 {
		t.Errorf("%d slots counted as given up, want 0: none was reserved", n)
	}
}

// A cold start reads the clock before it reserves: no group is spent on an
// unknown offset.
func TestWorkerReadsTheClockBeforeItsFirstReservation(t *testing.T) {
	grid := newGridLimiter(5*time.Millisecond, 20*time.Millisecond)
	rec := &recordingLimiter{inner: grid}
	q := newMemQueue()
	q.add(3, notification.ChannelSMS, notification.PriorityHigh)
	cfg := testConfig()
	cfg.Channels = []notification.Channel{notification.ChannelSMS}
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: rec, Sender: scriptSender{accept}}))
	waitFor(t, "all 3 sent", func() bool { return q.count(notification.StatusSent) == 3 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if ns := rec.asked(); len(ns) < 2 || ns[0] != 0 {
		t.Fatalf("Takes asked for %v, want a reading of 0 slots first", ns)
	}
}

// recordingLimiter remembers how many slots each Take asked for.
type recordingLimiter struct {
	inner notification.Limiter
	mu    sync.Mutex
	ns    []int
}

func (l *recordingLimiter) Take(ctx context.Context, ch notification.Channel, n int) (notification.Grant, error) {
	l.mu.Lock()
	l.ns = append(l.ns, n)
	l.mu.Unlock()
	return l.inner.Take(ctx, ch, n)
}

func (l *recordingLimiter) asked() []int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.ns)
}

// steppingLimiter is the Valkey limiter over a clock the test controls: GCRA
// with burst 1 (consecutive slots one interval apart, at most horizon ahead;
// a schedule found stale restarts at now). Its clock runs on the local
// monotonic clock plus an offset that step moves, as Valkey's TIME does when
// the VM it runs in catches its clock up after a stall.
type steppingLimiter struct {
	interval, horizon time.Duration
	start             time.Time // local, monotonic

	mu     sync.Mutex
	offset time.Duration
	tat    map[notification.Channel]time.Time // remote
}

func newSteppingLimiter(interval, horizon time.Duration) *steppingLimiter {
	return &steppingLimiter{interval: interval, horizon: horizon, start: time.Now(), tat: map[notification.Channel]time.Time{}}
}

func (l *steppingLimiter) step(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.offset += d
}

func (l *steppingLimiter) Take(_ context.Context, ch notification.Channel, n int) (notification.Grant, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := remoteBase.Add(time.Since(l.start) + l.offset)
	tat := l.tat[ch]
	if tat.Before(now) {
		tat = now // stale: the schedule restarts at now
	}
	first := tat.Sub(now)
	k := 0
	if first <= l.horizon {
		k = min(int((l.horizon-first)/l.interval)+1, n)
	}
	tat = tat.Add(time.Duration(k) * l.interval)
	l.tat[ch] = tat
	var wait time.Duration
	if k < n {
		wait = max(tat.Sub(now)-l.horizon, 0)
	}
	return notification.Grant{N: k, At: now, First: first, Interval: l.interval, Wait: wait}, nil
}

// busiestFrom is the most sends in any sliding second (sends sorted) and the
// send that window starts at.
func busiestFrom(sends []time.Time) (int, time.Time) {
	most, j := 0, 0
	var at time.Time
	for i := range sends {
		for j < len(sends) && sends[j].Sub(sends[i]) < time.Second {
			j++
		}
		if j-i > most {
			most, at = j-i, sends[i]
		}
	}
	return most, at
}

// R8 across a step of the limiter's clock: the limiter restarts a channel's
// schedule at its new "now" while slots of the old one are still pending in
// the workers. The first Take after the step, on any channel of a worker,
// restarts that worker's clock mapping and so voids every slot it placed
// before, on all its channels; without that, a channel whose own next sample
// agrees with the restarted mapping would send its old slots beside the new
// schedule's.
func TestWorkersHoldTheRateAcrossALimiterClockStep(t *testing.T) {
	const interval, perChannel = 10 * time.Millisecond, 300
	paced := int(time.Second / interval)
	chans := []notification.Channel{notification.ChannelSMS, notification.ChannelEmail, notification.ChannelPush}
	q := newMemQueue()
	for _, ch := range chans {
		q.add(perChannel, ch, notification.PriorityNormal)
	}
	lim := newSteppingLimiter(interval, 100*time.Millisecond)
	snd := &channelSender{at: map[notification.Channel][]time.Time{}}
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()
	cfg := testConfig()
	cfg.Channels = chans
	cfg.Horizon = 100 * time.Millisecond
	cfg.SendAlign = 100 * time.Millisecond
	var stops []func() error
	for range 2 {
		stops = append(stops, run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: snd, Meter: mp})))
	}
	waitFor(t, "a steady stream before the step", func() bool { return snd.count() >= 300 })
	stepAt := time.Now()
	lim.step(5 * time.Second)
	deadline := time.Now().Add(20 * time.Second)
	for q.count(notification.StatusSent) < perChannel*len(chans) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	for _, stop := range stops {
		if err := stop(); err != nil {
			t.Fatal(err)
		}
	}
	stepped := counted(collect(t, reader), "notifier.slots.given_up", attribute.String("reason", "clock_stepped"))
	t.Logf("%d slots given up as clock_stepped", stepped)
	for _, ch := range chans {
		sends := snd.times(ch)
		slices.SortFunc(sends, func(a, b time.Time) int { return a.Compare(b) })
		after := 0
		for _, s := range sends {
			if s.After(stepAt.Add(time.Second)) {
				after++
			}
		}
		most, at := busiestFrom(sends)
		t.Logf("%s: %d sends (%d more than 1 s after the step), busiest sliding second %d (paced %d) from %s after the step; per 100 ms from 0.5 s before: %v",
			ch, len(sends), after, most, paced, at.Sub(stepAt).Round(time.Millisecond), per100(sends, stepAt.Add(-500*time.Millisecond), 15))
		// A send may leave up to one interval after its slot, so a sliding
		// second can hold one more than the paced rate.
		if most > paced+1 {
			t.Errorf("%s: busiest sliding second %d sends, want at most %d: the old and new schedules overlapped", ch, most, paced+1)
		}
		if len(sends) != perChannel || after == 0 {
			t.Errorf("%s: %d of %d sent, %d after the step: sends must resume", ch, len(sends), perChannel, after)
		}
	}
	if stepped == 0 {
		t.Errorf("no slot counted as given up for the clock step")
	}
}

// channelSender records when each request left, by channel.
type channelSender struct {
	mu sync.Mutex
	at map[notification.Channel][]time.Time
}

func (s *channelSender) Send(_ context.Context, m provider.Message) (provider.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.at[m.Channel] = append(s.at[m.Channel], time.Now())
	return provider.Response{Status: 202, MessageID: "m"}, nil
}

func (s *channelSender) times(ch notification.Channel) []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.at[ch])
}

func (s *channelSender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, ts := range s.at {
		n += len(ts)
	}
	return n
}

func per100(sends []time.Time, from time.Time, n int) []int {
	out := make([]int, n)
	for _, s := range sends {
		if k := int(s.Sub(from) / (100 * time.Millisecond)); s.After(from) && k < n {
			out[k]++
		}
	}
	return out
}

// R8 across steps of the limiter's clock that do not restart its schedule:
// a forward step of s makes the new slots arrive s early against the old
// mapping's, so the streams overlap for s. Under one interval that is at most
// one request more in a second and nothing is voided; from one interval on
// (one step, or several close together) the old slots are voided. A backward
// step leaves a gap and voids nothing.
func TestWorkersAcrossSmallLimiterClockSteps(t *testing.T) {
	const interval, perChannel = 10 * time.Millisecond, 300
	paced := int(time.Second / interval)
	cases := []struct {
		name  string
		steps []time.Duration // one every every, once the stream is steady
		every time.Duration
		// voids: some slots are given up as clock_stepped; slack: the most
		// sends a sliding second may hold above the paced rate.
		voids bool
		slack int
	}{
		{"small forward steps void nothing and keep the paced rate",
			[]time.Duration{time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond}, 300 * time.Millisecond, false, 2},
		{"a forward step of just over one interval voids the old slots",
			[]time.Duration{interval + time.Millisecond}, 0, true, 1},
		{"a backward step voids nothing",
			[]time.Duration{-3 * interval}, 0, false, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chans := []notification.Channel{notification.ChannelSMS, notification.ChannelEmail, notification.ChannelPush}
			q := newMemQueue()
			for _, ch := range chans {
				q.add(perChannel, ch, notification.PriorityNormal)
			}
			lim := newSteppingLimiter(interval, 100*time.Millisecond)
			snd := &channelSender{at: map[notification.Channel][]time.Time{}}
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() { _ = mp.Shutdown(context.Background()) }()
			cfg := testConfig()
			cfg.Channels = chans
			cfg.Horizon = 100 * time.Millisecond
			cfg.SendAlign = 100 * time.Millisecond
			var stops []func() error
			for range 2 {
				stops = append(stops, run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: snd, Meter: mp})))
			}
			waitFor(t, "a steady stream before the steps", func() bool { return snd.count() >= 150 })
			for i, d := range tc.steps {
				if i > 0 {
					time.Sleep(tc.every)
				}
				lim.step(d)
			}
			deadline := time.Now().Add(20 * time.Second)
			for q.count(notification.StatusSent) < perChannel*len(chans) && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			for _, stop := range stops {
				if err := stop(); err != nil {
					t.Fatal(err)
				}
			}
			rm := collect(t, reader)
			stepped := counted(rm, "notifier.slots.given_up", attribute.String("reason", "clock_stepped"))
			others := counted(rm, "notifier.slots.given_up") - stepped
			t.Logf("slots given up: %d clock_stepped, %d for other reasons", stepped, others)
			// The pace allows five intervals, a backward step's gap, and one
			// interval for each slot given up for another reason (a loaded
			// machine misses some): what it catches is slots voided.
			allow := 5 * interval
			for _, d := range tc.steps {
				allow += max(-d, 0)
			}
			allow += time.Duration(others) * interval
			if (stepped > 0) != tc.voids {
				t.Errorf("%d slots given up as clock_stepped, want voided %v", stepped, tc.voids)
			}
			for _, ch := range chans {
				sends := snd.times(ch)
				slices.SortFunc(sends, func(a, b time.Time) int { return a.Compare(b) })
				if len(sends) != perChannel {
					t.Errorf("%s: %d of %d sent", ch, len(sends), perChannel)
					continue
				}
				most, _ := busiestFrom(sends)
				// From the 10th send on the stream is steady: the last leaves
				// about one interval per send later; a void costs a horizon.
				span := sends[len(sends)-1].Sub(sends[9])
				want := time.Duration(len(sends)-10) * interval
				t.Logf("%s: busiest sliding second %d (paced %d); sends 10..%d took %s (paced %s); %d clock_stepped",
					ch, most, paced, len(sends), span.Round(time.Millisecond), want, stepped)
				if most > paced+tc.slack {
					t.Errorf("%s: busiest sliding second %d sends, want at most %d", ch, most, paced+tc.slack)
				}
				if !tc.voids && span > want+allow {
					t.Errorf("%s: sends 10..%d took %s, want at most %s: the paced rate was not kept", ch, len(sends), span, want+allow)
				}
			}
		})
	}
}
