package worker

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

func TestClockMapBounds(t *testing.T) {
	epoch := time.Now()
	remote0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	// A sample is a Take: local ms since epoch before and after, remote ms
	// since remote0 at the reservation. Offsets are reported relative to
	// local epoch minus remote0.
	type sample struct{ sent, got, at int }
	cases := []struct {
		name         string
		samples      []sample
		lower, upper time.Duration
	}{
		{"one sample bounds the offset by its round trip",
			[]sample{{100, 110, 105}}, ms(-5), ms(5)},
		{"a faster round trip tightens the bounds",
			[]sample{{100, 110, 105}, {200, 202, 201}}, ms(-1), ms(1)},
		{"a slow round trip does not loosen them",
			[]sample{{200, 202, 201}, {300, 340, 301}}, ms(-1), ms(1)},
		{"bounds from different samples combine",
			[]sample{{100, 120, 119}, {200, 220, 201}}, ms(-1), ms(1)},
		{"samples older than the window expire",
			[]sample{{100, 102, 101}, {20_100, 20_140, 20_120}}, ms(-20), ms(20)},
		{"crossed bounds restart from the newest sample",
			[]sample{{100, 102, 101}, {300, 302, 250}}, ms(50), ms(52)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newClockMap(epoch, 10*time.Second)
			base := -time.Duration(remote0.UnixNano())
			var lower, upper time.Duration
			for _, s := range tc.samples {
				lower, upper = c.observe(epoch.Add(ms(s.sent)), epoch.Add(ms(s.got)), remote0.Add(ms(s.at)))
			}
			if lower-base != tc.lower || upper-base != tc.upper {
				t.Fatalf("bounds [%s, %s], want [%s, %s]", lower-base, upper-base, tc.lower, tc.upper)
			}
			// A remote moment maps to local with the given offset.
			if got := c.toLocal(remote0.Add(ms(7)), upper); got.Sub(epoch) != ms(7)+tc.upper {
				t.Fatalf("toLocal: %s after epoch, want %s", got.Sub(epoch), ms(7)+tc.upper)
			}
		})
	}
}

// delayedLimiter is a gridLimiter whose replies arrive late: the grant is
// made (and At stamped) at once, then held for the next of delays. It
// records every slot it handed out on the local monotonic clock.
type delayedLimiter struct {
	grid   *gridLimiter
	delays []time.Duration

	mu    sync.Mutex
	k     int
	slots []time.Time
}

func (l *delayedLimiter) Take(ctx context.Context, ch core.Channel, n int) (core.Grant, error) {
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
	q.add(40, core.ChannelSMS, core.PriorityNormal)
	lim := &delayedLimiter{grid: newGridLimiter(interval, 100*time.Millisecond), delays: []time.Duration{0, delay}}
	snd := &timedSender{}
	cfg := testConfig()
	cfg.Channels = []core.Channel{core.ChannelSMS}
	cfg.Horizon = 100 * time.Millisecond
	cfg.SendAlign = 150 * time.Millisecond // above the delay: late groups lose no slot
	stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: snd}))
	waitFor(t, "all 40 sent", func() bool { return q.count(core.StatusSent) == 40 })
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
		q.add(1, core.ChannelSMS, core.PriorityHigh)
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}, Meter: mp}))
		waitFor(t, "the job sent", func() bool { return q.count(core.StatusSent) == 1 })
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
	t.Run("an uncertain limiter clock gives slots up and sends nothing", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		defer func() { _ = mp.Shutdown(context.Background()) }()
		q := newMemQueue()
		q.add(5, core.ChannelSMS, core.PriorityHigh)
		// Every reply takes longer than an interval: the offset is never
		// known to within one interval, so no slot can be placed safely.
		lim := &delayedLimiter{grid: newGridLimiter(10*time.Millisecond, 50*time.Millisecond), delays: []time.Duration{25 * time.Millisecond}}
		snd := &countingSender{fn: accept}
		cfg := testConfig()
		cfg.Channels = []core.Channel{core.ChannelSMS}
		stop := run(t, New(cfg, Deps{Queue: q, Limiter: lim, Sender: snd, Meter: mp}))
		waitFor(t, "slots given up", func() bool {
			return counted(collect(t, reader), "notifier.slots.given_up", sms, attribute.String("reason", "clock_uncertain")) >= 3
		})
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if n := snd.calls.Load(); n != 0 {
			t.Fatalf("%d provider calls with the limiter's clock unknown to within an interval, want 0", n)
		}
	})
}
