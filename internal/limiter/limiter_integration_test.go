//go:build integration

package limiter_test

import (
	"context"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/limiter"
	"github.com/LeventAksakal/notification-system/test/support"
)

// grant is one Take as a taker saw it: the call's local start and end.
type grant struct {
	start, end time.Time
	g          core.Grant
}

// TestLimiterHoldsTheRateAcrossTakers runs three takers that reserve slots in
// batches, as three workers do, and checks the slots themselves: no two
// overlap, no sliding second holds more than the paced rate, and the paced
// rate is used.
func TestLimiterHoldsTheRateAcrossTakers(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const rate, headroom, takers, run = 100, 3, 3, 4 * time.Second
	const horizon = 200 * time.Millisecond
	var mu sync.Mutex
	var grants []grant
	ctx, cancel := context.WithTimeout(t.Context(), run)
	defer cancel()
	var wg sync.WaitGroup
	for range takers {
		// One Limiter per taker, as each worker process has its own.
		l := limiter.New(client, rate, headroom, horizon, "test:rate:")
		want := int(horizon/l.Interval()) + 1
		wg.Go(func() {
			for ctx.Err() == nil {
				start := time.Now()
				g, err := l.Take(ctx, core.ChannelSMS, want)
				end := time.Now()
				if err != nil {
					if ctx.Err() == nil {
						t.Errorf("take: %v", err)
					}
					return
				}
				if g.N > 0 {
					mu.Lock()
					grants = append(grants, grant{start: start, end: end, g: g})
					mu.Unlock()
				}
				// As the worker: ask again once half a horizon can be reserved.
				select {
				case <-ctx.Done():
				case <-time.After(g.Wait + horizon/2):
				}
			}
		})
	}
	wg.Wait()
	if len(grants) < 2 {
		t.Fatalf("only %d grants", len(grants))
	}
	interval := grants[0].g.Interval
	paced := int(math.Ceil(float64(time.Second) / float64(interval)))

	// A slot's local time is known up to the round trip of its Take. Grants
	// from one key are contiguous or separated by a restart; place each grant
	// right after the previous one when its estimate is within the round trip.
	firstAt := func(x grant) time.Time { return x.end.Add(x.g.First) }
	slices.SortFunc(grants, func(a, b grant) int { return firstAt(a).Compare(firstAt(b)) })
	var slots []time.Time
	var prevEnd time.Time
	var prevRTT time.Duration
	restarts := 0
	for i, x := range grants {
		at := firstAt(x)
		rtt := x.end.Sub(x.start)
		tol := max(rtt, prevRTT) + time.Millisecond
		if i > 0 {
			switch d := at.Sub(prevEnd); {
			case d < -tol:
				t.Fatalf("grant %d overlaps the previous one by %s (tolerance %s)", i, -d, tol)
			case d <= tol:
				at = prevEnd
			default:
				restarts++
			}
		}
		for k := range x.g.N {
			slots = append(slots, at.Add(time.Duration(k)*interval))
		}
		prevEnd, prevRTT = at.Add(time.Duration(x.g.N)*interval), rtt
	}
	for i := 1; i < len(slots); i++ {
		if d := slots[i].Sub(slots[i-1]); d < interval {
			t.Fatalf("slots %d and %d are %s apart, interval %s", i-1, i, d, interval)
		}
	}
	busiest := 0
	for i := range slots {
		j, _ := slices.BinarySearchFunc(slots, slots[i].Add(time.Second), func(a, b time.Time) int { return a.Compare(b) })
		busiest = max(busiest, j-i)
	}
	avg := float64(len(slots)-1) / slots[len(slots)-1].Sub(slots[0]).Seconds()
	t.Logf("%d slots in %d grants to %d takers (interval %s, %d restarts): busiest 1 s window %d (paced %d), average %.2f/s",
		len(slots), len(grants), takers, interval, restarts, busiest, paced, avg)
	if busiest > paced {
		t.Errorf("busiest one-second window: %d slots, paced rate %d", busiest, paced)
	}
	if avg < 96.5 {
		t.Errorf("average %.2f/s, want at least 96.5 (the paced rate must be used)", avg)
	}
}

func TestLimiterReservesWithinTheHorizon(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	l := limiter.New(client, 100, 0, 50*time.Millisecond, "test:horizon:")
	g, err := l.Take(t.Context(), core.ChannelEmail, 100)
	if err != nil {
		t.Fatal(err)
	}
	// 10 ms slots at 0, 10, ..., 50 ms: six fit in a 50 ms horizon.
	if g.N != 6 || g.First != 0 || g.Interval != 10*time.Millisecond || g.Wait <= 0 || g.Wait > 10*time.Millisecond {
		t.Fatalf("first take: got %+v, want 6 slots from now and a wait of at most one interval", g)
	}
	g, err = l.Take(t.Context(), core.ChannelEmail, 100)
	if err != nil {
		t.Fatal(err)
	}
	if g.N > 1 || g.First < 50*time.Millisecond {
		t.Fatalf("second take at once: got %+v, want nothing (or one slot) beyond the first six", g)
	}
}

// TestLimiterGrantCarriesValkeysClock: At is the "now" First is measured
// from, so two grants on one key name the same grid on Valkey's timeline.
func TestLimiterGrantCarriesValkeysClock(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	l := limiter.New(client, 100, 0, 500*time.Millisecond, "test:clock:")
	g1, err := l.Take(t.Context(), core.ChannelPush, 3)
	if err != nil {
		t.Fatal(err)
	}
	// A Take of no slots reads the clock and reserves nothing.
	g0, err := l.Take(t.Context(), core.ChannelPush, 0)
	if err != nil {
		t.Fatal(err)
	}
	if g0.N != 0 || g0.At.Before(g1.At) || g0.Interval != g1.Interval {
		t.Fatalf("Take of 0: want no slot, At not before the first grant's (%s), the interval; got %+v", g1.At, g0)
	}
	g2, err := l.Take(t.Context(), core.ChannelPush, 3)
	if err != nil {
		t.Fatal(err)
	}
	if g1.At.IsZero() || g2.At.Before(g1.At) {
		t.Fatalf("At: want Valkey's clock, non-decreasing; got %s then %s", g1.At, g2.At)
	}
	// Containers share the host's clock up to a VM's skew: a minute is generous.
	if d := time.Since(g1.At); d < -time.Minute || d > time.Minute {
		t.Errorf("At %s is %s away from the local clock", g1.At, d)
	}
	want := g1.At.Add(g1.First + time.Duration(g1.N)*g1.Interval)
	if got := g2.At.Add(g2.First); g1.N != 3 || g2.N != 3 || !got.Equal(want) {
		t.Fatalf("second grant's first slot at %s, want %s right after the first grant (%+v, %+v)", got, want, g1, g2)
	}
}

func TestLimiterFailsClosed(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	l := limiter.New(client, 100, 3, 200*time.Millisecond, "x:")
	client.Close() // the connection is gone, as when Valkey goes away
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if g, err := l.Take(ctx, core.ChannelSMS, 1); err == nil || g.N != 0 {
		t.Fatalf("unreachable Valkey: want an error and no slot, got %+v, %v", g, err)
	}
}
