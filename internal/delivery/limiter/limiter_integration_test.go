//go:build integration

package limiter_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/delivery/limiter"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/support"
)

// grant is one Take that reserved something.
type grant struct {
	g notification.Grant
}

// TestLimiterHoldsTheRateAcrossTakers runs three takers that reserve slots in
// batches, as three workers do, and checks the slots themselves: no two
// overlap, no sliding second holds more than the paced rate, and (with one
// lead for all) the paced rate is used.
// Takers whose leads differ and change on every Take are workers whose
// measured claim times differ: their slots must still share one grid.
func TestLimiterHoldsTheRateAcrossTakers(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for i, c := range []struct {
		name    string
		lead    func() time.Duration
		useRate bool // a free grid restarts at now+lead, unused up to it
	}{
		{"one lead for every taker", func() time.Duration { return 0 }, true},
		{"leads that differ and change on every take", func() time.Duration {
			return []time.Duration{20 * time.Millisecond, 50 * time.Millisecond, 400 * time.Millisecond, 1600 * time.Millisecond}[rand.IntN(4)] //nolint:gosec // test jitter
		}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			takersShareOneGrid(t, client, fmt.Sprintf("test:rate%d:", i), c.lead, c.useRate)
		})
	}
}

func takersShareOneGrid(t *testing.T, client valkey.Client, prefix string, lead func() time.Duration, useRate bool) {
	// The deployed default: 100 per second paced 5% under, 95 per second.
	const rate, headroom, takers, run = 100, 5, 3, 4 * time.Second
	const minHorizon = 200 * time.Millisecond
	var mu sync.Mutex
	var grants []grant
	ctx, cancel := context.WithTimeout(t.Context(), run)
	defer cancel()
	var wg sync.WaitGroup
	for range takers {
		// One Limiter per taker, as each worker process has its own.
		l := limiter.New(client, rate, headroom, prefix)
		wg.Go(func() {
			for ctx.Err() == nil {
				// As the worker: the horizon is at least the lead.
				ld := lead()
				horizon := max(minHorizon, ld)
				want := int(horizon/l.Interval()) + 1
				g, err := l.Take(ctx, notification.ChannelSMS, notification.SlotRequest{N: want, Lead: ld, Horizon: horizon})
				if err != nil {
					if ctx.Err() == nil {
						t.Errorf("take: %v", err)
					}
					return
				}
				if g.N > 0 {
					mu.Lock()
					grants = append(grants, grant{g: g})
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

	// Slots are judged on the limiter's own clock, where they are exact:
	// slot k of a grant is at At + First + k*Interval, in whole microseconds.
	// (Placing them from the moment the reply arrived, as this test once did,
	// measures the taker's scheduling: on a two-core runner it saw grants
	// "overlap" by more than a round trip.) Grants of one key follow each
	// other exactly, or after a gap when the schedule restarted at "now".
	firstAt := func(x grant) time.Time { return x.g.At.Add(x.g.First) }
	slices.SortFunc(grants, func(a, b grant) int { return firstAt(a).Compare(firstAt(b)) })
	var slots []time.Time
	var prevEnd time.Time
	restarts := 0
	for i, x := range grants {
		at := firstAt(x)
		if i > 0 {
			switch d := at.Sub(prevEnd); {
			case d < 0:
				t.Fatalf("grant %d overlaps the previous one by %s", i, -d)
			case d > 0:
				restarts++
			}
		}
		for k := range x.g.N {
			slots = append(slots, at.Add(time.Duration(k)*interval))
		}
		prevEnd = at.Add(time.Duration(x.g.N) * interval)
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
	// The paced rate must be used: the average may fall short of the exact
	// slot rate (1 s / interval, 94.99 for 95 per second) by half a slot.
	if floor := float64(time.Second)/float64(interval) - 0.5; useRate && avg < floor {
		t.Errorf("average %.2f/s, want at least %.2f (the paced rate must be used)", avg, floor)
	}
}

func TestLimiterReservesWithinTheHorizon(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	l := limiter.New(client, 100, 0, "test:horizon:")
	r := notification.SlotRequest{N: 100, Horizon: 50 * time.Millisecond}
	g, err := l.Take(t.Context(), notification.ChannelEmail, r)
	if err != nil {
		t.Fatal(err)
	}
	// 10 ms slots at 0, 10, ..., 50 ms: six fit in a 50 ms horizon.
	if g.N != 6 || g.First != 0 || g.Interval != 10*time.Millisecond || g.Wait <= 0 || g.Wait > 10*time.Millisecond {
		t.Fatalf("first take: got %+v, want 6 slots from now and a wait of at most one interval", g)
	}
	end := g.At.Add(g.First + time.Duration(g.N)*g.Interval) // the first grant's end
	g, err = l.Take(t.Context(), notification.ChannelEmail, r)
	if err != nil {
		t.Fatal(err)
	}
	// Measured against the first grant's end, not the second Take's now: a
	// machine that pauses between the two calls moves the latter.
	if g.N > 1 || (g.N == 1 && !g.At.Add(g.First).Equal(end)) {
		t.Fatalf("second take at once: got %+v, want nothing, or one slot at the first grant's end (%s)", g, end)
	}
}

// TestLimiterRestartsAFreeGridAtTheLead: a free grid restarts at now+lead; a
// busy grid continues at its next free slot, whatever lead the next taker
// asks with, so a smaller lead never brings a slot closer and a larger one
// never leaves a gap.
func TestLimiterRestartsAFreeGridAtTheLead(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	l := limiter.New(client, 100, 0, "test:lead:")
	g1, err := l.Take(t.Context(), notification.ChannelSMS, notification.SlotRequest{N: 100, Lead: 300 * time.Millisecond, Horizon: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if g1.N != 6 || g1.First != 300*time.Millisecond {
		t.Fatalf("free grid, lead 300ms: got %+v, want 6 slots from 300ms", g1)
	}
	end := func(g notification.Grant) time.Time { return g.At.Add(g.First + time.Duration(g.N)*g.Interval) }
	for _, lead := range []time.Duration{20 * time.Millisecond, 2 * time.Second} {
		prev := end(g1)
		g, err := l.Take(t.Context(), notification.ChannelSMS, notification.SlotRequest{N: 3, Lead: lead, Horizon: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if got := g.At.Add(g.First); g.N != 3 || !got.Equal(prev) {
			t.Fatalf("busy grid, lead %s: first slot at %s, want %s right after the grant before (%+v)", lead, got, prev, g)
		}
		g1 = g
	}
}

// TestLimiterTakesBackUnusedSlots: slots a taker gives back go to the next
// Take, unless someone reserved after them; given back into the past, they
// leave the grid free since that moment.
func TestLimiterTakesBackUnusedSlots(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	l := limiter.New(client, 100, 0, "test:giveback:")
	// Each case on its own channel, on a grid of its own.
	ch := notification.ChannelSMS
	take := func(n int, lead, horizon time.Duration) notification.Grant {
		t.Helper()
		g, err := l.Take(t.Context(), ch, notification.SlotRequest{N: n, Lead: lead, Horizon: horizon})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	slot := func(g notification.Grant, k int) time.Time { return g.At.Add(g.First + time.Duration(k)*g.Interval) }
	giveBack := func(g notification.Grant, from time.Time) bool {
		t.Helper()
		ok, err := l.GiveBack(t.Context(), ch, g, from)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}

	t.Run("untouched: the next Take gets the slots back", func(t *testing.T) {
		g := take(10, time.Second, 200*time.Millisecond)
		if g.N != 10 || !giveBack(g, slot(g, 4)) {
			t.Fatalf("want 10 slots given back from the fifth, got %+v", g)
		}
		next := take(1, time.Second, 200*time.Millisecond)
		if got := slot(next, 0); !got.Equal(slot(g, 4)) {
			t.Fatalf("next slot at %s, want the first given back, %s", got, slot(g, 4))
		}
	})
	t.Run("reserved after: nothing is given back", func(t *testing.T) {
		ch = notification.ChannelEmail
		g := take(5, time.Second, 200*time.Millisecond)
		after := take(1, time.Second, 200*time.Millisecond)
		if giveBack(g, slot(g, 0)) {
			t.Fatal("given back although a slot was reserved after the grant")
		}
		next := take(1, time.Second, 200*time.Millisecond)
		if got, want := slot(next, 0), slot(after, 1); !got.Equal(want) {
			t.Fatalf("next slot at %s, want %s right after the later grant", got, want)
		}
	})
	t.Run("given back into the past: the grid is free since then", func(t *testing.T) {
		ch = notification.ChannelPush
		g := take(3, 0, 20*time.Millisecond)
		time.Sleep(100 * time.Millisecond) // the slots pass
		from := slot(g, 1)
		if !giveBack(g, from) {
			t.Fatalf("want the slots given back, grant %+v", g)
		}
		next := take(1, 30*time.Millisecond, 20*time.Millisecond)
		if next.First != 30*time.Millisecond || next.Idle != next.At.Sub(from) {
			t.Fatalf("want a free grid restarted at the lead and Idle %s since the moment given back, got %+v", next.At.Sub(from), next)
		}
	})
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
	l := limiter.New(client, 100, 0, "test:clock:")
	three := notification.SlotRequest{N: 3, Horizon: 500 * time.Millisecond}
	g1, err := l.Take(t.Context(), notification.ChannelPush, three)
	if err != nil {
		t.Fatal(err)
	}
	// A Take of no slots reads the clock and reserves nothing.
	g0, err := l.Take(t.Context(), notification.ChannelPush, notification.SlotRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if g0.N != 0 || g0.At.Before(g1.At) || g0.Interval != g1.Interval {
		t.Fatalf("Take of 0: want no slot, At not before the first grant's (%s), the interval; got %+v", g1.At, g0)
	}
	g2, err := l.Take(t.Context(), notification.ChannelPush, three)
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
	l := limiter.New(client, 100, 3, "x:")
	client.Close() // the connection is gone, as when Valkey goes away
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if g, err := l.Take(ctx, notification.ChannelSMS, notification.SlotRequest{N: 1, Horizon: 200 * time.Millisecond}); err == nil || g.N != 0 {
		t.Fatalf("unreachable Valkey: want an error and no slot, got %+v, %v", g, err)
	}
}

// TestLimiterReportsHowLongTheGridWasFree: Idle tells a taker how long ago the
// last slot reserved before its grant was due, so it can tell whether any
// earlier request can still be waiting to leave. It reserves nothing itself.
func TestLimiterReportsHowLongTheGridWasFree(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	l := limiter.New(client, 100, 0, "test:idle:")
	req := func(n int) notification.SlotRequest {
		return notification.SlotRequest{N: n, Horizon: 50 * time.Millisecond}
	}
	ch := notification.ChannelSMS

	g, err := l.Take(t.Context(), ch, req(2))
	if err != nil {
		t.Fatal(err)
	}
	if g.N != 2 || g.Idle != time.Second {
		t.Fatalf("first take on a fresh key: want 2 slots and Idle of exactly the 1 s a missing key proves, got %+v", g)
	}
	g, err = l.Take(t.Context(), ch, req(2))
	if err != nil {
		t.Fatal(err)
	}
	if g.Idle != 0 {
		t.Fatalf("take while earlier slots are still ahead: want Idle 0, got %+v", g)
	}
	last := g.At.Add(g.First + time.Duration(g.N-1)*g.Interval) // the last slot reserved so far
	time.Sleep(150 * time.Millisecond)                          // let the grid fall idle
	g0, err := l.Take(t.Context(), ch, req(0))
	if err != nil {
		t.Fatal(err)
	}
	g, err = l.Take(t.Context(), ch, req(1))
	if err != nil {
		t.Fatal(err)
	}
	// The TAT is one interval past the last slot.
	want := g.At.Sub(last.Add(g.Interval))
	if g.N != 1 || g.First != 0 || g.Idle != want || g0.Idle <= 0 || g0.Idle > g.Idle {
		t.Fatalf("take after an idle gap: want 1 slot now and Idle %s (a Take of 0 before it: 0 < %s <= that), got %+v", want, g0.Idle, g)
	}
}
