//go:build integration

package limiter_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/delivery/limiter"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/support"
)

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

// TestLimiterReturnsUnusedSlotsInAnyOrder: a grant given back while a later
// one sits after it is returned once the later one is given back from its
// start, whatever the order; a later grant that sent something keeps it.
// Workers woken together reserve one after another, and the first (the flex
// group) often gives back before the ones after it (TestGapDeliveryR7Second
// RowAfterALoneSend).
func TestLimiterReturnsUnusedSlotsInAnyOrder(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	slot := func(g notification.Grant, k int) time.Time { return g.At.Add(g.First + time.Duration(k)*g.Interval) }
	for i, c := range []struct {
		name  string
		sizes []int
		// back[k] = {grant, first slot given back}, in the order given back.
		back [][2]int
		// want: the next Take's first slot is slot want[1] of grant want[0].
		want [2]int
	}{
		{"the earlier grant first, then the later one in full", []int{5, 3}, [][2]int{{0, 1}, {1, 0}}, [2]int{0, 1}},
		{"the later grant first, then the earlier one", []int{5, 3}, [][2]int{{1, 0}, {0, 2}}, [2]int{0, 2}},
		{"three grants, the middle one first", []int{4, 2, 3}, [][2]int{{1, 0}, {0, 3}, {2, 0}}, [2]int{0, 3}},
		{"a later grant that sent keeps its sent slots", []int{5, 3}, [][2]int{{0, 0}, {1, 1}}, [2]int{1, 1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			l := limiter.New(client, 100, 0, fmt.Sprintf("test:giveback-order-%d:", i))
			ch := notification.ChannelSMS
			var gs []notification.Grant
			for _, n := range c.sizes {
				g, err := l.Take(t.Context(), ch, notification.SlotRequest{N: n, Lead: time.Second, Horizon: time.Second})
				if err != nil || g.N != n {
					t.Fatalf("take %d: %+v, %v", n, g, err)
				}
				gs = append(gs, g)
			}
			for _, b := range c.back {
				if _, err := l.GiveBack(t.Context(), ch, gs[b[0]], slot(gs[b[0]], b[1])); err != nil {
					t.Fatal(err)
				}
			}
			next, err := l.Take(t.Context(), ch, notification.SlotRequest{N: 1, Lead: time.Second, Horizon: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if got, want := slot(next, 0), slot(gs[c.want[0]], c.want[1]); !got.Equal(want) {
				t.Fatalf("next slot at %s, want %s (slot %d of grant %d)", got, want, c.want[1], c.want[0])
			}
		})
	}
}
