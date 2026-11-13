//go:build integration

package limiter_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/limiter"
	"github.com/LeventAksakal/notification-system/test/support"
)

func TestLimiterHoldsTheRateAcrossTakers(t *testing.T) {
	addr := support.Valkey(t)
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const rate, takers, run = 100, 8, 4 * time.Second
	var mu sync.Mutex
	var grants []time.Time
	ctx, cancel := context.WithTimeout(t.Context(), run)
	defer cancel()
	var wg sync.WaitGroup
	for range takers {
		// One Limiter per taker, as each worker process has its own.
		l := limiter.New(client, rate, 3, "test:rate:")
		wg.Go(func() {
			for ctx.Err() == nil {
				got, wait, err := l.Take(ctx, core.ChannelSMS, 1)
				if err != nil {
					if ctx.Err() == nil {
						t.Errorf("take: %v", err)
					}
					return
				}
				if got > 0 {
					mu.Lock()
					grants = append(grants, time.Now())
					mu.Unlock()
					continue
				}
				select {
				case <-ctx.Done():
				case <-time.After(wait):
				}
			}
		})
	}
	wg.Wait()
	slices.SortFunc(grants, func(a, b time.Time) int { return a.Compare(b) })
	busiest := 0
	for i := range grants {
		j, _ := slices.BinarySearchFunc(grants, grants[i].Add(time.Second), func(a, b time.Time) int { return a.Compare(b) })
		busiest = max(busiest, j-i)
	}
	avg := float64(len(grants)-1) / grants[len(grants)-1].Sub(grants[0]).Seconds()
	t.Logf("%d grants to %d takers: busiest 1 s window %d, average %.1f/s", len(grants), takers, busiest, avg)
	if busiest > rate {
		t.Errorf("busiest one-second window: %d grants, limit %d", busiest, rate)
	}
	if avg < 95 {
		t.Errorf("average %.1f/s, want at least 95 (the limit must be used)", avg)
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
	if got, _, err := l.Take(ctx, core.ChannelSMS, 1); err == nil || got != 0 {
		t.Fatalf("unreachable Valkey: want an error and no permit, got %d, %v", got, err)
	}
}
