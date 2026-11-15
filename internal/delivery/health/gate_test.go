package health_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
)

// swappable lets a test replace the state under a gate, as Valkey does when
// it restarts from an older snapshot.
type swappable struct {
	mu sync.Mutex
	health.State
}

func (s *swappable) set(st health.State) { s.mu.Lock(); s.State = st; s.mu.Unlock() }
func (s *swappable) get() health.State   { s.mu.Lock(); defer s.mu.Unlock(); return s.State }
func (s *swappable) Admit(ctx context.Context, ch notification.Channel, l time.Duration, seen int64) (health.Reply, error) {
	return s.get().Admit(ctx, ch, l, seen)
}

func (s *swappable) Report(ctx context.Context, ch notification.Channel, gen int64, probe, fault bool, ra time.Duration) (health.Reply, error) {
	return s.get().Report(ctx, ch, gen, probe, fault, ra)
}

func (s *swappable) Cancel(ctx context.Context, ch notification.Channel, gen int64) (health.Reply, error) {
	return s.get().Cancel(ctx, ch, gen)
}

func TestAGateKeepsSendingAfterTheStateGoesBackToAnOlderGeneration(t *testing.T) {
	ctx := context.Background()
	c := &fakeClock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}
	st := &swappable{State: health.NewMemory(health.Config{Failures: 1, Cooldown: time.Minute}, c.now)}
	g := health.NewGate(st, notification.ChannelSMS)
	tk, ok, _, err := g.Allow(ctx, time.Second)
	if err != nil || !ok {
		t.Fatalf("first admit: ok %v err %v", ok, err)
	}
	if _, err := g.Done(ctx, tk, true, 0); err != nil { // declares: the gate caches the open generation
		t.Fatal(err)
	}
	back := &fakeClock{t: c.now().Add(-time.Minute)}
	st.set(health.NewMemory(health.Config{Failures: 1, Cooldown: time.Minute}, back.now)) // an older snapshot, closed
	tk, ok, _, err = g.Allow(ctx, time.Second)
	if err != nil || !ok {
		t.Fatalf("admit after the restore: ok %v err %v, want admitted (closed)", ok, err)
	}
	if !g.Current(tk) {
		t.Fatal("the ticket from the restored state is not current: the worker would never send on this channel again")
	}
}

func TestAProbeTicketExpiresWithItsLease(t *testing.T) {
	ctx := context.Background()
	st := health.NewMemory(health.Config{Failures: 1, Cooldown: 0}, time.Now)
	g := health.NewGate(st, notification.ChannelSMS)
	tk, _, _, _ := g.Allow(ctx, time.Second)
	if _, err := g.Done(ctx, tk, true, 0); err != nil {
		t.Fatal(err)
	}
	p, ok, _, err := g.Allow(ctx, 0) // the cool-down is 0: this is the probe, with a lease of 0, over at once
	if err != nil || !ok || !p.Probe() {
		t.Fatalf("want the probe; ok %v err %v probe %v", ok, err, p.Probe())
	}
	if g.Current(p) {
		t.Error("a probe past its lease is still current: another worker may hold the probe by now")
	}
}
