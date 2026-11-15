package health_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery/health"
	"github.com/LeventAksakal/notification-system/internal/notification"
)

// contractCfg is small so the Valkey run (real time) stays short: 3 faults
// declare an outage, 200 ms of cool-down, a probe lease of 100 ms.
var contractCfg = health.Config{Failures: 3, Cooldown: 200 * time.Millisecond}

const contractLease = 100 * time.Millisecond

// fixture is one fresh state, a way to move its clock, and a way to lose it
// (a Valkey restart): MemoryState with a fake clock, ValkeyState in real time.
type fixture struct {
	state   health.State
	advance func(time.Duration)
	wipe    func() health.State // returns the state as seen after the loss
	// restoreOld puts the state back to one whose generation is older than
	// every one handed out so far (an older snapshot, a replica behind).
	restoreOld func() health.State
}

// runStateContract checks the rules of the package doc against one State
// implementation. Both MemoryState and ValkeyState must pass it.
func runStateContract(t *testing.T, fresh func(t *testing.T) fixture) {
	ctx := context.Background()
	const ch = notification.ChannelSMS
	// must(t)(step) fails t on a step's error and returns its reply.
	must := func(t *testing.T) func(health.Reply, error) health.Reply {
		return func(r health.Reply, err error) health.Reply {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
			return r
		}
	}
	// declare reports Failures faults on closed tickets and returns the last reply.
	declare := func(t *testing.T, s health.State) health.Reply {
		t.Helper()
		a := must(t)(s.Admit(ctx, ch, contractLease, 0))
		var r health.Reply
		for range contractCfg.Failures {
			r = must(t)(s.Report(ctx, ch, a.Gen, false, true, 0))
		}
		return r
	}

	t.Run("faults below the threshold count and keep it closed; the threshold-th declares the outage and does not count", func(t *testing.T) {
		f := fresh(t)
		a := must(t)(f.state.Admit(ctx, ch, contractLease, 0))
		if a.Kind != health.Closed {
			t.Fatalf("a fresh channel admits %v, want closed", a.Kind)
		}
		for i := 1; i < contractCfg.Failures; i++ {
			if r := must(t)(f.state.Report(ctx, ch, a.Gen, false, true, 0)); !r.Healthy() || r.Transition != health.None {
				t.Fatalf("fault %d: %+v, want closed (counted)", i, r)
			}
		}
		r := must(t)(f.state.Report(ctx, ch, a.Gen, false, true, 0))
		if r.Healthy() || r.Transition != health.Opened {
			t.Fatalf("fault %d: %+v, want open, opened (not counted)", contractCfg.Failures, r)
		}
		if r.Wait <= 0 || r.Wait > contractCfg.Cooldown {
			t.Errorf("wait to the next probe %v, want in (0, %v]", r.Wait, contractCfg.Cooldown)
		}
	})

	t.Run("a success between faults resets the count", func(t *testing.T) {
		f := fresh(t)
		a := must(t)(f.state.Admit(ctx, ch, contractLease, 0))
		for range 2 {
			for i := 1; i < contractCfg.Failures; i++ {
				must(t)(f.state.Report(ctx, ch, a.Gen, false, true, 0))
			}
			must(t)(f.state.Report(ctx, ch, a.Gen, false, false, 0))
		}
		if r := must(t)(f.state.Admit(ctx, ch, contractLease, 0)); r.Kind != health.Closed {
			t.Fatalf("after interrupted runs of faults: %v, want closed", r.Kind)
		}
	})

	t.Run("while open nobody is admitted and late results of earlier calls change nothing", func(t *testing.T) {
		f := fresh(t)
		a := must(t)(f.state.Admit(ctx, ch, contractLease, 0))
		declare(t, f.state)
		if r := must(t)(f.state.Admit(ctx, ch, contractLease, 0)); r.Kind != health.Wait || r.Wait <= 0 {
			t.Fatalf("admit during the cool-down: %+v, want wait", r)
		}
		for _, fault := range []bool{true, false} {
			if r := must(t)(f.state.Report(ctx, ch, a.Gen, false, fault, 0)); r.Kind != health.Open || r.Transition != health.None {
				t.Fatalf("late result (fault %v) of a call admitted while closed: %+v, want open, no change", fault, r)
			}
		}
	})

	t.Run("one probe per cool-down: the first asker gets it, the others wait", func(t *testing.T) {
		f := fresh(t)
		declare(t, f.state)
		f.advance(contractCfg.Cooldown)
		p := must(t)(f.state.Admit(ctx, ch, contractLease, 0))
		if p.Kind != health.Probe {
			t.Fatalf("first admit after the cool-down: %v, want probe", p.Kind)
		}
		if r := must(t)(f.state.Admit(ctx, ch, contractLease, 0)); r.Kind != health.Wait || r.Wait <= 0 || r.Wait > contractLease {
			t.Fatalf("second admit while the probe is out: %+v, want wait up to the lease", r)
		}
		r := must(t)(f.state.Report(ctx, ch, p.Gen, true, true, 0))
		if r.Healthy() || r.Transition != health.None {
			t.Fatalf("failed probe: %+v, want open (not counted)", r)
		}
		if r := must(t)(f.state.Admit(ctx, ch, contractLease, 0)); r.Kind != health.Wait {
			t.Fatalf("admit right after a failed probe: %v, want wait for a new cool-down", r.Kind)
		}
		f.advance(contractCfg.Cooldown)
		p2 := must(t)(f.state.Admit(ctx, ch, contractLease, 0))
		if p2.Kind != health.Probe {
			t.Fatalf("admit a cool-down after the failed probe: %v, want probe", p2.Kind)
		}
		r = must(t)(f.state.Report(ctx, ch, p2.Gen, true, false, 0))
		if r.Kind != health.Closed || r.Transition != health.Recovered {
			t.Fatalf("successful probe: %+v, want closed, recovered", r)
		}
		if r := must(t)(f.state.Admit(ctx, ch, contractLease, 0)); r.Kind != health.Closed {
			t.Fatalf("admit after recovery: %v, want closed", r.Kind)
		}
	})

	t.Run("a probe whose holder died is freed by its lease, and its late result is ignored", func(t *testing.T) {
		f := fresh(t)
		declare(t, f.state)
		f.advance(contractCfg.Cooldown)
		dead := must(t)(f.state.Admit(ctx, ch, contractLease, 0))
		f.advance(contractLease)
		next := must(t)(f.state.Admit(ctx, ch, contractLease, 0))
		if next.Kind != health.Probe || next.Gen <= dead.Gen {
			t.Fatalf("admit after the dead probe's lease: %+v, want a probe of a newer generation than %d", next, dead.Gen)
		}
		if r := must(t)(f.state.Report(ctx, ch, dead.Gen, true, false, 0)); r.Kind != health.Open || r.Transition != health.None {
			t.Fatalf("the dead probe's late success: %+v, want ignored (still open)", r)
		}
	})

	t.Run("a cancelled probe lets the next asker probe at once", func(t *testing.T) {
		f := fresh(t)
		declare(t, f.state)
		f.advance(contractCfg.Cooldown)
		p := must(t)(f.state.Admit(ctx, ch, contractLease, 0))
		must(t)(f.state.Cancel(ctx, ch, p.Gen))
		if r := must(t)(f.state.Admit(ctx, ch, contractLease, 0)); r.Kind != health.Probe {
			t.Fatalf("admit after a cancelled probe: %v, want probe", r.Kind)
		}
	})

	t.Run("a Retry-After longer than the cool-down delays the next probe", func(t *testing.T) {
		f := fresh(t)
		a := must(t)(f.state.Admit(ctx, ch, contractLease, 0))
		for i := 1; i < contractCfg.Failures; i++ {
			must(t)(f.state.Report(ctx, ch, a.Gen, false, true, 0))
		}
		r := must(t)(f.state.Report(ctx, ch, a.Gen, false, true, 2*contractCfg.Cooldown))
		if r.Wait <= contractCfg.Cooldown {
			t.Fatalf("wait after a Retry-After of %v: %v, want more than the cool-down", 2*contractCfg.Cooldown, r.Wait)
		}
		f.advance(contractCfg.Cooldown)
		if r := must(t)(f.state.Admit(ctx, ch, contractLease, 0)); r.Kind != health.Wait {
			t.Fatalf("admit after the cool-down but before Retry-After: %v, want wait", r.Kind)
		}
	})

	t.Run("a lost state reads closed with a generation above every earlier one", func(t *testing.T) {
		f := fresh(t)
		r := declare(t, f.state)
		s := f.wipe()
		a := must(t)(s.Admit(ctx, ch, contractLease, 0))
		if a.Kind != health.Closed || a.Gen <= r.Gen {
			t.Fatalf("after the loss: %+v, want closed with a generation above %d", a, r.Gen)
		}
	})

	t.Run("a state restored with an older generation is raised above the one the asker has seen", func(t *testing.T) {
		f := fresh(t)
		r := declare(t, f.state) // the asker has seen the open generation
		s := f.restoreOld()
		a := must(t)(s.Admit(ctx, ch, contractLease, r.Gen))
		if a.Kind != health.Closed || a.Gen <= r.Gen {
			t.Fatalf("after the restore: %+v, want closed with a generation above the seen %d", a, r.Gen)
		}
		if again := must(t)(s.Admit(ctx, ch, contractLease, a.Gen)); again.Gen != a.Gen {
			t.Fatalf("a second admit moved the generation from %d to %d; it must stay while nothing changes", a.Gen, again.Gen)
		}
	})

	t.Run("concurrent askers after the cool-down get exactly one probe", func(t *testing.T) {
		f := fresh(t)
		declare(t, f.state)
		f.advance(contractCfg.Cooldown)
		const askers = 20
		var mu sync.Mutex
		probes := 0
		var wg sync.WaitGroup
		for range askers {
			wg.Go(func() {
				r, err := f.state.Admit(ctx, ch, time.Minute, 0)
				if err != nil {
					t.Error(err)
					return
				}
				if r.Kind == health.Probe {
					mu.Lock()
					probes++
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		if probes != 1 {
			t.Fatalf("%d askers got %d probes, want 1", askers, probes)
		}
	})
}

// fakeClock is the MemoryState's clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestMemoryStateKeepsTheContract(t *testing.T) {
	runStateContract(t, func(t *testing.T) fixture {
		c := &fakeClock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}
		return fixture{
			state:   health.NewMemory(contractCfg, c.now),
			advance: c.add,
			wipe: func() health.State {
				// Generations lead the clock by at most the changes made within
				// one microsecond; a Valkey restart takes seconds.
				c.add(time.Second)
				return health.NewMemory(contractCfg, c.now)
			},
			restoreOld: func() health.State {
				// A fresh state on a clock a minute behind: its generation is
				// below every one handed out before.
				back := &fakeClock{t: c.now().Add(-time.Minute)}
				return health.NewMemory(contractCfg, back.now)
			},
		}
	})
}
