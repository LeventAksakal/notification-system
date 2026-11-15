package breaker

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTest() (*Breaker, *fakeClock) {
	c := &fakeClock{t: time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)}
	return New(Config{Window: 2 * time.Second, MinRequests: 5, FailureRatio: 0.5, Cooldown: time.Second}, c.now), c
}

func allow(t *testing.T, b *Breaker) Ticket {
	t.Helper()
	tk, ok, _ := b.Allow()
	if !ok {
		t.Fatal("breaker refused a call it should allow")
	}
	return tk
}

func record(t *testing.T, b *Breaker, c *fakeClock, results ...bool) {
	t.Helper()
	for _, ok := range results {
		b.Done(allow(t, b), ok)
		c.advance(10 * time.Millisecond)
	}
}

// tripAndCool opens the breaker and spends its cool-down.
func tripAndCool(t *testing.T, b *Breaker, c *fakeClock) {
	t.Helper()
	record(t, b, c, false, false, false, false, false)
	c.advance(time.Second)
}

func TestBreaker(t *testing.T) {
	t.Run("stays closed below the minimum number of calls", func(t *testing.T) {
		b, c := newTest()
		record(t, b, c, false, false, false, false)
		if b.State() != Closed {
			t.Fatalf("state %s after 4 failures with min 5", b.State())
		}
	})

	t.Run("opens at the failure ratio and refuses until the cool-down ends", func(t *testing.T) {
		b, c := newTest()
		record(t, b, c, true, false, true, false, false)
		if b.State() != Open {
			t.Fatalf("state %s, want open at 3/5 failures", b.State())
		}
		_, ok, wait := b.Allow()
		if ok || wait <= 0 || wait > time.Second {
			t.Fatalf("open: Allow = %v, %s", ok, wait)
		}
		c.advance(wait - time.Millisecond)
		if _, ok, _ := b.Allow(); ok {
			t.Fatal("allowed before the cool-down ended")
		}
	})

	t.Run("does not open below the ratio", func(t *testing.T) {
		b, c := newTest()
		record(t, b, c, true, true, true, false, false, true)
		if b.State() != Closed {
			t.Fatalf("state %s at 2/6 failures", b.State())
		}
	})

	t.Run("old failures leave the window", func(t *testing.T) {
		b, c := newTest()
		record(t, b, c, false, false, false, false)
		c.advance(3 * time.Second)
		record(t, b, c, true, true, true, true, false)
		if b.State() != Closed {
			t.Fatalf("state %s: failures outside the window counted", b.State())
		}
	})

	t.Run("half-open lets exactly one probe through; success closes", func(t *testing.T) {
		b, c := newTest()
		tripAndCool(t, b, c)
		probe := allow(t, b)
		if !probe.Probe() {
			t.Fatal("the first call after the cool-down is not marked as the probe")
		}
		if _, ok, _ := b.Allow(); ok {
			t.Fatal("a second caller was allowed while the probe is out")
		}
		b.Done(probe, true)
		if b.State() != Closed {
			t.Fatalf("state %s after a good probe", b.State())
		}
		if _, ok, _ := b.Allow(); !ok {
			t.Fatal("closed breaker refuses")
		}
	})

	t.Run("a failed probe reopens for a full cool-down", func(t *testing.T) {
		b, c := newTest()
		tripAndCool(t, b, c)
		b.Done(allow(t, b), false)
		_, ok, wait := b.Allow()
		if ok || wait != time.Second {
			t.Fatalf("after a failed probe: Allow = %v, %s; want refused for 1s", ok, wait)
		}
	})

	t.Run("a cancelled probe hands the probe to the next caller", func(t *testing.T) {
		b, c := newTest()
		tripAndCool(t, b, c)
		b.Cancel(allow(t, b))
		if _, ok, _ := b.Allow(); !ok {
			t.Fatal("next caller may not probe after a cancel")
		}
	})

	t.Run("results of calls started before opening change nothing", func(t *testing.T) {
		b, c := newTest()
		early := allow(t, b)
		record(t, b, c, false, false, false, false, false)
		b.Done(early, true)
		if b.State() != Open {
			t.Fatalf("state %s: a late success closed an open breaker", b.State())
		}
	})

	t.Run("a late success from the closed generation does not decide half-open", func(t *testing.T) {
		b, c := newTest()
		early := allow(t, b) // admitted while closed, answers only after the probe started
		tripAndCool(t, b, c)
		probe := allow(t, b)
		b.Done(early, true)
		if b.State() != HalfOpen {
			t.Fatalf("state %s: the closed-generation result decided the half-open breaker", b.State())
		}
		b.Done(probe, false)
		if b.State() != Open {
			t.Fatalf("state %s: the probe's failure did not reopen", b.State())
		}
	})

	t.Run("a late failure from the closed generation does not reopen half-open", func(t *testing.T) {
		b, c := newTest()
		early := allow(t, b)
		tripAndCool(t, b, c)
		probe := allow(t, b)
		b.Done(early, false)
		b.Done(probe, true)
		if b.State() != Closed {
			t.Fatalf("state %s: want closed by the probe's success alone", b.State())
		}
	})

	t.Run("a stale cancel does not admit a second probe", func(t *testing.T) {
		b, c := newTest()
		early := allow(t, b)
		tripAndCool(t, b, c)
		probe := allow(t, b)
		b.Cancel(early)
		if _, ok, _ := b.Allow(); ok {
			t.Fatal("a cancel from the closed generation let a second probe through")
		}
		b.Done(probe, true)
		if b.State() != Closed {
			t.Fatalf("state %s after the probe succeeded", b.State())
		}
	})

	t.Run("a cancel after the probe answered changes nothing", func(t *testing.T) {
		b, c := newTest()
		tripAndCool(t, b, c)
		probe := allow(t, b)
		b.Done(probe, true)
		b.Cancel(probe)
		if b.State() != Closed {
			t.Fatalf("state %s: a cancel of an answered probe moved the breaker", b.State())
		}
	})
}
