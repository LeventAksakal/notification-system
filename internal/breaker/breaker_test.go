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

func record(b *Breaker, c *fakeClock, results ...bool) {
	for _, ok := range results {
		if allowed, _ := b.Allow(); !allowed {
			panic("record on a breaker that does not allow")
		}
		b.Done(ok)
		c.advance(10 * time.Millisecond)
	}
}

func TestBreaker(t *testing.T) {
	t.Run("stays closed below the minimum number of calls", func(t *testing.T) {
		b, c := newTest()
		record(b, c, false, false, false, false)
		if b.State() != Closed {
			t.Fatalf("state %s after 4 failures with min 5", b.State())
		}
	})

	t.Run("opens at the failure ratio and refuses until the cool-down ends", func(t *testing.T) {
		b, c := newTest()
		record(b, c, true, false, true, false, false)
		if b.State() != Open {
			t.Fatalf("state %s, want open at 3/5 failures", b.State())
		}
		ok, wait := b.Allow()
		if ok || wait <= 0 || wait > time.Second {
			t.Fatalf("open: Allow = %v, %s", ok, wait)
		}
		c.advance(wait - time.Millisecond)
		if ok, _ := b.Allow(); ok {
			t.Fatal("allowed before the cool-down ended")
		}
	})

	t.Run("does not open below the ratio", func(t *testing.T) {
		b, c := newTest()
		record(b, c, true, true, true, false, false, true)
		if b.State() != Closed {
			t.Fatalf("state %s at 2/6 failures", b.State())
		}
	})

	t.Run("old failures leave the window", func(t *testing.T) {
		b, c := newTest()
		record(b, c, false, false, false, false)
		c.advance(3 * time.Second)
		record(b, c, true, true, true, true, false)
		if b.State() != Closed {
			t.Fatalf("state %s: failures outside the window counted", b.State())
		}
	})

	t.Run("half-open lets exactly one probe through; success closes", func(t *testing.T) {
		b, c := newTest()
		record(b, c, false, false, false, false, false)
		c.advance(time.Second)
		if ok, _ := b.Allow(); !ok {
			t.Fatal("probe refused after the cool-down")
		}
		if ok, _ := b.Allow(); ok {
			t.Fatal("a second caller was allowed while the probe is out")
		}
		b.Done(true)
		if b.State() != Closed {
			t.Fatalf("state %s after a good probe", b.State())
		}
		if ok, _ := b.Allow(); !ok {
			t.Fatal("closed breaker refuses")
		}
	})

	t.Run("a failed probe reopens for a full cool-down", func(t *testing.T) {
		b, c := newTest()
		record(b, c, false, false, false, false, false)
		c.advance(time.Second)
		_, _ = b.Allow()
		b.Done(false)
		ok, wait := b.Allow()
		if ok || wait != time.Second {
			t.Fatalf("after a failed probe: Allow = %v, %s; want refused for 1s", ok, wait)
		}
	})

	t.Run("a cancelled probe hands the probe to the next caller", func(t *testing.T) {
		b, c := newTest()
		record(b, c, false, false, false, false, false)
		c.advance(time.Second)
		_, _ = b.Allow()
		b.Cancel()
		if ok, _ := b.Allow(); !ok {
			t.Fatal("next caller may not probe after a cancel")
		}
	})

	t.Run("results of calls started before opening change nothing", func(t *testing.T) {
		b, c := newTest()
		record(b, c, false, false, false, false, false)
		b.Done(true)
		if b.State() != Open {
			t.Fatalf("state %s: a late success closed an open breaker", b.State())
		}
	})
}
