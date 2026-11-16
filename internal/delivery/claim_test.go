package delivery

import (
	"testing"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func TestWorkerWeightsAndBorrowing(t *testing.T) {
	t.Run("all priorities busy: shares follow the weights", func(t *testing.T) {
		q := newMemQueue()
		for _, p := range notification.Priorities {
			q.add(400, notification.ChannelSMS, p)
		}
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}}))
		waitFor(t, "400 sent", func() bool { return q.count(notification.StatusSent) >= 400 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		h, n, l := q.claims[notification.PriorityHigh], q.claims[notification.PriorityNormal], q.claims[notification.PriorityLow]
		q.mu.Unlock()
		total := float64(h + n + l)
		if hs := float64(h) / total; hs < 0.65 || hs > 0.75 {
			t.Errorf("high share %.2f, want about 0.70 (h=%d n=%d l=%d)", hs, h, n, l)
		}
		if ls := float64(l) / total; ls < 0.07 || ls > 0.13 {
			t.Errorf("low share %.2f, want about 0.10 (h=%d n=%d l=%d)", ls, h, n, l)
		}
	})

	t.Run("normal empty: its share is lent down to low", func(t *testing.T) {
		q := newMemQueue()
		q.add(400, notification.ChannelSMS, notification.PriorityHigh)
		q.add(400, notification.ChannelSMS, notification.PriorityLow)
		stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}}))
		waitFor(t, "300 sent", func() bool { return q.count(notification.StatusSent) >= 300 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		h, l := q.claims[notification.PriorityHigh], q.claims[notification.PriorityLow]
		q.mu.Unlock()
		if ls := float64(l) / float64(h+l); ls < 0.25 || ls > 0.35 {
			t.Errorf("low share %.2f, want about 0.30 (low's 10 plus normal's 20)", ls)
		}
	})

	// A claim that runs short lends its unused slots on, but not to a
	// priority never answered for: each such ask is one more claim query in
	// the same claim, which tripled the claim time while the database was
	// slow (owner item 0011). NextDue answers those priorities instead.
	onlyHigh := [3]int{100, 0, 0}
	t.Run("a short claim does not query priorities never answered for", func(t *testing.T) {
		q := newMemQueue()
		q.add(5, notification.ChannelSMS, notification.PriorityHigh)
		cfg := testConfig()
		cfg.Weights = onlyHigh
		stop := run(t, New(cfg, Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}}))
		waitFor(t, "5 sent", func() bool { return q.count(notification.StatusSent) == 5 })
		waitFor(t, "the priorities without weight looked up", func() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.peeks > 0 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		n, l := q.asks[notification.PriorityNormal], q.asks[notification.PriorityLow]
		q.mu.Unlock()
		if n != 0 || l != 0 {
			t.Errorf("claim queries for normal %d, low %d with nothing due there; want none", n, l)
		}
	})

	t.Run("rows of a priority without weight are still sent", func(t *testing.T) {
		q := newMemQueue()
		q.add(5, notification.ChannelSMS, notification.PriorityHigh)
		q.add(20, notification.ChannelSMS, notification.PriorityNormal)
		cfg := testConfig()
		cfg.Weights = onlyHigh
		stop := run(t, New(cfg, Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}}))
		waitFor(t, "25 sent", func() bool { return q.count(notification.StatusSent) == 25 })
		if err := stop(); err != nil {
			t.Fatal(err)
		}
	})
}
