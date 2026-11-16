package delivery

import (
	"errors"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func TestWorkerSendsNothingWhenTheLimiterIsDown(t *testing.T) {
	q := newMemQueue()
	q.add(5, notification.ChannelSMS, notification.PriorityHigh)
	lim := testLimiter()
	lim.err = errors.New("valkey down")
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: lim, Sender: scriptSender{accept}}))
	waitFor(t, "the limiter is asked", func() bool { lim.mu.Lock(); defer lim.mu.Unlock(); return lim.takes > 3 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if n := q.count(notification.StatusPending); n != 5 {
		t.Errorf("want all 5 pending (nothing claimed), got %d", n)
	}
}
