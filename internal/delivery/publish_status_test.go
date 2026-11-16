package delivery

import (
	"fmt"
	"slices"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// The processing publish does not return until every job has been sent: were
// it on the claim path, nothing would be sent at all, whatever the timing. A
// timer woken late on a busy machine may still give a slot up and release a
// job (correct, and not what this test is about); the job is then claimed and
// sent again, so the test does not count releases. Each result must wait for
// its own processing event: nothing is published while that is held, and once
// it is let through no job has more results than processing events at any
// point of its event sequence. (A released job's pending event and its next
// claim's events wait for different publishes, so their mutual order is not
// asserted.)
func TestWorkerSlowPublishCostsNoSlotAndKeepsEventOrder(t *testing.T) {
	q := newMemQueue()
	q.add(12, notification.ChannelSMS, notification.PriorityHigh)
	pub := newGatedPublisher()
	defer pub.open()
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}, Publisher: pub}))
	waitFor(t, "all 12 sent while their processing publish does not return", func() bool {
		return q.count(notification.StatusSent) == 12
	})
	pub.mu.Lock()
	early := fmt.Sprint(pub.events)
	n := len(pub.events)
	pub.mu.Unlock()
	if n != 0 {
		t.Errorf("published before the processing publish returned: %s, want nothing", early)
	}
	pub.open()
	ids := make([]string, 12)
	for i := range ids {
		ids[i] = fmt.Sprintf("sms-high-%d", i)
	}
	// balance is how many processing events of id have no result yet; -1 when
	// a result came before its processing event.
	balance := func(got []notification.Status) int {
		n := 0
		for _, st := range got {
			if st == notification.StatusProcessing {
				n++
			} else if n--; n < 0 {
				return -1
			}
		}
		return n
	}
	waitFor(t, "every result published", func() bool {
		for _, id := range ids {
			got := pub.statuses(id)
			if !slices.Contains(got, notification.StatusSent) || balance(got) > 0 {
				return false
			}
		}
		return true
	})
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if got := pub.statuses(id); balance(got) != 0 || got[0] != notification.StatusProcessing {
			t.Errorf("%s events: %v, want each result after a processing event of its own", id, got)
		}
	}
}
