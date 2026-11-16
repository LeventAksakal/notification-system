package delivery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// slowFirstClaim makes the first claim take delay, longer than the group's
// slots last, so its rows come back with no slot left.
type slowFirstClaim struct {
	*memQueue
	delay time.Duration
	once  sync.Once
}

func (q *slowFirstClaim) Claim(ctx context.Context, ch notification.Channel, p notification.Priority, n int, lease time.Duration) ([]notification.Job, error) {
	q.once.Do(func() { sleep(ctx, q.delay) })
	return q.memQueue.Claim(ctx, ch, p, n, lease)
}

// Rows a claim brought back too late for its slots are released off the claim
// path: their release event waits for the processing publish, the channel's
// next claim does not, so the rows are claimed again and sent while that
// publish still has not returned.
func TestWorkerReleaseOfAnOutlastedClaimDoesNotWaitForThePublish(t *testing.T) {
	mq := newMemQueue()
	mq.add(12, notification.ChannelSMS, notification.PriorityHigh)
	q := &slowFirstClaim{memQueue: mq, delay: 100 * time.Millisecond} // slots span 20 ms
	pub := newGatedPublisher()
	defer pub.open()
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{accept}, Publisher: pub}))
	waitFor(t, "all 12 sent while the processing publish does not return", func() bool {
		return mq.count(notification.StatusSent) == 12
	})
	mq.mu.Lock()
	released := mq.released
	mq.mu.Unlock()
	if released == 0 {
		t.Error("no row released: the first claim did not outlast its slots, the test proves nothing")
	}
	pub.open()
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}
