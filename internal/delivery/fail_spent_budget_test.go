package delivery

import (
	"context"
	"sync"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

func TestWorkerFailsANotificationWhoseBudgetUnknownCallsSpent(t *testing.T) {
	q := newMemQueue()
	q.add(1, notification.ChannelPush, notification.PriorityHigh)
	id := "push-high-0"
	budget := testConfig().MaxAttempts
	q.rows[id].job.Attempts, q.rows[id].job.BudgetSpent = budget+2, budget // the reaper counted unknown calls
	var mu sync.Mutex
	calls := 0
	send := func(context.Context, provider.Message) (provider.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return provider.Response{Status: 202, MessageID: "m"}, nil
	}
	stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{send}}))
	waitFor(t, "a final status", func() bool { return q.row(id).status == notification.StatusFailed })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if r := q.row(id); calls != 0 || r.kind != notification.FailureExhausted || r.job.Attempts != budget+2 || r.job.BudgetSpent != budget {
		t.Errorf("got %d calls, %s after %d attempts (%d counted); want no call, exhausted after %d (%d counted)",
			calls, r.kind, r.job.Attempts, r.job.BudgetSpent, budget+2, budget)
	}
}
