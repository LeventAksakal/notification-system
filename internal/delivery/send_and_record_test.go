package delivery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/provider"
)

func TestWorkerRetryOutcomes(t *testing.T) {
	cases := []struct {
		name     string
		answers  []int // status per call; 0 = network error
		status   notification.Status
		kind     notification.FailureKind
		outcomes string
	}{
		{"transient twice then accepted", []int{503, 0, 202}, notification.StatusSent, "", "[transient transient accepted]"},
		{"permanent after one call", []int{400}, notification.StatusFailed, notification.FailurePermanent, "[permanent]"},
		{"exhausted after max attempts", []int{503, 503, 503}, notification.StatusFailed, notification.FailureExhausted, "[transient transient transient]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := newMemQueue()
			q.add(1, notification.ChannelPush, notification.PriorityHigh)
			var mu sync.Mutex
			calls := 0
			send := func(context.Context, provider.Message) (provider.Response, error) {
				mu.Lock()
				defer mu.Unlock()
				st := c.answers[min(calls, len(c.answers)-1)]
				calls++
				if st == 0 {
					return provider.Response{}, errors.New("connection reset")
				}
				return provider.Response{Status: st, Header: http.Header{}}, nil
			}
			stop := run(t, New(testConfig(), Deps{Queue: q, Limiter: testLimiter(), Sender: scriptSender{send}}))
			id := "push-high-0"
			waitFor(t, "a final status", func() bool {
				st := q.row(id).status
				return st == notification.StatusSent || st == notification.StatusFailed
			})
			if err := stop(); err != nil {
				t.Fatal(err)
			}
			r := q.row(id)
			q.mu.Lock()
			got := fmt.Sprint(q.results[id])
			q.mu.Unlock()
			if r.status != c.status || r.kind != c.kind || got != c.outcomes {
				t.Errorf("got %s/%s %s, want %s/%s %s", r.status, r.kind, got, c.status, c.kind, c.outcomes)
			}
		})
	}
}
