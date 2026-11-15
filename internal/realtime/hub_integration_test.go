//go:build integration

package realtime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"go.uber.org/goleak"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/realtime"
	"github.com/LeventAksakal/notification-system/internal/realtime/realtimetest"
	"github.com/LeventAksakal/notification-system/test/support"
)

func client(t *testing.T, addr string) valkey.Client {
	t.Helper()
	c, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// startHub runs a hub until the returned stop is called and waits until it is live.
func startHub(t *testing.T, c valkey.Client) (*realtime.Hub, func()) {
	t.Helper()
	h := realtime.NewHub(c)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = h.Run(ctx)
	}()
	stop := func() { cancel(); wg.Wait() }
	deadline := time.Now().Add(10 * time.Second)
	for !h.Live() {
		if time.Now().After(deadline) {
			stop()
			t.Fatal("hub not live within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return h, stop
}

func next(t *testing.T, s *realtime.Subscription) notification.Event {
	t.Helper()
	select {
	case e, ok := <-s.C:
		if !ok {
			t.Fatalf("subscription ended: %v", s.Err())
		}
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("no event within 5s")
	}
	return notification.Event{}
}

func TestHubFansOutPublishedEventsByFilter(t *testing.T) {
	addr := support.Valkey(t)
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	pubClient, subClient := client(t, addr), client(t, addr)
	defer pubClient.Close()
	defer subClient.Close()
	h, stop := startHub(t, subClient)
	defer stop()

	all, _ := h.Subscribe(realtime.Filter{}, 16)
	one, _ := h.Subscribe(realtime.Filter{NotificationID: "n2"}, 16)
	batch, _ := h.Subscribe(realtime.Filter{BatchID: "b1"}, 16)
	api := realtime.NewPublisher(pubClient, realtime.OriginAPI)
	now := time.Now().UTC()
	for _, e := range []notification.Event{
		{NotificationID: "n1", BatchID: "b1", Status: notification.StatusPending, At: now, CorrelationID: "c1"},
		{NotificationID: "n2", Status: notification.StatusPending, At: now, CorrelationID: "c2"},
	} {
		if err := api.Publish(t.Context(), e); err != nil {
			t.Fatal(err)
		}
	}
	if a, b := next(t, all), next(t, all); a.NotificationID != "n1" || b.NotificationID != "n2" {
		t.Fatalf("unfiltered: want n1 then n2, got %v %v", a, b)
	}
	if e := next(t, one); e.NotificationID != "n2" || e.CorrelationID != "c2" || !e.At.Equal(now) {
		t.Fatalf("by id: got %+v", e)
	}
	if e := next(t, batch); e.NotificationID != "n1" || e.BatchID != "b1" {
		t.Fatalf("by batch: got %+v", e)
	}
}

func TestHubDropsASlowSubscriberWithoutBlockingOthers(t *testing.T) {
	addr := support.Valkey(t)
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	pubClient, subClient := client(t, addr), client(t, addr)
	defer pubClient.Close()
	defer subClient.Close()
	h, stop := startHub(t, subClient)
	defer stop()

	slow, _ := h.Subscribe(realtime.Filter{}, 2)
	fast, _ := h.Subscribe(realtime.Filter{}, 64)
	pub := realtime.NewPublisher(pubClient, realtime.OriginAPI)
	for i := range 5 {
		e := notification.Event{NotificationID: "n" + string(rune('a'+i)), Status: notification.StatusPending, At: time.Now()}
		if err := pub.Publish(t.Context(), e); err != nil {
			t.Fatal(err)
		}
	}
	for range 5 {
		next(t, fast)
	}
	n := 0
	for range slow.C {
		n++
	}
	if n != 2 || !errors.Is(slow.Err(), realtime.ErrSlow) {
		t.Fatalf("slow subscriber: want 2 events then ErrSlow, got %d and %v", n, slow.Err())
	}
}

func TestHubEndsSubscriptionsWhenItStops(t *testing.T) {
	addr := support.Valkey(t)
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	subClient := client(t, addr)
	defer subClient.Close()
	h, stop := startHub(t, subClient)
	s, _ := h.Subscribe(realtime.Filter{}, 4)
	stop()
	if _, ok := <-s.C; ok || !errors.Is(s.Err(), realtime.ErrClosed) {
		t.Fatalf("want closed with ErrClosed, got %v", s.Err())
	}
	if _, err := h.Subscribe(realtime.Filter{}, 4); !errors.Is(err, realtime.ErrClosed) {
		t.Fatalf("subscribe after stop: want ErrClosed, got %v", err)
	}
}

// CONTRACT "Events": an API instance that loses its event feed closes its
// streams and refuses new ones until the feed is back.
func TestHubEndsStreamsWhenItsFeedDropsAndRefusesUntilBack(t *testing.T) {
	addr := support.Valkey(t)
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	proxy := realtimetest.NewProxy(t, addr)
	defer proxy.Close() // before goleak checks (t.Cleanup runs later)
	pubClient, subClient := client(t, addr), client(t, proxy.Addr)
	defer pubClient.Close()
	defer subClient.Close()
	h, stop := startHub(t, subClient)
	defer stop()

	open, err := h.Subscribe(realtime.Filter{}, 16)
	if err != nil {
		t.Fatal(err)
	}
	proxy.Close()

	select {
	case _, ok := <-open.C:
		if ok {
			t.Fatal("an event arrived after the feed was cut")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("open subscription still running 5s after the feed dropped")
	}
	if !errors.Is(open.Err(), realtime.ErrFeedLost) {
		t.Fatalf("ended with %v, want ErrFeedLost", open.Err())
	}
	if h.Live() {
		t.Fatal("hub reports live without its feed")
	}
	if _, err := h.Subscribe(realtime.Filter{}, 16); !errors.Is(err, realtime.ErrFeedLost) {
		t.Fatalf("subscribe without a feed: got %v, want ErrFeedLost", err)
	}

	proxy.Reopen(t)
	deadline := time.Now().Add(15 * time.Second)
	for !h.Live() {
		if time.Now().After(deadline) {
			t.Fatal("hub not live 15s after the feed came back")
		}
		time.Sleep(20 * time.Millisecond)
	}
	again, err := h.Subscribe(realtime.Filter{}, 16)
	if err != nil {
		t.Fatalf("subscribe after the feed came back: %v", err)
	}
	defer again.Close()
	e := notification.Event{NotificationID: "back", Status: notification.StatusPending, At: time.Now().UTC()}
	if err := realtime.NewPublisher(pubClient, realtime.OriginAPI).Publish(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if got := next(t, again); got.NotificationID != "back" {
		t.Fatalf("after recovery: got %+v", got)
	}
}
