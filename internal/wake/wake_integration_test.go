//go:build integration

package wake_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"go.uber.org/goleak"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/wake"
	"github.com/LeventAksakal/notification-system/test/support"
)

// listening runs a Listener for channels and reports what it hears.
type listening struct {
	hints      chan notification.Wake
	subscribed chan struct{}
	stop       func()
}

func listen(t *testing.T, client valkey.Client, channels ...notification.Channel) *listening {
	t.Helper()
	l := &listening{hints: make(chan notification.Wake, 16), subscribed: make(chan struct{}, 16)}
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = wake.NewListener(client).Run(ctx, channels,
			func(h notification.Wake) { l.hints <- h },
			func() { l.subscribed <- struct{}{} })
	})
	l.stop = func() { cancel(); wg.Wait() }
	return l
}

func (l *listening) waitSubscribed(t *testing.T) {
	t.Helper()
	select {
	case <-l.subscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("not subscribed within 5 s")
	}
}

func newClient(t *testing.T, addr string) valkey.Client {
	t.Helper()
	c, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAPublishedHintReachesAListenerOfItsChannelOnly(t *testing.T) {
	addr := support.Valkey(t)
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent()) // after the client below is closed
	client := newClient(t, addr)
	defer client.Close()

	l := listen(t, client, notification.ChannelSMS, notification.ChannelPush)
	defer l.stop()
	l.waitSubscribed(t)

	due := time.Now().Add(3 * time.Second).Truncate(time.Millisecond)
	p := wake.NewPublisher(client)
	err := p.Wake(t.Context(),
		notification.Wake{Channel: notification.ChannelEmail, Priority: notification.PriorityHigh, Due: due}, // not served
		notification.Wake{Channel: notification.ChannelSMS, Priority: notification.PriorityLow, Due: due.Add(time.Second)},
		notification.Wake{Channel: notification.ChannelSMS, Priority: notification.PriorityLow, Due: due},
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case h := <-l.hints:
		want := notification.Wake{Channel: notification.ChannelSMS, Priority: notification.PriorityLow, Due: due}
		if h.Channel != want.Channel || h.Priority != want.Priority || !h.Due.Equal(want.Due) {
			t.Fatalf("got %+v, want %+v (the earliest of the two)", h, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no hint within 5 s")
	}
	select {
	case h := <-l.hints:
		t.Fatalf("want one hint (coalesced, email not served), got another: %+v", h)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestAListenerResubscribesAfterItsConnectionIsKilledWithoutLooping(t *testing.T) {
	addr := support.Valkey(t)
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent()) // after the client below is closed
	client := newClient(t, addr)
	defer client.Close()

	l := listen(t, client, notification.ChannelEmail)
	defer l.stop()
	l.waitSubscribed(t)
	kill := func() {
		t.Helper()
		if err := client.Do(t.Context(), client.B().ClientKill().TypePubsub().Build()).Error(); err != nil {
			t.Fatalf("client kill: %v", err)
		}
	}

	// A subscription that held for a safety poll is retried at once.
	time.Sleep(wake.SafetyPoll + 100*time.Millisecond) // setup: let it hold
	kill()
	select {
	case <-l.subscribed:
	case <-time.After(wake.SafetyPoll):
		t.Fatalf("not resubscribed within %s after a subscription that held", wake.SafetyPoll)
	}
	// One dropped right after its SUBSCRIBE waits a safety poll: a server
	// that keeps dropping it costs one resubscribe per poll, not a loop.
	kill()
	lost := time.Now()
	select {
	case <-l.subscribed:
		if took := time.Since(lost); took < wake.SafetyPoll/2 {
			t.Fatalf("resubscribed %s after a subscription that held under a safety poll; want about %s", took, wake.SafetyPoll)
		}
	case <-time.After(3 * wake.SafetyPoll):
		t.Fatalf("not resubscribed within %s", 3*wake.SafetyPoll)
	}
	if err := wake.NewPublisher(client).Wake(t.Context(),
		notification.Wake{Channel: notification.ChannelEmail, Priority: notification.PriorityNormal, Due: time.Now()}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.hints:
	case <-time.After(5 * time.Second):
		t.Fatal("no hint after resubscribing")
	}
}
