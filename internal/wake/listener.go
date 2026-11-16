package wake

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// Listener holds one subscription to the wake channels of the channels a
// worker serves.
type Listener struct {
	client valkey.Client
}

// NewListener subscribes through client once Run starts.
func NewListener(client valkey.Client) *Listener { return &Listener{client: client} }

// Run keeps the subscription up until ctx ends. Each hint received goes to
// onHint; after every successful (re)subscribe onSubscribed is called, so the
// worker claims at once whatever was hinted while the subscription was down.
// A subscription lost after it held for SafetyPoll is retried at once; any
// other retry waits SafetyPoll: retrying more often gains nothing, as the
// safety poll finds the rows by then anyway, and a server that accepts each
// SUBSCRIBE and drops it at once so costs one onSubscribed per SafetyPoll,
// not a tight loop.
// Both callbacks run on the subscription's goroutine and must not block.
func (l *Listener) Run(ctx context.Context, channels []notification.Channel, onHint func(notification.Wake), onSubscribed func()) error {
	names := make([]string, len(channels))
	byName := make(map[string]notification.Channel, len(channels))
	for i, ch := range channels {
		names[i] = channelName(ch)
		byName[names[i]] = ch
	}
	for {
		var subscribed time.Time
		err := l.listen(ctx, names, func(name, payload string) {
			h, err := decode(byName[name], []byte(payload))
			if err != nil {
				slog.WarnContext(ctx, "dropping malformed wake hint", "channel", name, "err", err)
				return
			}
			onHint(h)
		}, func() {
			subscribed = time.Now()
			onSubscribed()
		})
		select {
		case <-ctx.Done():
			return nil // stopped, not failed
		default:
		}
		wait := SafetyPoll
		if !subscribed.IsZero() && time.Since(subscribed) >= SafetyPoll {
			wait = 0
		}
		slog.WarnContext(ctx, "wake hint subscription lost; resubscribing", "err", err, "retry_in", wait.String())
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
		case <-t.C:
		}
		t.Stop()
	}
}

// listen holds one subscription until it fails or ctx ends.
func (l *Listener) listen(ctx context.Context, names []string, onMessage func(name, payload string), onSubscribed func()) error {
	dc, release := l.client.Dedicate()
	defer release()
	defer dc.Close() // the connection carries the subscription; never reuse it
	gone := dc.SetPubSubHooks(valkey.PubSubHooks{
		OnMessage: func(m valkey.PubSubMessage) { onMessage(m.Channel, m.Message) },
	})
	if err := dc.Do(ctx, dc.B().Subscribe().Channel(names...).Build()).Error(); err != nil {
		return fmt.Errorf("subscribe %v: %w", names, err)
	}
	onSubscribed() // SUBSCRIBE answered: the server now forwards publishes
	select {
	case err := <-gone:
		return err
	case <-ctx.Done():
		return nil
	}
}
