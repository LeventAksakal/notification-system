package realtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/core"
)

var (
	// ErrSlow ends a subscription whose buffer was full when an event came:
	// the hub never waits for a subscriber.
	ErrSlow = errors.New("subscriber too slow; events were dropped")
	// ErrClosed ends a subscription because the hub stopped.
	ErrClosed = errors.New("event hub stopped")
	// ErrFeedLost ends every subscription when the hub's Valkey subscription
	// drops (changes in the gap would be missed), and refuses new ones until
	// it is back.
	ErrFeedLost = errors.New("status event feed lost; resubscribe and read the snapshot")
)

const (
	// holdFor bounds how long a claim waits for the pending it follows
	// (see sequencer); the race it covers lasts milliseconds.
	holdFor = 200 * time.Millisecond
	// rememberFor is how long the hub remembers a notification's last state.
	rememberFor = 10 * time.Minute
	// tickEvery is how often held events are checked for expiry.
	tickEvery = 25 * time.Millisecond
	// retryMax caps the wait between resubscribe attempts.
	retryMax = 5 * time.Second
)

// Filter selects the events a subscription receives; a zero Filter takes all.
type Filter struct {
	NotificationID string
	BatchID        string
}

func (f Filter) match(e core.Event) bool {
	return (f.NotificationID == "" || f.NotificationID == e.NotificationID) &&
		(f.BatchID == "" || f.BatchID == e.BatchID)
}

// Subscription receives the events matching its filter, in commit order per
// notification. C is closed when the subscription ends; Err then says why.
type Subscription struct {
	C <-chan core.Event

	ch     chan core.Event
	filter Filter
	hub    *Hub
	err    error // set under hub.mu before ch is closed
}

// Err is why C was closed: ErrSlow, ErrFeedLost, ErrClosed, or nil after Close.
func (s *Subscription) Err() error {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	return s.err
}

// Close ends the subscription; safe to call more than once.
func (s *Subscription) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	s.hub.drop(s, nil)
}

// Hub holds one Valkey subscription per process and fans events out to local
// subscribers. One mutex guards the subscribers and the ordering state.
type Hub struct {
	client  valkey.Client
	channel string
	now     func() time.Time
	live    atomic.Bool

	mu     sync.Mutex
	subs   map[*Subscription]struct{}
	seq    *sequencer
	closed bool
}

// NewHub returns a hub that listens on Channel through client once Run starts.
func NewHub(client valkey.Client) *Hub {
	return &Hub{
		client: client, channel: Channel, now: time.Now,
		subs: map[*Subscription]struct{}{}, seq: newSequencer(holdFor, rememberFor),
	}
}

// Live reports whether the Valkey subscription is active: events published
// now reach this hub's subscribers.
func (h *Hub) Live() bool { return h.live.Load() }

// Subscribe registers a subscriber with a buffer of buf events. Every event
// published after it returns is delivered, or the subscription ends: it is
// refused with ErrFeedLost while the hub is not live.
func (h *Hub) Subscribe(f Filter, buf int) (*Subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	if !h.live.Load() {
		return nil, ErrFeedLost
	}
	ch := make(chan core.Event, max(buf, 1))
	s := &Subscription{C: ch, ch: ch, filter: f, hub: h}
	h.subs[s] = struct{}{}
	return s, nil
}

// Run keeps the Valkey subscription up until ctx ends, resubscribing after a
// failure, then ends every local subscription with ErrClosed.
func (h *Hub) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // owned by Run through wg
		defer wg.Done()
		h.tick(ctx)
	}()
	wait := 100 * time.Millisecond
	for {
		started := h.now()
		err := h.listen(ctx)
		if ctx.Err() != nil {
			break
		}
		h.feedLost(ctx)
		if h.now().Sub(started) > retryMax {
			wait = 100 * time.Millisecond
		}
		slog.WarnContext(ctx, "status event subscription lost; resubscribing", "err", err, "retry_in", wait.String())
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
		case <-t.C:
		}
		t.Stop()
		if ctx.Err() != nil {
			break
		}
		wait = min(wait*2, retryMax)
	}
	wg.Wait()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for s := range h.subs {
		h.drop(s, ErrClosed)
	}
	return nil
}

// feedLost ends every subscription with ErrFeedLost: events published while
// the subscription was down are gone, and a stream with holes is worse than a
// closed one (the client reconnects and reads the snapshot). What was held
// for ordering is dropped with them.
func (h *Hub) feedLost(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.live.Store(false)
	h.seq = newSequencer(holdFor, rememberFor)
	n := len(h.subs)
	for s := range h.subs {
		h.drop(s, ErrFeedLost)
	}
	if n > 0 {
		slog.WarnContext(ctx, "status event feed lost; closed every stream", "streams", n)
	}
}

// listen holds one subscription until it fails or ctx ends.
func (h *Hub) listen(ctx context.Context) error {
	dc, release := h.client.Dedicate()
	defer release()
	defer dc.Close() // the connection carries the subscription; never reuse it
	defer func() {
		h.mu.Lock() // Subscribe reads live under h.mu: none slips in unended
		h.live.Store(false)
		h.mu.Unlock()
	}()
	gone := dc.SetPubSubHooks(valkey.PubSubHooks{
		OnMessage: func(m valkey.PubSubMessage) { h.receive(ctx, m.Message) },
		OnSubscription: func(s valkey.PubSubSubscription) {
			if s.Kind == "subscribe" && s.Channel == h.channel {
				h.live.Store(true)
			}
		},
	})
	if err := dc.Do(ctx, dc.B().Subscribe().Channel(h.channel).Build()).Error(); err != nil {
		return fmt.Errorf("subscribe %s: %w", h.channel, err)
	}
	h.live.Store(true) // SUBSCRIBE answered: the server now forwards publishes
	select {
	case err := <-gone:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (h *Hub) receive(ctx context.Context, payload string) {
	m, err := decode([]byte(payload))
	if err != nil {
		slog.WarnContext(ctx, "dropping malformed status event", "err", err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deliver(h.seq.push(m, h.now()))
}

func (h *Hub) tick(ctx context.Context) {
	t := time.NewTicker(tickEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		h.mu.Lock()
		h.deliver(h.seq.expire(h.now()))
		h.mu.Unlock()
	}
}

// deliver hands msgs to every matching subscriber without waiting; a
// subscriber with a full buffer is ended with ErrSlow. Caller holds h.mu.
func (h *Hub) deliver(msgs []Message) {
	if h.closed {
		return
	}
	for _, m := range msgs {
		for s := range h.subs {
			if !s.filter.match(m.Event) {
				continue
			}
			select {
			case s.ch <- m.Event:
			default:
				h.drop(s, ErrSlow)
			}
		}
	}
}

// drop ends s with err if it is still registered. Caller holds h.mu.
func (h *Hub) drop(s *Subscription, err error) {
	if _, ok := h.subs[s]; !ok {
		return
	}
	delete(h.subs, s)
	s.err = err
	close(s.ch)
}
