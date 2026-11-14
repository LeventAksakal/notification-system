// Package realtime carries committed status changes from the processes that
// make them (API and workers) to the API instances that hold WebSocket
// subscribers, over one Valkey pub/sub channel (decision 0013).
//
// Publishing is best effort and happens after commit; Valkey pub/sub may drop
// a message while a subscriber reconnects, so the REST API and the snapshot a
// notificationId subscription starts with stay the source of truth.
package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/core"
)

// Channel is the Valkey pub/sub channel every status change is published on.
const Channel = "notifier:status-events"

const (
	// publishTimeout bounds one publish call (one event, or a whole batch as
	// one pipeline), so an unreachable Valkey delays a commit's caller by at
	// most this much.
	publishTimeout = time.Second
	// publishHoldOff is how long publishes fail at once after one failed:
	// during an outage only the first change of each hold-off waits.
	publishHoldOff = 500 * time.Millisecond
)

// Origin says which kind of process published a change. The hub uses it to
// put a create or replay (published by the API) before the worker's claim of
// the same notification when the two arrive the other way round.
type Origin string

// The publishing roles.
const (
	OriginAPI    Origin = "api"
	OriginWorker Origin = "worker"
)

// Message is one status change as it travels between processes.
type Message struct {
	core.Event
	Origin Origin
}

type wireMessage struct {
	NotificationID string    `json:"notificationId"`
	BatchID        string    `json:"batchId,omitempty"`
	Status         string    `json:"status"`
	Attempts       int       `json:"attempts"`
	Error          string    `json:"error,omitempty"`
	At             time.Time `json:"at"`
	CorrelationID  string    `json:"correlationId"`
	Origin         Origin    `json:"origin"`
}

func encode(m Message) ([]byte, error) {
	return json.Marshal(wireMessage{
		NotificationID: m.NotificationID, BatchID: m.BatchID, Status: string(m.Status), Attempts: m.Attempts,
		Error: m.Error, At: m.At, CorrelationID: m.CorrelationID, Origin: m.Origin,
	})
}

func decode(b []byte) (Message, error) {
	var w wireMessage
	if err := json.Unmarshal(b, &w); err != nil {
		return Message{}, fmt.Errorf("decode status event: %w", err)
	}
	if w.NotificationID == "" || !core.Status(w.Status).Valid() {
		return Message{}, fmt.Errorf("decode status event: id %q status %q", w.NotificationID, w.Status)
	}
	return Message{Origin: w.Origin, Event: core.Event{
		NotificationID: w.NotificationID, BatchID: w.BatchID, Status: core.Status(w.Status), Attempts: w.Attempts,
		Error: w.Error, At: w.At, CorrelationID: w.CorrelationID,
	}}, nil
}

// ErrUnavailable is returned without trying while a recent publish failed:
// Valkey is taken as down for publishHoldOff, so a burst of status changes
// during an outage costs one bounded wait, not one per change.
var ErrUnavailable = errors.New("status events: valkey unavailable")

// Publisher is the core.Publisher that sends each change to Valkey.
type Publisher struct {
	client  valkey.Client
	channel string
	origin  Origin
	now     func() time.Time
	// downUntil (unix nanoseconds) is the end of the hold-off after a failure.
	downUntil atomic.Int64
}

var _ core.Publisher = (*Publisher)(nil)

// NewPublisher publishes on Channel through client, marking messages with origin.
func NewPublisher(client valkey.Client, origin Origin) *Publisher {
	return &Publisher{client: client, channel: Channel, origin: origin, now: time.Now}
}

// Publish sends e synchronously, so changes published one after another by
// one goroutine reach Valkey, and every subscriber, in that order. It ignores
// the caller's cancellation (the change is committed; a client that went away
// does not make it unannounced) but not its values.
func (p *Publisher) Publish(ctx context.Context, e core.Event) error {
	return p.PublishAll(ctx, []core.Event{e})
}

// PublishAll sends es in order as one pipeline under one deadline of
// publishTimeout, whatever their number. After a failure, calls fail at once
// with ErrUnavailable for publishHoldOff.
func (p *Publisher) PublishAll(ctx context.Context, es []core.Event) error {
	if len(es) == 0 {
		return nil
	}
	if p.now().UnixNano() < p.downUntil.Load() {
		return fmt.Errorf("publish %d status events: %w", len(es), ErrUnavailable)
	}
	cmds := make(valkey.Commands, 0, len(es))
	for _, e := range es {
		b, err := encode(Message{Event: e, Origin: p.origin})
		if err != nil {
			return err
		}
		cmds = append(cmds, p.client.B().Publish().Channel(p.channel).Message(string(b)).Build())
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	failed := 0
	var first error
	for _, r := range p.client.DoMulti(ctx, cmds...) {
		if err := r.Error(); err != nil {
			failed++
			if first == nil {
				first = err
			}
		}
	}
	if first != nil {
		p.downUntil.Store(p.now().Add(publishHoldOff).UnixNano())
		return fmt.Errorf("publish status events (%d of %d failed): %w", failed, len(es), first)
	}
	return nil
}
