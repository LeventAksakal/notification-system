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
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/core"
)

// Channel is the Valkey pub/sub channel every status change is published on.
const Channel = "notifier:status-events"

// publishTimeout bounds one PUBLISH, so an unreachable Valkey delays a commit's
// caller by at most this much.
const publishTimeout = time.Second

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

// Publisher is the core.Publisher that sends each change to Valkey.
type Publisher struct {
	client  valkey.Client
	channel string
	origin  Origin
}

var _ core.Publisher = (*Publisher)(nil)

// NewPublisher publishes on Channel through client, marking messages with origin.
func NewPublisher(client valkey.Client, origin Origin) *Publisher {
	return &Publisher{client: client, channel: Channel, origin: origin}
}

// Publish sends e synchronously, so changes published one after another by
// one goroutine reach Valkey, and every subscriber, in that order. It ignores
// the caller's cancellation (the change is committed; a client that went away
// does not make it unannounced) but not its values.
func (p *Publisher) Publish(ctx context.Context, e core.Event) error {
	b, err := encode(Message{Event: e, Origin: p.origin})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	if err := p.client.Do(ctx, p.client.B().Publish().Channel(p.channel).Message(string(b)).Build()).Error(); err != nil {
		return fmt.Errorf("publish status event: %w", err)
	}
	return nil
}
