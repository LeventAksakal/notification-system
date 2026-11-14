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
	"io"
	"log/slog"
	"net"
	"sync"
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
	// publishHoldOff is how long publishes fail at once after one timed out
	// or lost its connection: during an outage only the first change of each
	// hold-off waits. A successful publish ends it early.
	publishHoldOff = 100 * time.Millisecond
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

// ErrUnavailable is returned without trying while a recent publish timed out
// or lost its connection: Valkey is taken as down for publishHoldOff, so a
// burst of status changes during an outage costs one bounded wait, not one
// per change. One probe publish at a time still tries, and its success ends
// the hold-off.
var ErrUnavailable = errors.New("status events: valkey unavailable")

// Publisher is the core.Publisher that sends each change to Valkey.
type Publisher struct {
	client  valkey.Client
	channel string
	origin  Origin
	now     func() time.Time

	mu sync.Mutex
	// down is set by a timeout or connection failure and cleared by the next
	// success; downUntil is the end of the hold-off it started.
	down      bool
	downUntil time.Time
	probing   bool // a publish is trying during the hold-off
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
// publishTimeout, whatever their number. After a timeout or a connection
// failure, calls fail at once with ErrUnavailable for publishHoldOff, except
// one probe at a time; the first success ends the hold-off.
func (p *Publisher) PublishAll(ctx context.Context, es []core.Event) error {
	if len(es) == 0 {
		return nil
	}
	probe, ok := p.admit()
	if !ok {
		return fmt.Errorf("publish %d status events: %w", len(es), ErrUnavailable)
	}
	cmds := make(valkey.Commands, 0, len(es))
	for _, e := range es {
		b, err := encode(Message{Event: e, Origin: p.origin})
		if err != nil {
			p.settle(ctx, probe, errAnswered)
			return err
		}
		cmds = append(cmds, p.client.B().Publish().Channel(p.channel).Message(string(b)).Build())
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	failed := 0
	var first, conn error
	for _, r := range p.client.DoMulti(ctx, cmds...) {
		if err := r.Error(); err != nil {
			failed++
			if first == nil {
				first = err
			}
			if conn == nil && connectionFailure(err) {
				conn = err
			}
		}
	}
	switch {
	case conn != nil:
		p.settle(ctx, probe, conn)
	case failed < len(es):
		p.settle(ctx, probe, nil) // Valkey answered: it is up
	default:
		p.settle(ctx, probe, errAnswered)
	}
	if first != nil {
		return fmt.Errorf("publish status events (%d of %d failed): %w", failed, len(es), first)
	}
	return nil
}

// errAnswered marks a call that failed without a sign that Valkey is down
// (error replies only, or nothing sent): it neither starts nor ends a hold-off.
var errAnswered = errors.New("answered with errors")

// admit says whether a call may try now, and whether it is the probe of a
// hold-off.
func (p *Publisher) admit() (probe, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.down || !p.now().Before(p.downUntil) {
		return false, true
	}
	if p.probing {
		return false, false
	}
	p.probing = true
	return true, true
}

// settle records the outcome of a call: err nil is a success, which ends a
// hold-off; a connection failure starts one; errAnswered changes nothing.
func (p *Publisher) settle(ctx context.Context, probe bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if probe {
		p.probing = false
	}
	switch {
	case err == nil:
		if p.down {
			p.down = false
			slog.InfoContext(ctx, "status events publishing again")
		}
	case errors.Is(err, errAnswered):
	default:
		if !p.down {
			slog.WarnContext(ctx, "status events: valkey unreachable; publishes fail at once until one succeeds",
				"hold_off", publishHoldOff, "err", err)
		}
		p.down = true
		p.downUntil = p.now().Add(publishHoldOff)
	}
}

// connectionFailure reports whether err says Valkey did not answer in time or
// the connection to it failed, as opposed to an error reply.
func connectionFailure(err error) bool {
	var reply *valkey.ValkeyError
	if errors.As(err, &reply) {
		return false
	}
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &ne) || errors.Is(err, valkey.ErrClosing) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
}
