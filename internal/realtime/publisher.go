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

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// Channel is the Valkey pub/sub channel every status change is published on.
const Channel = "notifier:status-events"

// publishTimeout bounds one publish call (one event, or a whole batch as one
// pipeline), so an unreachable Valkey delays a commit's caller by at most
// this much, and while Valkey is down only the one probe in flight waits it.
//
// Where it comes from: a PUBLISH to a healthy Valkey on the same network is
// answered in about a millisecond, so a call that takes a second is a Valkey
// that is down or hung, not a slow one; and one second spent by a caller is
// far inside the API's request timeout (APP_REQUEST_TIMEOUT, 15 s) and the
// worker's result-write timeout (storeTimeout, 10 s). A subscriber waits
// twice this for a missing predecessor (holdForPredecessor). Arbitrary
// within those bounds; to tune it, take the p99.9 of publish latency under
// load and keep it two orders of magnitude above that.
const publishTimeout = time.Second

// wireMessage is one status change as it travels between processes.
type wireMessage struct {
	NotificationID string    `json:"notificationId"`
	BatchID        string    `json:"batchId,omitempty"`
	Status         string    `json:"status"`
	Attempts       int       `json:"attempts"`
	Error          string    `json:"error,omitempty"`
	At             time.Time `json:"at"`
	CorrelationID  string    `json:"correlationId"`
	Seq            uint64    `json:"seq,omitempty"` // absent from a publisher that predates it
}

func encode(e notification.Event) ([]byte, error) {
	return json.Marshal(wireMessage{
		NotificationID: e.NotificationID, BatchID: e.BatchID, Status: string(e.Status), Attempts: e.Attempts,
		Error: e.Error, At: e.At, CorrelationID: e.CorrelationID, Seq: e.Seq,
	})
}

func decode(b []byte) (notification.Event, error) {
	var w wireMessage
	if err := json.Unmarshal(b, &w); err != nil {
		return notification.Event{}, fmt.Errorf("decode status event: %w", err)
	}
	if w.NotificationID == "" || !notification.Status(w.Status).Valid() {
		return notification.Event{}, fmt.Errorf("decode status event: id %q status %q", w.NotificationID, w.Status)
	}
	return notification.Event{
		NotificationID: w.NotificationID, BatchID: w.BatchID, Status: notification.Status(w.Status), Attempts: w.Attempts,
		Error: w.Error, At: w.At, CorrelationID: w.CorrelationID, Seq: w.Seq,
	}, nil
}

// ErrUnavailable is returned without trying while Valkey is taken as down: a
// publish timed out or lost its connection, and no publish has succeeded
// since. One probe publish at a time still tries, and its success ends it, so
// a burst of status changes during an outage costs one bounded wait at a
// time, not one per change.
var ErrUnavailable = errors.New("status events: valkey unavailable")

// Publisher is the notification.Publisher that sends each change to Valkey.
type Publisher struct {
	client  valkey.Client
	channel string

	mu sync.Mutex
	// down is set by a timeout or connection failure and cleared by the next
	// success.
	down    bool
	probing bool // a publish is trying while down
}

var _ notification.Publisher = (*Publisher)(nil)

// NewPublisher publishes on Channel through client.
func NewPublisher(client valkey.Client) *Publisher {
	return &Publisher{client: client, channel: Channel}
}

// Publish sends e synchronously, so changes published one after another by
// one goroutine reach Valkey, and every subscriber, in that order. It ignores
// the caller's cancellation (the change is committed; a client that went away
// does not make it unannounced) but not its values.
func (p *Publisher) Publish(ctx context.Context, e notification.Event) error {
	return p.PublishAll(ctx, []notification.Event{e})
}

// PublishAll sends es in order as one pipeline under one deadline of
// publishTimeout, whatever their number. After a timeout or a connection
// failure, calls fail at once with ErrUnavailable, except one probe at a
// time, until a publish succeeds.
func (p *Publisher) PublishAll(ctx context.Context, es []notification.Event) error {
	if len(es) == 0 {
		return nil
	}
	probe, ok := p.admit()
	if !ok {
		return fmt.Errorf("publish %d status events: %w", len(es), ErrUnavailable)
	}
	cmds := make(valkey.Commands, 0, len(es))
	for _, e := range es {
		b, err := encode(e)
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
// (error replies only, or nothing sent): it neither marks Valkey down nor up.
var errAnswered = errors.New("answered with errors")

// admit says whether a call may try now, and whether it is the probe while
// Valkey is down.
func (p *Publisher) admit() (probe, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.down {
		return false, true
	}
	if p.probing {
		return false, false
	}
	p.probing = true
	return true, true
}

// settle records the outcome of a call: err nil is a success, which marks
// Valkey up; a connection failure marks it down; errAnswered changes nothing.
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
			slog.WarnContext(ctx, "status events: valkey unreachable; publishes fail at once until a probe succeeds",
				"err", err)
		}
		p.down = true
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
