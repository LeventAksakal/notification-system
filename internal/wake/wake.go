// Package wake carries hints that work became pending from the processes that
// commit it (the API, and workers releasing or rescheduling) to the idle
// delivery workers, over Valkey pub/sub. MySQL stays the queue (decision
// 0003): a hint says only "channel C, priority P, due at T"; the worker still
// claims the row from the database. Hints are best effort. A lost one delays
// a send by at most SafetyPoll and never loses it, because an idle worker
// still asks the database that often.
package wake

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

// SafetyPoll is the longest a missed hint may delay a send: an idle worker
// asks the database for the channel's next due time at least this often.
// Hints go missing only while a subscription reconnects, the listener claims
// at once on every (re)subscribe, and the rate limiter needs the same Valkey,
// so nothing is sent during that window anyway. One second keeps the cost of
// that guarantee at one indexed query per channel per second per worker.
// It is not a dial: it bounds a fault case and offers an operator no
// trade-off worth tuning.
const SafetyPoll = time.Second

const (
	// publishTimeout bounds one hint publish. It is the owner's p99 target
	// for a first send after idle (100 ms): a hint that cannot be published
	// within it has missed its purpose, and the safety poll delivers the row
	// anyway, so a commit's caller never waits longer for one.
	publishTimeout = 100 * time.Millisecond
	// publishHoldOff is how long hints fail at once after a publish timed out
	// or lost its connection. A hint withheld for this long costs at most what
	// the safety poll already allows.
	publishHoldOff = SafetyPoll
)

// Prefix is the start of every wake channel's name; the channel's name
// follows it ("notifier:wake:sms").
const Prefix = "notifier:wake:"

// channelName is the pub/sub channel hints for ch travel on. One per delivery
// channel, so a worker serving some channels hears only theirs.
func channelName(ch notification.Channel) string { return Prefix + string(ch) }

type wireHint struct {
	Priority notification.Priority `json:"priority"`
	Due      time.Time             `json:"due"`
}

func encode(h notification.Wake) ([]byte, error) {
	return json.Marshal(wireHint{Priority: h.Priority, Due: h.Due})
}

func decode(ch notification.Channel, b []byte) (notification.Wake, error) {
	var w wireHint
	if err := json.Unmarshal(b, &w); err != nil {
		return notification.Wake{}, fmt.Errorf("decode wake hint: %w", err)
	}
	if !w.Priority.Valid() {
		return notification.Wake{}, fmt.Errorf("decode wake hint: priority %q", w.Priority)
	}
	return notification.Wake{Channel: ch, Priority: w.Priority, Due: w.Due}, nil
}

// coalesce keeps one hint per channel and priority, with the earliest due time.
func coalesce(hints []notification.Wake) []notification.Wake {
	type key struct {
		ch notification.Channel
		p  notification.Priority
	}
	at := map[key]int{}
	var out []notification.Wake
	for _, h := range hints {
		k := key{h.Channel, h.Priority}
		if i, ok := at[k]; ok {
			if h.Due.Before(out[i].Due) {
				out[i].Due = h.Due
			}
			continue
		}
		at[k] = len(out)
		out = append(out, h)
	}
	return out
}

// ErrUnavailable is returned without trying while a recent publish timed out
// or lost its connection (for publishHoldOff); one probe at a time still
// tries, and its success ends the hold-off.
var ErrUnavailable = errors.New("wake hints: valkey unavailable")

// Publisher is the Valkey notification.Waker.
type Publisher struct {
	client valkey.Client
	now    func() time.Time

	mu        sync.Mutex
	down      bool
	downUntil time.Time
	probing   bool
}

var _ notification.Waker = (*Publisher)(nil)

// NewPublisher publishes hints through client.
func NewPublisher(client valkey.Client) *Publisher {
	return &Publisher{client: client, now: time.Now}
}

// Wake publishes hints, one per channel and priority with the earliest due
// time, as one pipeline under one publishTimeout. It ignores the caller's
// cancellation (the change is committed) but not its values.
func (p *Publisher) Wake(ctx context.Context, hints ...notification.Wake) error {
	hints = coalesce(hints)
	if len(hints) == 0 {
		return nil
	}
	probe, ok := p.admit()
	if !ok {
		return fmt.Errorf("publish %d wake hints: %w", len(hints), ErrUnavailable)
	}
	cmds := make(valkey.Commands, 0, len(hints))
	for _, h := range hints {
		b, err := encode(h)
		if err != nil {
			p.settle(ctx, probe, errAnswered)
			return err
		}
		cmds = append(cmds, p.client.B().Publish().Channel(channelName(h.Channel)).Message(string(b)).Build())
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
	case failed < len(hints):
		p.settle(ctx, probe, nil)
	default:
		p.settle(ctx, probe, errAnswered)
	}
	if first != nil {
		return fmt.Errorf("publish wake hints (%d of %d failed): %w", failed, len(hints), first)
	}
	return nil
}

// errAnswered marks a call that failed without a sign that Valkey is down: it
// neither starts nor ends a hold-off.
var errAnswered = errors.New("answered with errors")

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
			slog.InfoContext(ctx, "wake hints publishing again")
		}
	case errors.Is(err, errAnswered):
	default:
		if !p.down {
			slog.WarnContext(ctx, "wake hints: valkey unreachable; hints fail at once until one succeeds",
				"hold_off", publishHoldOff.String(), "err", err)
		}
		p.down = true
		p.downUntil = p.now().Add(publishHoldOff)
	}
}

// connectionFailure reports whether err says Valkey did not answer in time or
// the connection failed, as opposed to an error reply.
func connectionFailure(err error) bool {
	var reply *valkey.ValkeyError
	if errors.As(err, &reply) {
		return false
	}
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &ne) || errors.Is(err, valkey.ErrClosing) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
}

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
