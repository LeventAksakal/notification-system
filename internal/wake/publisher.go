package wake

import (
	"context"
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
