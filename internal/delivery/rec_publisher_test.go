package delivery

import (
	"context"
	"sync"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

type recPublisher struct {
	mu     sync.Mutex
	events []notification.Event
}

func (p *recPublisher) Publish(_ context.Context, e notification.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
	return nil
}

func (p *recPublisher) statuses(id string) []notification.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []notification.Status
	for _, e := range p.events {
		if e.NotificationID == id {
			out = append(out, e.Status)
		}
	}
	return out
}

// gatedPublisher holds every batch call (the processing events) until open is
// called, like a Valkey that does not answer, and records the batch's events
// when its call is over. Single events (the results) go out at once, so a
// result not held back for its processing event would overtake it.
type gatedPublisher struct {
	recPublisher
	gate chan struct{}
	once sync.Once
}

func newGatedPublisher() *gatedPublisher { return &gatedPublisher{gate: make(chan struct{})} }

func (p *gatedPublisher) open() { p.once.Do(func() { close(p.gate) }) }

func (p *gatedPublisher) PublishAll(ctx context.Context, es []notification.Event) error {
	select {
	case <-p.gate:
	case <-ctx.Done():
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, es...)
	return nil
}
