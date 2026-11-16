package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// onHint applies a wake hint: its priority is asked again by its due time.
func (w *Worker) onHint(h notification.Wake) {
	if cr := w.runs[h.Channel]; cr != nil {
		cr.hint(h.Priority, h.Due)
	}
}

// onSubscribed asks every priority of every channel at once: hints published
// while the subscription was down are lost, the rows they announced are not.
func (w *Worker) onSubscribed() {
	for _, cr := range w.runs {
		for _, p := range notification.Priorities {
			cr.hint(p, time.Time{})
		}
	}
}

// hint lowers priority p's sleep to due and wakes the channel loop.
func (cr *channelRun) hint(p notification.Priority, due time.Time) {
	i := priorityIndex(p)
	if i < 0 {
		return
	}
	cr.mu.Lock()
	cr.sel.hint(i, due)
	cr.mu.Unlock()
	select {
	case cr.wake <- struct{}{}:
	default: // a wake is already pending
	}
}

// wakeAll hints that jobs became pending: to this worker at once and, through
// Deps.Waker, to the others. Best effort.
func (w *Worker) wakeAll(ctx context.Context, hints ...notification.Wake) {
	for _, h := range hints {
		w.onHint(h)
	}
	if w.deps.Waker == nil || len(hints) == 0 {
		return
	}
	if err := w.deps.Waker.Wake(ctx, hints...); err != nil {
		slog.WarnContext(ctx, "wake hint", "count", len(hints), "err", err)
	}
}
