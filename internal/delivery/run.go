package delivery

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// Run delivers until ctx is cancelled. Then it stops claiming, lets in-flight
// sends finish for up to ShutdownTimeout, releases what was claimed but not
// sent, and returns after every goroutine it started has ended.
func (w *Worker) Run(ctx context.Context) error {
	// Sends outlive ctx by up to the shutdown grace.
	sendCtx, cancelSends := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSends()

	var inflight sync.WaitGroup
	n := len(w.cfg.Channels)
	own := max(1, w.cfg.Concurrency/(2*n))
	shared := make(chan struct{}, max(0, w.cfg.Concurrency-own*n))
	g, gctx := errgroup.WithContext(ctx)
	w.runs = make(map[notification.Channel]*channelRun, n)
	for _, ch := range w.cfg.Channels {
		w.runs[ch] = w.newChannelRun(ch, callPlaces{own: make(chan struct{}, own), shared: shared})
	}
	for _, cr := range w.runs {
		g.Go(func() error {
			w.channelLoop(gctx, sendCtx, cr, &inflight)
			return nil
		})
	}
	if w.deps.Wakes != nil {
		g.Go(func() error {
			return w.deps.Wakes.Run(gctx, w.cfg.Channels, w.onHint, w.onSubscribed)
		})
	}
	g.Go(func() error {
		w.reaper(gctx)
		return nil
	})
	err := g.Wait()

	done := make(chan struct{})
	go func() { // owned here: Run waits on done below before returning
		inflight.Wait()
		close(done)
	}()
	grace := time.NewTimer(w.cfg.ShutdownTimeout)
	defer grace.Stop()
	select {
	case <-done:
	case <-grace.C:
		slog.WarnContext(sendCtx, "shutdown grace over; aborting in-flight sends", "grace", w.cfg.ShutdownTimeout.String())
		cancelSends()
		<-done
	}
	return err
}
