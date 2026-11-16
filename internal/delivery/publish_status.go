package delivery

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// announce publishes that jobs are processing, in a goroutine owned by Run
// through inflight. The returned channel is closed once that publish is over;
// it is nil when there is nothing to publish.
func (w *Worker) announce(ctx context.Context, jobs []notification.Job, inflight *sync.WaitGroup) <-chan struct{} {
	if w.deps.Publisher == nil || len(jobs) == 0 {
		return nil
	}
	done := make(chan struct{})
	inflight.Add(1)
	go func() { // owned by Run through inflight; one bounded publish
		defer inflight.Done()
		defer close(done)
		w.publishAll(ctx, jobs, notification.StatusProcessing)
	}()
	return done
}

// publish announces job's move to st once after is closed (nil: at once), so
// it follows the event published before it.
// seq is the notification's status_seq after the change.
func (w *Worker) publish(ctx context.Context, job notification.Job, after <-chan struct{}, st notification.Status, attempts int, errText string, seq uint64) {
	if w.deps.Publisher == nil {
		return
	}
	if after != nil {
		<-after // closed when one bounded publish is over
	}
	err := w.deps.Publisher.Publish(ctx, notification.Event{
		NotificationID: job.ID, BatchID: job.BatchID, Status: st, Attempts: attempts,
		Error: errText, At: time.Now().UTC(), CorrelationID: job.CorrelationID, Seq: seq,
	})
	if err != nil {
		slog.WarnContext(ctx, "publish status change", "status", st, "err", err)
	}
}

// batchPublisher is a Publisher that sends many events as one call with one
// bounded wait (realtime.Publisher).
type batchPublisher interface {
	PublishAll(ctx context.Context, es []notification.Event) error
}

// publishAll announces that jobs moved to st (their attempts unchanged, their
// StatusSeq the change's), in order: in one call when the publisher has a
// batch form, else one by one.
func (w *Worker) publishAll(ctx context.Context, jobs []notification.Job, st notification.Status) {
	if w.deps.Publisher == nil || len(jobs) == 0 {
		return
	}
	bp, ok := w.deps.Publisher.(batchPublisher)
	if !ok {
		for _, job := range jobs {
			w.publish(jobContext(ctx, job), job, nil, st, job.Attempts, "", job.StatusSeq)
		}
		return
	}
	now := time.Now().UTC()
	es := make([]notification.Event, len(jobs))
	for i, job := range jobs {
		es[i] = notification.Event{NotificationID: job.ID, BatchID: job.BatchID, Status: st, Attempts: job.Attempts,
			At: now, CorrelationID: job.CorrelationID, Seq: job.StatusSeq}
	}
	if err := bp.PublishAll(ctx, es); err != nil {
		slog.WarnContext(ctx, "publish status changes", "status", st, "count", len(es), "err", err)
	}
}
