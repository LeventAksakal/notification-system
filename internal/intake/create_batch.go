package intake

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// BatchCreated is the answer to an accepted batch.
type BatchCreated struct {
	ID              string
	Total           int
	NotificationIDs []string // in request order
	CreatedAt       time.Time
}

// CreateBatch validates every item, then stores the batch and all of its
// notifications in one transaction, or nothing (decision 0011).
func (s *Service) CreateBatch(ctx context.Context, rs []Request, m Meta) (b BatchCreated, replayed bool, err error) {
	if len(rs) == 0 || len(rs) > notification.MaxBatchSize {
		return BatchCreated{}, false, invalid("notifications", fmt.Sprintf("must hold 1 to %d notifications", notification.MaxBatchSize))
	}
	if err := checkIdempotencyKey(m.IdempotencyKey); err != nil {
		return BatchCreated{}, false, err
	}
	hash := requestHash("batch", rs)
	if id, ok, err := s.earlier(ctx, m.IdempotencyKey, hash, sqlc.IdempotencyKeysResourceKindBatch); err != nil || ok {
		if err != nil {
			return BatchCreated{}, false, err
		}
		b, err = s.batchCreated(ctx, id)
		return b, err == nil, err
	}
	templates, err := s.loadTemplates(ctx, rs)
	if err != nil {
		return BatchCreated{}, false, err
	}
	now, err := s.scheduleClock(ctx, rs)
	if err != nil {
		return BatchCreated{}, false, err
	}
	items := make([]store.BatchItem, len(rs))
	var all []FieldError
	for i, r := range rs {
		a, errs := Validate(r, templates, now)
		for _, e := range errs {
			e.Index = &i
			all = append(all, e)
		}
		if len(all) > 0 {
			continue // the batch is refused; keep no content
		}
		items[i] = store.BatchItem{BatchIndex: i, Recipient: a.To, Channel: string(a.Channel),
			Priority: string(a.Priority), Content: a.Content, ScheduledAt: a.ScheduledAt}
		if a.Template != nil {
			items[i].TemplateName, items[i].TemplateVersion = a.Template.Name, a.Template.Version
		}
	}
	if len(all) > 0 {
		return BatchCreated{}, false, &ValidationError{Details: all}
	}

	batchID := uuid.Must(uuid.NewV7())
	b = BatchCreated{ID: batchID.String(), Total: len(rs), CreatedAt: idTime(batchID), NotificationIDs: make([]string, len(rs))}
	for i := range items {
		items[i].ID = uuid.Must(uuid.NewV7()).String()
		b.NotificationIDs[i] = items[i].ID
	}
	meta := store.BatchMeta{BatchID: b.ID, CorrelationID: m.CorrelationID, Traceparent: m.Traceparent, CreatedAt: b.CreatedAt}
	stored, err := s.store(ctx, m.IdempotencyKey, hash, sqlc.IdempotencyKeysResourceKindBatch, b.ID,
		func(q *sqlc.Queries, tx *sql.Tx) error {
			if err := q.InsertBatch(ctx, sqlc.InsertBatchParams{
				ID: b.ID, Total: uint32(len(rs)), CorrelationID: m.CorrelationID, CreatedAt: b.CreatedAt, //nolint:gosec // at most MaxBatchSize
			}); err != nil {
				return fmt.Errorf("insert batch: %w", err)
			}
			return store.InsertBatchItems(ctx, tx, meta, items)
		})
	if err != nil {
		return BatchCreated{}, false, err
	}
	if stored != b.ID {
		b, err = s.batchCreated(ctx, stored)
		return b, true, err
	}

	slog.InfoContext(ctx, "batch accepted", "batch_id", b.ID, "total", b.Total)
	events := make([]notification.Event, len(items))
	for i, it := range items {
		events[i] = notification.Event{
			NotificationID: it.ID, BatchID: b.ID, Status: notification.StatusPending, At: b.CreatedAt, CorrelationID: m.CorrelationID,
			Seq: 1,
		}
	}
	// One pipelined publish with one deadline, whatever the batch size.
	if err := s.pub.PublishAll(ctx, events); err != nil {
		slog.WarnContext(ctx, "publish batch status changes", "batch_id", b.ID, "count", len(events), "err", err)
	}
	// One pipelined hint per channel and priority (the waker keeps the
	// earliest due time of each).
	hints := make([]notification.Wake, len(items))
	for i, it := range items {
		due := b.CreatedAt // due now: any time not after the commit
		if it.ScheduledAt != nil {
			due = *it.ScheduledAt
		}
		hints[i] = notification.Wake{Channel: notification.Channel(it.Channel), Priority: notification.Priority(it.Priority), Due: due}
	}
	s.wake(ctx, hints...)
	return b, false, nil
}

func (s *Service) batchCreated(ctx context.Context, id string) (BatchCreated, error) {
	b, err := s.q.GetBatch(ctx, id)
	if err != nil {
		return BatchCreated{}, fmt.Errorf("read batch %s: %w", id, err)
	}
	ids, err := s.q.ListBatchNotificationIDs(ctx, sql.NullString{String: id, Valid: true})
	if err != nil {
		return BatchCreated{}, fmt.Errorf("read batch %s ids: %w", id, err)
	}
	return BatchCreated{ID: b.ID, Total: int(b.Total), NotificationIDs: ids, CreatedAt: b.CreatedAt}, nil
}
