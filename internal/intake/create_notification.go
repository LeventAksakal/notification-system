package intake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
)

// Meta is what a create request carries besides its body.
type Meta struct {
	IdempotencyKey string // "" when absent
	CorrelationID  string
	Traceparent    string // "" when the request has no span context
}

// Create validates, renders and stores one notification. replayed reports
// that the Idempotency-Key matched an earlier identical request, whose
// notification is returned.
func (s *Service) Create(ctx context.Context, r Request, m Meta) (n Notification, replayed bool, err error) {
	if err := checkIdempotencyKey(m.IdempotencyKey); err != nil {
		return Notification{}, false, err
	}
	// A replay answers from the stored resource before anything that reads
	// current state (templates, the clock): the first response is the answer.
	hash := requestHash("notification", r)
	if id, ok, err := s.earlier(ctx, m.IdempotencyKey, hash, sqlc.IdempotencyKeysResourceKindNotification); err != nil || ok {
		if err != nil {
			return Notification{}, false, err
		}
		return s.replayed(ctx, id)
	}
	templates, err := s.loadTemplates(ctx, []Request{r})
	if err != nil {
		return Notification{}, false, err
	}
	now, err := s.scheduleClock(ctx, []Request{r})
	if err != nil {
		return Notification{}, false, err
	}
	a, errs := Validate(r, templates, now)
	if len(errs) > 0 {
		return Notification{}, false, &ValidationError{Details: errs}
	}
	id := uuid.Must(uuid.NewV7())
	createdAt := idTime(id)
	params := sqlc.InsertNotificationParams{
		ID:            id.String(),
		Recipient:     a.To,
		Channel:       sqlc.NotificationsChannel(a.Channel),
		Priority:      sqlc.NotificationsPriority(a.Priority),
		Content:       a.Content,
		ScheduledAt:   nullTime(a.ScheduledAt),
		CorrelationID: m.CorrelationID,
		Traceparent:   sql.NullString{String: m.Traceparent, Valid: m.Traceparent != ""},
		CreatedAt:     createdAt,
	}
	if a.Template != nil {
		params.TemplateName = sql.NullString{String: a.Template.Name, Valid: true}
		params.TemplateVersion = sql.NullInt64{Int64: int64(a.Template.Version), Valid: true}
	}
	stored, err := s.store(ctx, m.IdempotencyKey, hash, sqlc.IdempotencyKeysResourceKindNotification, params.ID,
		func(q *sqlc.Queries, _ *sql.Tx) error { return q.InsertNotification(ctx, params) })
	if err != nil {
		return Notification{}, false, err
	}
	if stored != params.ID {
		return s.replayed(ctx, stored)
	}
	n, err = s.get(ctx, stored)
	if err != nil {
		return Notification{}, false, err
	}
	lctx := telemetry.WithNotification(ctx, n.ID, n.CorrelationID)
	slog.InfoContext(lctx, "notification accepted", "channel", a.Channel, "priority", a.Priority, "scheduled", n.Scheduled)
	s.publishCreated(lctx, n.Notification)
	s.wake(lctx, hintFor(n.Notification))
	return n, false, nil
}

// scheduleClock reads the database clock when any of rs carries a schedule,
// once per request whatever the batch size, so the horizon is measured on the
// clock that sets due_at and runs the claim (decision 0016). A request
// without a schedule reads nothing and gets the zero time, which Validate
// does not look at then. The scheduled_within_horizon CHECK repeats the rule
// in the INSERT with the statement's own NOW(3), which is never earlier than
// this read, so it only refuses what this check accepted if the server clock
// stepped back in between.
func (s *Service) scheduleClock(ctx context.Context, rs []Request) (time.Time, error) {
	for _, r := range rs {
		if r.ScheduledAt != nil {
			now, err := s.q.DatabaseNow(ctx)
			if err != nil {
				return time.Time{}, fmt.Errorf("read the database clock: %w", err)
			}
			return now, nil
		}
	}
	return time.Time{}, nil
}

// replayed answers a create replay with the stored notification.
func (s *Service) replayed(ctx context.Context, id string) (Notification, bool, error) {
	n, err := s.get(ctx, id)
	if err != nil {
		return Notification{}, false, err
	}
	slog.InfoContext(telemetry.WithNotification(ctx, n.ID, n.CorrelationID), "notification create replayed",
		"request_correlation_id", telemetry.CorrelationID(ctx))
	return n, true, nil
}

// loadTemplates reads the templates the requests name, body and version in
// one row read, so the version stored on a notification is the one rendered;
// an unknown name is simply absent from the map.
func (s *Service) loadTemplates(ctx context.Context, rs []Request) (map[string]sqlc.Template, error) {
	out := map[string]sqlc.Template{}
	tried := map[string]bool{}
	for _, r := range rs {
		if r.Template == nil || tried[r.Template.Name] {
			continue
		}
		tried[r.Template.Name] = true
		t, err := s.q.GetTemplate(ctx, r.Template.Name)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read template %q: %w", r.Template.Name, err)
		}
		out[t.Name] = t
	}
	return out, nil
}

// idTime is the creation time a UUIDv7 carries, to the millisecond; it is
// stored as created_at, so id order and created_at order agree.
func idTime(id uuid.UUID) time.Time {
	sec, nsec := id.Time().UnixTime()
	return time.Unix(sec, nsec).UTC().Truncate(time.Millisecond)
}

func nullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: t.UTC(), Valid: true}
}
