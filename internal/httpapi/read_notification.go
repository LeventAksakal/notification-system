package httpapi

import (
	"context"
	"errors"
	"time"
	"uuid"

	"github.com/danielgtaylor/huma/v2"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
)

type idInput struct {
	ID uuidParam `path:"id"`
}

type notificationOutput struct{ Body Notification }

func (s *Server) GetNotification(ctx context.Context, in *idInput) (*notificationOutput, error) {
	n, err := s.intake.Get(ctx, in.ID.String())
	if errors.Is(err, intake.ErrNotFound) {
		return nil, notFound("notification")
	}
	if err != nil {
		return nil, err
	}
	return &notificationOutput{toNotification(n)}, nil
}

type listNotificationsInput struct {
	Status      Status    "query:\"status\" doc:\"`scheduled` selects pending notifications whose due time is in the future and that were never tried; `pending` selects the other pending ones.\""
	Channel     Channel   `query:"channel"`
	BatchID     uuidParam `query:"batchId"`
	CreatedFrom time.Time "query:\"createdFrom\" doc:\"Inclusive lower bound on `createdAt`\""
	CreatedTo   time.Time "query:\"createdTo\" doc:\"Exclusive upper bound on `createdAt`\""
	Limit       pageLimit `query:"limit"`
	Cursor      cursor    `query:"cursor"`
}

// Resolve refuses a single-value parameter given twice (Huma reads the first).
func (*listNotificationsInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type pageOutput struct{ Body NotificationPage }

func (s *Server) ListNotifications(ctx context.Context, in *listNotificationsInput) (*pageOutput, error) {
	f := intake.Filter{Status: string(in.Status), Channel: notification.Channel(in.Channel),
		Limit: int(in.Limit), Cursor: string(in.Cursor)}
	if in.BatchID != (uuidParam{}) {
		f.BatchID = in.BatchID.String()
	}
	if !in.CreatedFrom.IsZero() {
		f.CreatedFrom = &in.CreatedFrom
	}
	if !in.CreatedTo.IsZero() {
		f.CreatedTo = &in.CreatedTo
	}
	page, err := s.intake.List(ctx, f)
	if err := refusal(err); err != nil {
		return nil, err
	}
	out := &pageOutput{NotificationPage{Items: make([]Notification, len(page.Items)), NextCursor: page.NextCursor}}
	for i, n := range page.Items {
		out.Body.Items[i] = toNotification(n)
	}
	return out, nil
}

func toNotification(n intake.Notification) Notification {
	out := Notification{
		ID:            uuid.MustParse(n.ID),
		To:            n.Recipient,
		Channel:       Channel(n.Channel),
		Content:       n.Content,
		Priority:      Priority(n.Priority),
		Status:        Status(n.Status()),
		Attempts:      int(n.Attempts),
		DueAt:         n.DueAt,
		CorrelationID: n.CorrelationID,
		CreatedAt:     n.CreatedAt,
		UpdatedAt:     n.UpdatedAt,
	}
	if n.BatchID.Valid {
		out.BatchID = ptr(uuid.MustParse(n.BatchID.String))
	}
	if n.FailureKind.Valid {
		out.FailureKind = ptr(FailureKind(n.FailureKind.NotificationsFailureKind))
	}
	if n.LastError.Valid {
		out.LastError = &n.LastError.String
	}
	if n.ProviderMessageID.Valid {
		out.ProviderMessageID = &n.ProviderMessageID.String
	}
	out.ScheduledAt = timePtr(n.ScheduledAt.Time, n.ScheduledAt.Valid)
	out.FinishedAt = timePtr(n.FinishedAt.Time, n.FinishedAt.Valid)
	if n.TemplateName.Valid && n.TemplateVersion.Valid {
		out.Template = &TemplateOrigin{Name: TemplateName(n.TemplateName.String), Version: int(n.TemplateVersion.Int64)}
	}
	if n.History != nil {
		out.DeliveryAttempts = make([]DeliveryAttempt, len(n.History))
		for i, a := range n.History {
			d := DeliveryAttempt{
				AttemptNo:  int(a.AttemptNo),
				StartedAt:  a.StartedAt,
				DurationMs: int(a.DurationMs),
				Outcome:    string(a.Outcome),
			}
			if a.HttpStatus.Valid {
				d.HTTPStatus = ptr(int(a.HttpStatus.Int16))
			}
			if a.Error.Valid {
				d.Error = &a.Error.String
			}
			if a.ProviderMessageID.Valid {
				d.ProviderMessageID = &a.ProviderMessageID.String
			}
			out.DeliveryAttempts[i] = d
		}
	}
	return out
}

func timePtr(t time.Time, ok bool) *time.Time {
	if !ok {
		return nil
	}
	return &t
}
