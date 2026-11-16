package httpapi

import (
	"context"

	"github.com/danielgtaylor/huma/v2"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
)

type CreateHeaders struct {
	IdempotencyKey idempotencyKey `header:"Idempotency-Key"`
	CorrelationID  correlationID  `header:"X-Correlation-ID"`
}

type createNotificationInput struct {
	CreateHeaders
	Body NotificationCreate
}

func (*createNotificationInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type CreatedHeaders struct {
	CorrelationID string  `header:"X-Correlation-ID" doc:"The correlation id in force for this request"`
	Replayed      *string "header:\"Idempotent-Replayed\" enum:\"true\" nullable:\"false\" doc:\"Present and `true` when this response repeats an earlier one for the same Idempotency-Key\""
}

type createNotificationOutput struct {
	CreatedHeaders
	Body Notification
}

func (s *Server) CreateNotification(ctx context.Context, in *createNotificationInput) (*createNotificationOutput, error) {
	n, replayed, err := s.intake.Create(ctx, toRequest(in.Body), meta(ctx, in.IdempotencyKey))
	if err := refusal(err); err != nil {
		return nil, err
	}
	return &createNotificationOutput{CreatedHeaders: created(ctx, replayed), Body: toNotification(n)}, nil
}

// meta collects what the service stores besides the body: the key, the
// correlation id the middleware put in the context, the active trace.
func meta(ctx context.Context, key idempotencyKey) intake.Meta {
	return intake.Meta{IdempotencyKey: string(key), CorrelationID: telemetry.CorrelationID(ctx),
		Traceparent: telemetry.Traceparent(ctx)}
}

func created(ctx context.Context, replayed bool) CreatedHeaders {
	h := CreatedHeaders{CorrelationID: telemetry.CorrelationID(ctx)}
	if replayed {
		h.Replayed = ptr("true")
	}
	return h
}

func toRequest(n NotificationCreate) intake.Request {
	r := intake.Request{To: n.To, Channel: notification.Channel(n.Channel),
		ScheduledAt: n.ScheduledAt, Priority: notification.Priority(n.Priority)}
	if n.Content != "" {
		r.Content = &n.Content
	}
	if n.Template != nil {
		r.Template = &intake.TemplateRef{Name: string(n.Template.Name), Variables: n.Template.Variables}
	}
	return r
}
