package httpapi

import (
	"context"
	"uuid"

	"github.com/danielgtaylor/huma/v2"

	"github.com/LeventAksakal/notification-system/internal/intake"
)

type createBatchInput struct {
	CreateHeaders
	Body BatchCreate
}

func (*createBatchInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type createBatchOutput struct {
	CreatedHeaders
	Body BatchCreated
}

func (s *Server) CreateBatch(ctx context.Context, in *createBatchInput) (*createBatchOutput, error) {
	rs := make([]intake.Request, len(in.Body.Notifications))
	for i, n := range in.Body.Notifications {
		rs[i] = toRequest(n)
	}
	b, replayed, err := s.intake.CreateBatch(ctx, rs, meta(ctx, in.IdempotencyKey))
	if err := refusal(err); err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, len(b.NotificationIDs))
	for i, id := range b.NotificationIDs {
		ids[i] = uuid.MustParse(id)
	}
	return &createBatchOutput{
		CreatedHeaders: created(ctx, replayed),
		Body:           BatchCreated{ID: uuid.MustParse(b.ID), Total: b.Total, NotificationIDs: ids, CreatedAt: b.CreatedAt},
	}, nil
}
