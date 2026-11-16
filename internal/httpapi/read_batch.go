package httpapi

import (
	"context"
	"errors"
	"uuid"

	"github.com/danielgtaylor/huma/v2"

	"github.com/LeventAksakal/notification-system/internal/intake"
)

type getBatchInput struct {
	ID     uuidParam `path:"id"`
	Limit  pageLimit `query:"limit"`
	Cursor cursor    `query:"cursor"`
}

func (*getBatchInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type batchOutput struct{ Body BatchStatus }

func (s *Server) GetBatch(ctx context.Context, in *getBatchInput) (*batchOutput, error) {
	b, err := s.intake.GetBatch(ctx, in.ID.String(), int(in.Limit), string(in.Cursor))
	if errors.Is(err, intake.ErrNotFound) {
		return nil, notFound("batch")
	}
	if err := refusal(err); err != nil {
		return nil, err
	}
	out := &batchOutput{BatchStatus{
		ID:         uuid.MustParse(b.ID),
		Total:      int(b.Total),
		CreatedAt:  b.CreatedAt,
		Counts:     toCounts(b.Counts),
		Items:      make([]Notification, len(b.Items)),
		NextCursor: b.NextCursor,
	}}
	for i, n := range b.Items {
		out.Body.Items[i] = toNotification(n)
	}
	return out, nil
}

func toCounts(c map[string]int) StatusCounts {
	return StatusCounts{
		Scheduled: c[string(StatusScheduled)], Pending: c[string(StatusPending)], Processing: c[string(StatusProcessing)],
		Sent: c[string(StatusSent)], Failed: c[string(StatusFailed)], Cancelled: c[string(StatusCancelled)],
	}
}
