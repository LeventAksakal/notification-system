package intake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// Batch is one page of a batch with its counts per API status.
type Batch struct {
	sqlc.Batch
	Counts     map[string]int
	Items      []Notification
	NextCursor string
}

// BatchStatusSeqs returns the status_seq of every notification of a batch,
// keyed by id: what a batch subscription already has once it is read.
func (s *Service) BatchStatusSeqs(ctx context.Context, id string) (map[string]uint64, error) {
	rows, err := s.q.ListBatchStatusSeqs(ctx, sql.NullString{String: id, Valid: true})
	if err != nil {
		return nil, fmt.Errorf("batch %s status sequences: %w", id, err)
	}
	out := make(map[string]uint64, len(rows))
	for _, r := range rows {
		out[r.ID] = r.StatusSeq
	}
	return out, nil
}

// GetBatch returns a batch's counts per API status and one page of its items
// in request order.
func (s *Service) GetBatch(ctx context.Context, id string, limit int, cursor string) (Batch, error) {
	after := -1
	if cursor != "" {
		i, err := DecodeBatchCursor(cursor)
		if err != nil {
			return Batch{}, err
		}
		after = i
	}
	b, err := s.q.GetBatch(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Batch{}, ErrNotFound
	}
	if err != nil {
		return Batch{}, fmt.Errorf("read batch %s: %w", id, err)
	}
	out := Batch{Batch: b, Counts: map[string]int{}}
	bid := sql.NullString{String: id, Valid: true}
	counts, err := s.q.CountBatchByStatus(ctx, bid)
	if err != nil {
		return Batch{}, fmt.Errorf("count batch %s: %w", id, err)
	}
	for _, c := range counts {
		out.Counts[asString(c.ApiStatus)] = int(c.N)
	}
	n := pageSize(limit)
	rows, err := s.q.ListBatchItems(ctx, sqlc.ListBatchItemsParams{
		BatchID: bid, AfterIndex: sql.NullInt32{Int32: int32(after), Valid: true}, Limit: int32(n + 1), //nolint:gosec // bounded
	})
	if err != nil {
		return Batch{}, fmt.Errorf("list batch %s: %w", id, err)
	}
	out.Items = make([]Notification, 0, min(len(rows), n))
	for i, r := range rows {
		if i == n {
			out.NextCursor = EncodeBatchCursor(int(out.Items[n-1].BatchIndex.Int32))
			break
		}
		out.Items = append(out.Items, Notification{Notification: r.Notification, Scheduled: r.IsScheduled != 0})
	}
	return out, nil
}

func asString(v any) string {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case string:
		return x
	}
	return fmt.Sprint(v)
}
