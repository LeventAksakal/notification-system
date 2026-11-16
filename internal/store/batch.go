package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// BatchItem is one notification row of a batch insert.
type BatchItem struct {
	ID          string
	BatchIndex  int
	Recipient   string
	Channel     string
	Priority    string
	Content     string
	ScheduledAt *time.Time // nil: due now
	// TemplateName and TemplateVersion: the template that rendered Content;
	// "" and 0 for plain content, stored as NULL.
	TemplateName    string
	TemplateVersion uint32
}

// BatchMeta is what every row of one batch shares.
type BatchMeta struct {
	BatchID       string
	CorrelationID string
	Traceparent   string // "" stores NULL
	CreatedAt     time.Time
}

// Chunk bounds of one multi-row INSERT: few enough placeholders for MySQL's
// 65535 limit and few enough bytes to stay far below max_allowed_packet.
const (
	maxRowsPerInsert  = 500
	maxBytesPerInsert = 4 << 20
)

const insertColumns = `INSERT INTO notifications
  (id, batch_id, batch_index, recipient, channel, priority, content, due_at, scheduled_at, correlation_id, traceparent, created_at,
   template_name, template_version)
VALUES `

// One row; due_at is the schedule or now on the database clock, as in the
// sqlc query InsertNotification.
const insertRow = `(?, ?, ?, ?, ?, ?, ?, GREATEST(COALESCE(?, NOW(3)), NOW(3)), ?, ?, ?, ?, ?, ?)`

// argsPerRow is the number of placeholders in insertRow.
const argsPerRow = 14

// InsertBatchItems inserts a batch's notifications with multi-row INSERTs in
// chunks, inside tx. sqlc cannot express a variable-length VALUES list, so
// this one statement is built by hand; the values are always placeholders.
func InsertBatchItems(ctx context.Context, tx *sql.Tx, meta BatchMeta, items []BatchItem) error {
	var tp sql.NullString
	if meta.Traceparent != "" {
		tp = sql.NullString{String: meta.Traceparent, Valid: true}
	}
	for start := 0; start < len(items); {
		end, size := start, 0
		for end < len(items) && end-start < maxRowsPerInsert && (end == start || size+len(items[end].Content) <= maxBytesPerInsert) {
			size += len(items[end].Content)
			end++
		}
		chunk := items[start:end]
		var b strings.Builder
		b.WriteString(insertColumns)
		args := make([]any, 0, len(chunk)*argsPerRow)
		for i, it := range chunk {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(insertRow)
			var sched sql.NullTime
			if it.ScheduledAt != nil {
				sched = sql.NullTime{Time: it.ScheduledAt.UTC(), Valid: true}
			}
			var tname sql.NullString
			var tversion sql.NullInt64
			if it.TemplateName != "" {
				tname = sql.NullString{String: it.TemplateName, Valid: true}
				tversion = sql.NullInt64{Int64: int64(it.TemplateVersion), Valid: true}
			}
			args = append(args, it.ID, meta.BatchID, it.BatchIndex, it.Recipient, it.Channel, it.Priority, it.Content,
				sched, sched, meta.CorrelationID, tp, meta.CreatedAt.UTC(), tname, tversion)
		}
		if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
			return fmt.Errorf("insert batch items %d..%d: %w", start, end-1, err)
		}
		start = end
	}
	return nil
}
