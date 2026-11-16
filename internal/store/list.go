package store

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// ListFilter narrows a notification list. Zero fields do not filter.
type ListFilter struct {
	Status string // stored status
	// Scheduled, with Status "pending": true keeps only the scheduled view
	// (never tried, due in the future), false only the rest.
	Scheduled   *bool
	Channel     string
	BatchID     string
	CreatedFrom *time.Time // inclusive
	CreatedTo   *time.Time // exclusive
	BeforeID    string     // keyset cursor: ids strictly below
	Limit       int
}

// ListedNotification is one row of a list.
type ListedNotification struct {
	sqlc.Notification
	IsScheduled bool
}

const listColumns = `SELECT id, batch_id, batch_index, recipient, channel, priority, content, status, failure_kind,
  attempts, due_at, scheduled_at, lease_until, claim_version, provider_message_id, last_error, correlation_id,
  traceparent, created_at, updated_at, finished_at,
  status = 'pending' AND attempts = 0 AND due_at > NOW(3) AS is_scheduled
FROM notifications`

// ListNotificationsQuery builds the list statement for f: newest first, id
// order, only the conditions f sets (an `? IS NULL OR` guard would hide the
// driving index from the optimizer when parameters are not interpolated).
//
// The index that yields id order is named: list_status_idx (status, id),
// list_channel_idx (channel, id), or the primary key. A batch filter is left
// to the optimizer: batch_position_idx narrows to at most MaxBatchSize rows.
//
// A date range also becomes an id range. Ids are UUIDv7 and created_at is the
// id's millisecond (intake sets both from one value), so the smallest UUIDv7
// of a millisecond bounds every row created in it or later; the created_at
// conditions stay as the exact check.
func ListNotificationsQuery(f ListFilter) (string, []any) {
	var where []string
	var args []any
	add := func(cond string, a ...any) {
		where = append(where, cond)
		args = append(args, a...)
	}
	hint := ""
	switch {
	case f.BatchID != "":
	case f.Status != "":
		hint = " FORCE INDEX (list_status_idx)"
	case f.Channel != "":
		hint = " FORCE INDEX (list_channel_idx)"
	}
	if f.Status != "" {
		add("status = ?", f.Status)
	}
	if f.Scheduled != nil {
		if *f.Scheduled {
			add("attempts = 0 AND due_at > NOW(3)")
		} else {
			add("NOT (attempts = 0 AND due_at > NOW(3))")
		}
	}
	if f.Channel != "" {
		add("channel = ?", f.Channel)
	}
	if f.BatchID != "" {
		add("batch_id = ?", f.BatchID)
	}
	if f.CreatedFrom != nil {
		add("id >= ?", MinIDAt(*f.CreatedFrom))
		add("created_at >= ?", f.CreatedFrom.UTC())
	}
	if f.CreatedTo != nil {
		add("id < ?", MinIDAt(*f.CreatedTo))
		add("created_at < ?", f.CreatedTo.UTC())
	}
	if f.BeforeID != "" {
		add("id < ?", f.BeforeID)
	}
	var b strings.Builder
	b.WriteString(listColumns)
	b.WriteString(hint)
	if len(where) > 0 {
		b.WriteString("\nWHERE ")
		b.WriteString(strings.Join(where, " AND "))
	}
	b.WriteString("\nORDER BY id DESC\nLIMIT ?")
	args = append(args, f.Limit)
	return b.String(), args
}

// ListNotifications runs ListNotificationsQuery.
func ListNotifications(ctx context.Context, db *sql.DB, f ListFilter) ([]ListedNotification, error) {
	query, args := ListNotificationsQuery(f)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []ListedNotification
	for rows.Next() {
		var r ListedNotification
		n := &r.Notification
		if err := rows.Scan(&n.ID, &n.BatchID, &n.BatchIndex, &n.Recipient, &n.Channel, &n.Priority, &n.Content,
			&n.Status, &n.FailureKind, &n.Attempts, &n.DueAt, &n.ScheduledAt, &n.LeaseUntil, &n.ClaimVersion,
			&n.ProviderMessageID, &n.LastError, &n.CorrelationID, &n.Traceparent, &n.CreatedAt, &n.UpdatedAt,
			&n.FinishedAt, &r.IsScheduled); err != nil {
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	return out, nil
}

// MinIDAt is the smallest UUIDv7 whose timestamp is at or after t: the
// millisecond of t, rounded up when t has a sub-millisecond part, with every
// random bit zero. Every UUIDv7 created at that millisecond or later sorts at
// or above it, also as a lowercase string.
func MinIDAt(t time.Time) string {
	ms := t.UnixMilli()
	if t.Sub(time.UnixMilli(ms)) > 0 {
		ms++
	}
	if ms < 0 {
		ms = 0
	}
	var u uuid.UUID
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(ms)) //nolint:gosec // clamped to >= 0 above
	copy(u[:6], ts[2:])
	u[6] = 0x70 // version 7
	u[8] = 0x80 // RFC 9562 variant
	return u.String()
}
