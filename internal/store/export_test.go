package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// LatencyBucket is latencyBucket, for the tests that hold the SQL bucket
// expression equal to it.
var LatencyBucket = latencyBucket

// DeliverySince is the delivery part of Read over [since, now], for a since
// the test chooses (Read takes it from the database clock).
func DeliverySince(ctx context.Context, db *sql.DB, since time.Time) (map[notification.Channel]Delivery, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	by, err := delivery(ctx, sqlc.New(tx), since)
	if err != nil {
		return nil, err
	}
	out := map[notification.Channel]Delivery{}
	for ch, d := range by {
		dd := d.Delivery
		dd.Latency = d.latency.percentiles()
		out[ch] = dd
	}
	return out, nil
}
