package store

import (
	"context"
	"fmt"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// NextDue reports how long until each priority's earliest pending
// notification of channel is due, on the database's clock (<= 0: due now);
// a priority with nothing pending is absent. It costs one claim_idx dive per
// priority, whatever the backlog.
func (s *Queue) NextDue(ctx context.Context, channel notification.Channel) (map[notification.Priority]time.Duration, error) {
	rows, err := s.q.NextDue(ctx, sqlc.NextDueParams{Channel: sqlc.NotificationsChannel(channel)})
	if err != nil {
		return nil, fmt.Errorf("next due %s: %w", channel, err)
	}
	out := make(map[notification.Priority]time.Duration, len(rows))
	for _, r := range rows {
		if r.Pending {
			out[notification.Priority(r.Priority)] = time.Duration(r.WaitUs) * time.Microsecond
		}
	}
	return out, nil
}
