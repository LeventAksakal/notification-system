package intake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// Notification is a stored notification as the API shows it.
type Notification struct {
	sqlc.Notification
	// Scheduled: stored pending, never tried, due in the future (database clock).
	Scheduled bool
	// History is the attempt history, oldest first; only filled by Get.
	History []sqlc.DeliveryAttempt
}

// Status is the API status: the stored one, or "scheduled".
func (n Notification) Status() string {
	if n.Scheduled {
		return "scheduled"
	}
	return string(n.Notification.Status)
}

// Filter narrows a notification list. Status is an API status.
type Filter struct {
	Status      string
	Channel     notification.Channel
	BatchID     string
	CreatedFrom *time.Time
	CreatedTo   *time.Time
	Limit       int
	Cursor      string
}

// Page is one page of a list, newest first.
type Page struct {
	Items      []Notification
	NextCursor string // "" on the last page
}

// Get returns one notification with its attempt history.
func (s *Service) Get(ctx context.Context, id string) (Notification, error) {
	n, err := s.get(ctx, id)
	if err != nil {
		return Notification{}, err
	}
	n.History, err = s.q.ListDeliveryAttempts(ctx, id)
	if err != nil {
		return Notification{}, fmt.Errorf("read attempts of %s: %w", id, err)
	}
	if n.History == nil {
		n.History = []sqlc.DeliveryAttempt{}
	}
	return n, nil
}

func (s *Service) get(ctx context.Context, id string) (Notification, error) {
	row, err := s.q.GetNotification(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("read notification %s: %w", id, err)
	}
	return Notification{Notification: row.Notification, Scheduled: row.IsScheduled != 0}, nil
}

// List returns one page of notifications, newest first.
func (s *Service) List(ctx context.Context, f Filter) (Page, error) {
	limit := pageSize(f.Limit)
	lf := store.ListFilter{
		Channel: string(f.Channel), BatchID: f.BatchID, CreatedFrom: f.CreatedFrom, CreatedTo: f.CreatedTo, Limit: limit + 1,
	}
	switch f.Status {
	case "":
	case "scheduled", "pending":
		lf.Status = string(notification.StatusPending)
		only := f.Status == "scheduled"
		lf.Scheduled = &only
	default:
		if !notification.Status(f.Status).Valid() {
			return Page{}, invalid("status", "unknown status")
		}
		lf.Status = f.Status
	}
	if f.Cursor != "" {
		id, err := DecodeListCursor(f.Cursor)
		if err != nil {
			return Page{}, err
		}
		lf.BeforeID = id
	}
	rows, err := store.ListNotifications(ctx, s.db, lf)
	if err != nil {
		return Page{}, err
	}
	page := Page{Items: make([]Notification, 0, min(len(rows), limit))}
	for i, r := range rows {
		if i == limit {
			page.NextCursor = EncodeListCursor(page.Items[limit-1].ID)
			break
		}
		page.Items = append(page.Items, Notification{Notification: r.Notification, Scheduled: r.IsScheduled})
	}
	return page, nil
}

func pageSize(limit int) int {
	switch {
	case limit <= 0:
		return notification.DefaultPageSize
	case limit > notification.MaxPageSize:
		return notification.MaxPageSize
	}
	return limit
}
