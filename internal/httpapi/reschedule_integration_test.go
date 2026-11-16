//go:build integration

package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/support"
)

type nopPublisher struct{}

func (nopPublisher) Publish(context.Context, notification.Event) error { return nil }

// PATCH /v1/notifications/{id} over HTTP against MySQL: the body rules the
// validator cannot see (null allowed, the horizon) and the error mapping.
func TestReschedulingOverHTTP(t *testing.T) {
	db, _ := support.MySQL(t)
	svc := intake.New(db, nopPublisher{}, time.Hour)
	h, err := NewServer(svc, nil).Handler()
	if err != nil {
		t.Fatal(err)
	}
	do := func(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	create := func(t *testing.T, scheduledAt string) Notification {
		t.Helper()
		rec := do(t, http.MethodPost, "/v1/notifications",
			`{"to":"device-1","channel":"push","content":"hi","scheduledAt":"`+scheduledAt+`"}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("create: %d %s", rec.Code, rec.Body)
		}
		var n Notification
		if err := json.Unmarshal(rec.Body.Bytes(), &n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	inADay := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)

	t.Run("a new time answers 200 with the notification", func(t *testing.T) {
		n := create(t, inADay)
		at := time.Now().Add(48 * time.Hour).UTC().Truncate(time.Second)
		rec := do(t, http.MethodPatch, "/v1/notifications/"+n.ID.String(), `{"scheduledAt":"`+at.Format(time.RFC3339)+`"}`)
		var got Notification
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &got) != nil {
			t.Fatalf("got %d %s", rec.Code, rec.Body)
		}
		if got.Status != StatusScheduled || got.ScheduledAt == nil || !got.ScheduledAt.Equal(at) || !got.DueAt.Equal(at) {
			t.Fatalf("got %+v, want scheduled at %v", got, at)
		}
	})
	t.Run("null answers 200, due now", func(t *testing.T) {
		n := create(t, inADay)
		rec := do(t, http.MethodPatch, "/v1/notifications/"+n.ID.String(), `{"scheduledAt":null}`)
		var got Notification
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &got) != nil {
			t.Fatalf("got %d %s", rec.Code, rec.Body)
		}
		if got.Status != StatusPending || got.ScheduledAt != nil {
			t.Fatalf("got status %s scheduledAt %v, want pending without a schedule", got.Status, got.ScheduledAt)
		}
	})
	t.Run("past the horizon answers 400 naming scheduledAt", func(t *testing.T) {
		n := create(t, inADay)
		far := time.Now().Add(notification.ScheduleHorizon + 24*time.Hour).UTC().Format(time.RFC3339)
		rec := do(t, http.MethodPatch, "/v1/notifications/"+n.ID.String(), `{"scheduledAt":"`+far+`"}`)
		e := decodeError(t, rec)
		if rec.Code != http.StatusBadRequest || e.Code != "invalid_request" || len(e.Details) != 1 || e.Details[0].Field != "scheduledAt" {
			t.Fatalf("got %d %s", rec.Code, rec.Body)
		}
	})
	t.Run("a cancelled notification answers 409 with its status", func(t *testing.T) {
		n := create(t, inADay)
		if rec := do(t, http.MethodPost, "/v1/notifications/"+n.ID.String()+"/cancel", ""); rec.Code != http.StatusOK {
			t.Fatalf("cancel: %d %s", rec.Code, rec.Body)
		}
		rec := do(t, http.MethodPatch, "/v1/notifications/"+n.ID.String(), `{"scheduledAt":null}`)
		e := decodeError(t, rec)
		if rec.Code != http.StatusConflict || e.Code != "not_reschedulable" || !strings.Contains(e.Message, "cancelled") {
			t.Fatalf("got %d %s", rec.Code, rec.Body)
		}
	})
	t.Run("an unknown id answers 404", func(t *testing.T) {
		rec := do(t, http.MethodPatch, "/v1/notifications/01a1e115-35f4-713d-8d33-fbf2f9af0c3c", `{"scheduledAt":null}`)
		if e := decodeError(t, rec); rec.Code != http.StatusNotFound || e.Code != "not_found" {
			t.Fatalf("got %d %s", rec.Code, rec.Body)
		}
	})
}
