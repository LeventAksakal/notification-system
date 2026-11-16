//go:build integration

package acceptance

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// O1: Operational: storage does not grow forever (row proposed by the
// db-rules lane for docs/REQUIREMENTS.md: "Finished notifications (sent,
// failed, cancelled) and their delivery attempts are deleted once finished
// longer than APP_NOTIFICATION_RETENTION ago; pending, processing, scheduled
// and recently finished ones, and templates, stay; a batch older than the
// retention goes when none of its notifications is left"; decision 0025).
// Dials: APP_NOTIFICATION_RETENTION=1h, APP_IDEMPOTENCY_RETENTION=1h,
// APP_IDEMPOTENCY_SWEEP_INTERVAL=1s (the sweep tick runs retention too).
// The expired rows are back-dated with SQL; the status does not change, so
// the state machine allows it.
func TestO1RetentionDeletesExpiredFinished(t *testing.T) {
	const failTo = "+905550000002"
	s := startStack(t, stackOptions{Workers: 1, Env: dlFastEnv(map[string]string{
		"APP_NOTIFICATION_RETENTION":     "1h",
		"APP_IDEMPOTENCY_RETENTION":      "1h",
		"APP_IDEMPOTENCY_SWEEP_INTERVAL": "1s",
	})})
	s.dlFaults(t, fakeprovider.Fault{To: failTo, Mode: fakeprovider.ModeStatus, Status: http.StatusBadRequest})

	sent := s.create(t, msg("sms", "O1 will be sent"))
	failBody := msg("sms", "O1 will fail")
	failBody.To = failTo
	failed := s.create(t, failBody)
	later := msg("push", "O1 will be cancelled")
	later.ScheduledAt = ptr(time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond))
	cancelled := s.create(t, later)
	s.cancel(t, cancelled.Id)
	keepBody := msg("email", "O1 scheduled an hour ahead")
	keepBody.ScheduledAt = ptr(time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond))
	scheduled := s.create(t, keepBody)
	batch := s.createBatch(t, []apiclient.NotificationCreate{msg("email", "O1 batch 0"), msg("push", "O1 batch 1")})
	s.putTemplate(t, "o1-keep", "Kept {{name}}")

	dlEventually(t, 30*time.Second, "the three single notifications and the batch are final", func() (bool, string) {
		a, b, c := s.storedStatus(t, sent.Id), s.storedStatus(t, failed.Id), s.storedStatus(t, cancelled.Id)
		b0, b1 := s.storedStatus(t, batch.NotificationIds[0]), s.storedStatus(t, batch.NotificationIds[1])
		return a == "sent" && b == "failed" && c == "cancelled" && b0 == "sent" && b1 == "sent",
			fmt.Sprintf("sent=%s failed=%s cancelled=%s batch=%s,%s", a, b, c, b0, b1)
	})
	expired := []uuid.UUID{sent.Id, failed.Id, cancelled.Id, batch.NotificationIds[0], batch.NotificationIds[1]}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, id := range expired {
		if _, err := s.DB.ExecContext(ctx, "UPDATE notifications SET finished_at = NOW(3) - INTERVAL 2 HOUR, created_at = NOW(3) - INTERVAL 2 HOUR WHERE id = ?", id.String()); err != nil {
			t.Fatalf("back-date %s: %v", id, err)
		}
	}
	if _, err := s.DB.ExecContext(ctx, "UPDATE batches SET created_at = NOW(3) - INTERVAL 2 HOUR WHERE id = ?", batch.Id.String()); err != nil {
		t.Fatalf("back-date batch %s: %v", batch.Id, err)
	}

	recent := s.create(t, msg("sms", "O1 recent"))
	s.waitStatus(t, recent.Id, "sent", 30*time.Second)

	gone := func(id uuid.UUID) bool {
		r, err := s.API.GetNotificationWithResponse(t.Context(), id)
		return err == nil && r.StatusCode() == http.StatusNotFound
	}
	dlEventually(t, 10*time.Second, "the expired notifications and their batch answer 404", func() (bool, string) {
		var left []string
		for _, id := range expired {
			if !gone(id) {
				left = append(left, id.String())
			}
		}
		br, err := s.API.GetBatchWithResponse(t.Context(), batch.Id, nil)
		batchGone := err == nil && br.StatusCode() == http.StatusNotFound
		return len(left) == 0 && batchGone, fmt.Sprintf("still answering: %v; batch gone %v", left, batchGone)
	})
	for _, id := range expired {
		if n := s.count(t, "SELECT COUNT(*) FROM delivery_attempts WHERE notification_id = ?", id.String()); n != 0 {
			t.Errorf("%s: %d delivery_attempts rows left after retention, want 0", id, n)
		}
	}
	if r := answered(t)(s.API.RetryNotificationWithResponse(t.Context(), failed.Id)); r.StatusCode() != http.StatusNotFound {
		t.Errorf("replay of the expired failed notification: want 404, got %s", show(r))
	}

	if g := s.get(t, scheduled.Id); g.Status != "scheduled" {
		t.Errorf("scheduled notification: want status scheduled, got %q", g.Status)
	}
	if g := s.get(t, recent.Id); g.Status != "sent" || len(history(g)) == 0 {
		t.Errorf("recent notification: want sent with its attempts, got status %q and %d attempts", g.Status, len(history(g)))
	}
	if r := answered(t)(s.API.GetTemplateWithResponse(t.Context(), "o1-keep")); r.StatusCode() != http.StatusOK {
		t.Errorf("template after retention: want 200, got %s", show(r))
	}
}

// R5: Cancel pending notifications ("Cancel after sending: refused") and
// decision 0005 "every rule at every layer": the database itself refuses a
// status change the state machine forbids, even from a writer that skips the
// service. Here cancelled -> sent is written straight into MySQL.
func TestR5DatabaseRefusesAForbiddenStatusChange(t *testing.T) {
	s := startStack(t, stackOptions{})
	b := msg("sms", "R5 cancelled, then forced to sent")
	b.ScheduledAt = ptr(time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond))
	n := s.create(t, b)
	s.cancel(t, n.Id)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := s.DB.ExecContext(ctx, "UPDATE notifications SET status = 'sent', finished_at = NOW(3), provider_message_id = 'x', attempts = 1 WHERE id = ?", n.Id.String())
	var me *mysql.MySQLError
	switch {
	case err == nil:
		t.Errorf("the database accepted cancelled -> sent")
	case !errors.As(err, &me) || me.Number != 3819 || !strings.Contains(me.Message, "status_transition"):
		t.Errorf("want MySQL error 3819 naming status_transition, got %v", err)
	}
	if g := s.get(t, n.Id); g.Status != "cancelled" {
		t.Errorf("after the refused write: status %q, want cancelled", g.Status)
	}
}
