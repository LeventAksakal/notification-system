//go:build integration

package intake_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
	"github.com/LeventAksakal/notification-system/test/support"
)

// Every line about a notification carries the correlation id stored on it,
// whichever request writes the line; a later request's own id rides along as
// request_correlation_id (decision 0019, owner item 0006 §4, option a).
func TestLinesAboutANotificationCarryItsStoredCorrelationIDWhicheverRequestWritesThem(t *testing.T) {
	db, _ := support.MySQL(t)
	svc := intake.New(db, discardEvents{}, time.Hour)
	logs := captureLogs(t)

	const stored, replay, cancel, retry = "corr-create", "corr-replay", "corr-cancel", "corr-retry"
	content := "log capture"
	req := intake.Request{To: "+905551234567", Channel: notification.ChannelSMS, Content: &content}
	key := "log-capture-" + time.Now().Format("150405.000000000")

	n, _, err := svc.Create(telemetry.WithCorrelationID(t.Context(), stored), req,
		intake.Meta{IdempotencyKey: key, CorrelationID: stored})
	if err != nil {
		t.Fatal(err)
	}
	if _, replayed, err := svc.Create(telemetry.WithCorrelationID(t.Context(), replay), req,
		intake.Meta{IdempotencyKey: key, CorrelationID: replay}); err != nil || !replayed {
		t.Fatalf("replay: replayed %v, err %v", replayed, err)
	}
	if _, err := svc.Cancel(telemetry.WithCorrelationID(t.Context(), cancel), n.ID); err != nil {
		t.Fatal(err)
	}
	// A second notification with the same stored id, failed by the worker
	// (set here directly), then retried by a request with its own id.
	f, _, err := svc.Create(telemetry.WithCorrelationID(t.Context(), stored), req, intake.Meta{CorrelationID: stored})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`UPDATE notifications SET status = 'failed', failure_kind = 'exhausted', finished_at = NOW(3) WHERE id = ?`, f.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Retry(telemetry.WithCorrelationID(t.Context(), retry), f.ID); err != nil {
		t.Fatal(err)
	}

	expectLines(t, logs, n.ID, stored, map[string]string{ // msg -> request_correlation_id ("" when the line has none)
		"notification accepted":        "",
		"notification create replayed": replay,
		"notification cancelled":       cancel,
	})
	expectLines(t, logs, f.ID, stored, map[string]string{"notification replayed": retry})
}

// expectLines checks that each line in want (msg -> request_correlation_id)
// exists about id and carries stored as correlation_id.
func expectLines(t *testing.T, logs *capturedLogs, id, stored string, want map[string]string) {
	t.Helper()
	for _, l := range logs.about(t, id) {
		msg, _ := l["msg"].(string)
		reqID, ok := want[msg]
		if !ok {
			continue
		}
		delete(want, msg)
		if l["correlation_id"] != stored {
			t.Errorf("%q: correlation_id want the stored %q, got %v", msg, stored, l["correlation_id"])
		}
		if got, _ := l["request_correlation_id"].(string); got != reqID {
			t.Errorf("%q: request_correlation_id want %q, got %q", msg, reqID, got)
		}
	}
	for msg := range want {
		t.Errorf("no %q line about %s", msg, id)
	}
}

// discardEvents is a notification.Publisher that drops every event.
type discardEvents struct{}

func (discardEvents) Publish(context.Context, notification.Event) error { return nil }

// capturedLogs holds the JSON lines the default logger wrote during a test.
type capturedLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capturedLogs) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

// captureLogs points the default logger, with the ids the context carries
// (as telemetry.Setup does), at a buffer until the test ends.
func captureLogs(t *testing.T) *capturedLogs {
	t.Helper()
	c := &capturedLogs{}
	prev := slog.Default()
	slog.SetDefault(slog.New(telemetry.NewHandler(slog.NewJSONHandler(c, nil))))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

// about returns the lines whose notification_id is id.
func (c *capturedLogs) about(t *testing.T, id string) []map[string]any {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []map[string]any
	dec := json.NewDecoder(bytes.NewReader(c.buf.Bytes()))
	for dec.More() {
		var l map[string]any
		if err := dec.Decode(&l); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		if l["notification_id"] == id {
			out = append(out, l)
		}
	}
	return out
}
