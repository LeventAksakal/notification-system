package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestLinesAboutANotificationCarryItsStoredCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))
	req := WithCorrelationID(context.Background(), "request-id")

	log.InfoContext(req, "access")
	log.InfoContext(WithNotification(req, "n-1", "stored-id"), "about n-1")
	log.InfoContext(WithNotification(req, "n-2", ""), "about n-2, nothing stored")

	want := []struct{ correlation, notification string }{{"request-id", ""}, {"stored-id", "n-1"}, {"request-id", "n-2"}}
	dec := json.NewDecoder(&buf)
	for i, w := range want {
		var line map[string]any
		if err := dec.Decode(&line); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		if line["correlation_id"] != w.correlation || (w.notification != "" && line["notification_id"] != w.notification) {
			t.Errorf("line %d: %v, want correlation_id %q notification_id %q", i, line, w.correlation, w.notification)
		}
	}
}
