package wake

import (
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func TestHintsTravelAsTheyWereSent(t *testing.T) {
	due := time.Date(2026, 11, 15, 9, 0, 0, 123456000, time.UTC)
	h := notification.Wake{Channel: notification.ChannelSMS, Priority: notification.PriorityHigh, Due: due}
	b, err := encode(h)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decode(notification.ChannelSMS, b)
	if err != nil || got != h {
		t.Fatalf("round trip: got %+v, %v; want %+v", got, err, h)
	}
	for name, payload := range map[string]string{
		"not json":         "{",
		"unknown priority": `{"priority":"urgent","due":"2026-11-15T09:00:00Z"}`,
	} {
		t.Run("a malformed hint is refused: "+name, func(t *testing.T) {
			if _, err := decode(notification.ChannelSMS, []byte(payload)); err == nil {
				t.Fatalf("decode %q: want an error", payload)
			}
		})
	}
}
