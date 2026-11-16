package intake

import (
	"strings"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func TestCursorsRoundTripAndRefuseForeignInput(t *testing.T) {
	id := "0193a5b2-7c1e-7d4f-8a2b-1c3d4e5f6a7b"
	if got, err := DecodeListCursor(EncodeListCursor(id)); err != nil || got != id {
		t.Fatalf("list cursor round trip: %q, %v", got, err)
	}
	if got, err := DecodeBatchCursor(EncodeBatchCursor(42)); err != nil || got != 42 {
		t.Fatalf("batch cursor round trip: %d, %v", got, err)
	}
	// MaxCursorLen is the longest cursor issued, so the spec's maxLength refuses no real cursor.
	if n := len(EncodeListCursor(id)); n != notification.MaxCursorLen {
		t.Errorf("list cursor is %d characters, MaxCursorLen %d", n, notification.MaxCursorLen)
	}
	if n := len(EncodeBatchCursor(notification.MaxBatchSize - 1)); n > notification.MaxCursorLen {
		t.Errorf("batch cursor is %d characters, past MaxCursorLen %d", n, notification.MaxCursorLen)
	}
	for _, tc := range []struct {
		name, cursor string
		decode       func(string) error
	}{
		{"garbage list cursor", strings.Repeat("z", 40), func(c string) error { _, err := DecodeListCursor(c); return err }},
		{"not base64", "!!!", func(c string) error { _, err := DecodeListCursor(c); return err }},
		{"batch cursor on the list", EncodeBatchCursor(3), func(c string) error { _, err := DecodeListCursor(c); return err }},
		{"list cursor on a batch", EncodeListCursor(id), func(c string) error { _, err := DecodeBatchCursor(c); return err }},
		{"list cursor with a non-uuid", EncodeListCursor("x' OR 1=1"), func(c string) error { _, err := DecodeListCursor(c); return err }},
		{"negative batch index", EncodeBatchCursor(-1), func(c string) error { _, err := DecodeBatchCursor(c); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.decode(tc.cursor)
			ve, ok := err.(*ValidationError) //nolint:errorlint // the concrete type is the contract here
			if !ok || len(ve.Details) != 1 || ve.Details[0].Field != "cursor" {
				t.Fatalf("got %v, want a validation error naming cursor", err)
			}
		})
	}
}
