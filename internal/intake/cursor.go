package intake

import (
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Cursors are opaque to clients: base64url of a kind prefix and the key of
// the last row of the previous page. A cursor of one list is refused by the
// other.
const (
	cursorNotifications = "n1:"
	cursorBatchItems    = "b1:"
)

var errBadCursor = invalid("cursor", "not a cursor this list issued")

// EncodeListCursor returns the cursor after the notification with this id
// (lists run newest first, by id).
func EncodeListCursor(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorNotifications + id))
}

// DecodeListCursor returns the id a list cursor points after.
func DecodeListCursor(c string) (string, error) {
	v, err := decodeCursor(c, cursorNotifications)
	if err != nil {
		return "", err
	}
	if _, err := uuid.Parse(v); err != nil || len(v) != 36 {
		return "", errBadCursor
	}
	return v, nil
}

// EncodeBatchCursor returns the cursor after the batch item at index.
func EncodeBatchCursor(index int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorBatchItems + strconv.Itoa(index)))
}

// DecodeBatchCursor returns the batch index a batch cursor points after.
func DecodeBatchCursor(c string) (int, error) {
	v, err := decodeCursor(c, cursorBatchItems)
	if err != nil {
		return 0, err
	}
	i, err := strconv.Atoi(v)
	if err != nil || i < 0 {
		return 0, errBadCursor
	}
	return i, nil
}

func decodeCursor(c, prefix string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return "", errBadCursor
	}
	v, ok := strings.CutPrefix(string(raw), prefix)
	if !ok {
		return "", errBadCursor
	}
	return v, nil
}
