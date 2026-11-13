package store

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMinIDAtBoundsEveryIDOfItsMillisecond(t *testing.T) {
	for range 1000 {
		id := uuid.Must(uuid.NewV7())
		sec, nsec := id.Time().UnixTime()
		ms := time.Unix(sec, nsec).UTC().Truncate(time.Millisecond)
		s := id.String()
		if lo := MinIDAt(ms); lo > s {
			t.Fatalf("MinIDAt(%s) = %s above %s", ms, lo, s)
		}
		if hi := MinIDAt(ms.Add(time.Millisecond)); hi <= s {
			t.Fatalf("MinIDAt(next ms) = %s not above %s", hi, s)
		}
		// A sub-millisecond bound rounds up: created_at (ms) >= ms+0.5 means ms+1.
		if up := MinIDAt(ms.Add(500 * time.Microsecond)); up != MinIDAt(ms.Add(time.Millisecond)) {
			t.Fatalf("sub-millisecond bound not rounded up: %s", up)
		}
	}
}

func TestListQueryHoldsOnlyTheFiltersSet(t *testing.T) {
	q, args := ListNotificationsQuery(ListFilter{Channel: "sms", Limit: 10})
	if strings.Contains(q, "IS NULL") || !strings.Contains(q, "FORCE INDEX (list_channel_idx)") || len(args) != 2 {
		t.Fatalf("query %s args %v", q, args)
	}
	if _, err := uuid.Parse(MinIDAt(time.Now())); err != nil {
		t.Fatal(err)
	}
}
