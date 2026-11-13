//go:build integration

package acceptance

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/core"
)

func batchItems(n int) []map[string]any {
	items := make([]map[string]any, n)
	chans := []string{"sms", "email", "push"}
	for i := range items {
		items[i] = msg(chans[i%3], fmt.Sprintf("batch item %d", i))
	}
	return items
}

// R2: Support batch creation (up to 1000 notifications per request)
// (1000 items accepted in one request; 1001 refused with a clear error).
func TestR2BatchCreate(t *testing.T) {
	s := startStack(t, stackOptions{})

	// 1000 accepted in one request. Not a subtest: unless this works, the
	// refusals below prove nothing (the spec validator alone refuses 1001).
	b := s.createBatch(t, batchItems(core.MaxBatchSize))
	if b.ID == "" || b.Total != core.MaxBatchSize || len(b.NotificationIDs) != core.MaxBatchSize {
		t.Fatalf("batch: id=%q total=%d ids=%d", b.ID, b.Total, len(b.NotificationIDs))
	}
	if n := s.count(t, "SELECT COUNT(*) FROM notifications WHERE batch_id = ?", b.ID); n != core.MaxBatchSize {
		t.Errorf("rows of the batch: want %d, got %d", core.MaxBatchSize, n)
	}
	if n := s.count(t, "SELECT COUNT(*) FROM batches"); n != 1 {
		t.Errorf("batch rows: want 1, got %d", n)
	}
	uniq := map[string]bool{}
	for _, id := range b.NotificationIDs {
		uniq[id] = true
	}
	if len(uniq) != core.MaxBatchSize {
		t.Errorf("notificationIds: want %d distinct, got %d", core.MaxBatchSize, len(uniq))
	}

	before := s.count(t, "SELECT COUNT(*) FROM notifications")

	t.Run("1001 refused", func(t *testing.T) {
		r := s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": batchItems(core.MaxBatchSize + 1)})
		wantError(t, r, http.StatusBadRequest, "invalid_request", "notifications")
		if n := s.count(t, "SELECT COUNT(*) FROM notifications"); n != before {
			t.Errorf("rows after a refused batch: want %d, got %d", before, n)
		}
	})

	t.Run("999 valid plus 1 invalid stores nothing and names index and field", func(t *testing.T) {
		items := batchItems(core.MaxBatchSize)
		const bad = 500
		items[bad] = map[string]any{"to": "not-a-phone-number", "channel": "sms", "content": "x"}
		r := s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": items})
		e := wantError(t, r, http.StatusBadRequest, "invalid_request")
		if !slices.ContainsFunc(e.Details, func(d errorDetail) bool {
			return d.Index != nil && *d.Index == bad && d.Field == "to"
		}) {
			t.Errorf("details do not name index %d field to: %s", bad, r)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM notifications"); n != before {
			t.Errorf("rows after a refused batch: want %d, got %d", before, n)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM batches"); n != 1 {
			t.Errorf("batch rows after a refused batch: want 1, got %d", n)
		}
	})

	t.Run("empty batch refused", func(t *testing.T) {
		r := s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": []any{}})
		wantError(t, r, http.StatusBadRequest, "invalid_request")
	})
}

// R4: Query notification status by batch ID
// (GET by batch id returns counts per status and the items).
func TestR4GetBatch(t *testing.T) {
	s := startStack(t, stackOptions{})

	items := batchItems(5)
	items[3]["scheduledAt"] = time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339)
	b := s.createBatch(t, items)
	if len(b.NotificationIDs) != 5 {
		t.Fatalf("batch ids: want 5, got %d", len(b.NotificationIDs))
	}
	s.cancel(t, b.NotificationIDs[1])

	r := s.getJSON(t, "/v1/batches/"+b.ID)
	if r.Status != http.StatusOK {
		t.Fatalf("GET batch: want 200, got %s", r)
	}
	var st batchStatus
	r.decode(t, &st)
	if st.ID != b.ID || st.Total != 5 {
		t.Errorf("batch: want id %s total 5, got id %s total %d", b.ID, st.ID, st.Total)
	}
	want := statusCounts{Scheduled: 1, Pending: 3, Cancelled: 1}
	if st.Counts != want {
		t.Errorf("counts: want %+v, got %+v", want, st.Counts)
	}
	if got := ids(st.Items); !slices.Equal(got, b.NotificationIDs) {
		t.Errorf("items: want request order %v, got %v", b.NotificationIDs, got)
	}
	for i, n := range st.Items {
		wantStatus := map[int]string{1: "cancelled", 3: "scheduled"}[i]
		if wantStatus == "" {
			wantStatus = "pending"
		}
		if n.Status != wantStatus {
			t.Errorf("item %d: want %s, got %s", i, wantStatus, n.Status)
		}
		if n.BatchID != b.ID {
			t.Errorf("item %d: batchId %q, want %s", i, n.BatchID, b.ID)
		}
	}

	t.Run("unknown batch is 404 not_found", func(t *testing.T) {
		wantError(t, s.getJSON(t, "/v1/batches/"+newUUID()), http.StatusNotFound, "not_found")
	})
}

// R4: Query notification status by batch ID
// (the items, page by page: every item once, in request order).
func TestR4GetBatchPaginated(t *testing.T) {
	s := startStack(t, stackOptions{})
	b := s.createBatch(t, batchItems(23))

	var walked []string
	q := url.Values{"limit": {"5"}}
	pages := 0
	for {
		r := s.getJSON(t, "/v1/batches/"+b.ID+"?"+q.Encode())
		if r.Status != http.StatusOK {
			t.Fatalf("GET batch page %d: want 200, got %s", pages+1, r)
		}
		var st batchStatus
		r.decode(t, &st)
		pages++
		if len(st.Items) > 5 {
			t.Fatalf("page %d: limit 5 but %d items", pages, len(st.Items))
		}
		if st.Total != 23 || st.Counts.Pending != 23 {
			t.Errorf("page %d: total %d, pending %d; want 23 and 23", pages, st.Total, st.Counts.Pending)
		}
		walked = append(walked, ids(st.Items)...)
		if st.NextCursor == "" {
			break
		}
		if pages > 20 {
			t.Fatal("batch pagination does not end")
		}
		q.Set("cursor", st.NextCursor)
	}
	if pages != 5 {
		t.Errorf("23 items at limit 5: want 5 pages, got %d", pages)
	}
	if !slices.Equal(walked, b.NotificationIDs) {
		t.Errorf("walked items differ from the request order:\nwant %v\ngot  %v", b.NotificationIDs, walked)
	}
}
