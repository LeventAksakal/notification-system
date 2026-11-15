//go:build integration

package acceptance

import (
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

func batchItems(n int) []apiclient.NotificationCreate {
	items := make([]apiclient.NotificationCreate, n)
	chans := []string{"sms", "email", "push"}
	for i := range items {
		items[i] = msg(chans[i%3], fmt.Sprintf("batch item %d", i))
	}
	return items
}

// postBatch posts a batch through the typed client and returns the answer
// unchecked, for the refusals.
func (s *stack) postBatch(t *testing.T, items []apiclient.NotificationCreate, opts ...reqOpt) typedAnswer {
	t.Helper()
	return s.createBatchResp(t, items, opts...)
}

// R2: Support batch creation (up to 1000 notifications per request)
// (1000 items accepted in one request; 1001 refused with a clear error).
func TestR2BatchCreate(t *testing.T) {
	s := startStack(t, stackOptions{})

	// 1000 accepted in one request. Not a subtest: unless this works, the
	// refusals below prove nothing (the spec validator alone refuses 1001).
	b := s.createBatch(t, batchItems(notification.MaxBatchSize))
	if b.Id == uuid.Nil || b.Total != notification.MaxBatchSize || len(b.NotificationIds) != notification.MaxBatchSize {
		t.Fatalf("batch: id=%q total=%d ids=%d", b.Id, b.Total, len(b.NotificationIds))
	}
	if n := s.count(t, "SELECT COUNT(*) FROM notifications WHERE batch_id = ?", b.Id.String()); n != notification.MaxBatchSize {
		t.Errorf("rows of the batch: want %d, got %d", notification.MaxBatchSize, n)
	}
	if n := s.count(t, "SELECT COUNT(*) FROM batches"); n != 1 {
		t.Errorf("batch rows: want 1, got %d", n)
	}
	uniq := map[uuid.UUID]bool{}
	for _, id := range b.NotificationIds {
		uniq[id] = true
	}
	if len(uniq) != notification.MaxBatchSize {
		t.Errorf("notificationIds: want %d distinct, got %d", notification.MaxBatchSize, len(uniq))
	}

	before := s.count(t, "SELECT COUNT(*) FROM notifications")

	t.Run("1001 refused", func(t *testing.T) {
		r := s.postBatch(t, batchItems(notification.MaxBatchSize+1))
		wantError(t, r, http.StatusBadRequest, "invalid_request", "notifications")
		if n := s.count(t, "SELECT COUNT(*) FROM notifications"); n != before {
			t.Errorf("rows after a refused batch: want %d, got %d", before, n)
		}
	})

	t.Run("999 valid plus 1 invalid stores nothing and names index and field", func(t *testing.T) {
		items := batchItems(notification.MaxBatchSize)
		const bad = 500
		items[bad] = apiclient.NotificationCreate{To: "not-a-phone-number", Channel: apiclient.Sms, Content: ptr("x")}
		r := s.postBatch(t, items)
		e := wantError(t, r, http.StatusBadRequest, "invalid_request")
		if !slices.ContainsFunc(deref(e.Details), func(d apiclient.ErrorDetail) bool {
			return d.Index != nil && *d.Index == bad && d.Field == "to"
		}) {
			t.Errorf("details do not name index %d field to: %s", bad, show(r))
		}
		if n := s.count(t, "SELECT COUNT(*) FROM notifications"); n != before {
			t.Errorf("rows after a refused batch: want %d, got %d", before, n)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM batches"); n != 1 {
			t.Errorf("batch rows after a refused batch: want 1, got %d", n)
		}
	})

	t.Run("empty batch refused", func(t *testing.T) {
		r := s.postBatch(t, []apiclient.NotificationCreate{})
		wantError(t, r, http.StatusBadRequest, "invalid_request")
	})
}

// R4: Query notification status by batch ID
// (GET by batch id returns counts per status and the items).
func TestR4GetBatch(t *testing.T) {
	s := startStack(t, stackOptions{})

	items := batchItems(5)
	items[3].ScheduledAt = ptr(time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second))
	b := s.createBatch(t, items)
	if len(b.NotificationIds) != 5 {
		t.Fatalf("batch ids: want 5, got %d", len(b.NotificationIds))
	}
	s.cancel(t, b.NotificationIds[1])

	st := s.getBatch(t, b.Id, apiclient.GetBatchParams{})
	if st.Id != b.Id || st.Total != 5 {
		t.Errorf("batch: want id %s total 5, got id %s total %d", b.Id, st.Id, st.Total)
	}
	want := apiclient.StatusCounts{Scheduled: 1, Pending: 3, Cancelled: 1}
	if st.Counts != want {
		t.Errorf("counts: want %+v, got %+v", want, st.Counts)
	}
	if got := ids(st.Items); !slices.Equal(got, b.NotificationIds) {
		t.Errorf("items: want request order %v, got %v", b.NotificationIds, got)
	}
	for i, n := range st.Items {
		wantStatus := map[int]apiclient.Status{1: apiclient.Cancelled, 3: apiclient.Scheduled}[i]
		if wantStatus == "" {
			wantStatus = apiclient.Pending
		}
		if n.Status != wantStatus {
			t.Errorf("item %d: want %s, got %s", i, wantStatus, n.Status)
		}
		if n.BatchId == nil || *n.BatchId != b.Id {
			t.Errorf("item %d: batchId %v, want %s", i, deref(n.BatchId), b.Id)
		}
	}

	t.Run("unknown batch is 404 not_found", func(t *testing.T) {
		wantError(t, answered(t)(s.API.GetBatchWithResponse(t.Context(), uuid.New(), nil)), http.StatusNotFound, "not_found")
	})
}

// R4: Query notification status by batch ID
// (the items, page by page: every item once, in request order).
func TestR4GetBatchPaginated(t *testing.T) {
	s := startStack(t, stackOptions{})
	b := s.createBatch(t, batchItems(23))

	var walked []uuid.UUID
	q := apiclient.GetBatchParams{Limit: ptr(5)}
	pages := 0
	for {
		st := s.getBatch(t, b.Id, q)
		pages++
		if len(st.Items) > 5 {
			t.Fatalf("page %d: limit 5 but %d items", pages, len(st.Items))
		}
		if st.Total != 23 || st.Counts.Pending != 23 {
			t.Errorf("page %d: total %d, pending %d; want 23 and 23", pages, st.Total, st.Counts.Pending)
		}
		walked = append(walked, ids(st.Items)...)
		if deref(st.NextCursor) == "" {
			break
		}
		if pages > 20 {
			t.Fatal("batch pagination does not end")
		}
		q.Cursor = st.NextCursor
	}
	if pages != 5 {
		t.Errorf("23 items at limit 5: want 5 pages, got %d", pages)
	}
	if !slices.Equal(walked, b.NotificationIds) {
		t.Errorf("walked items differ from the request order:\nwant %v\ngot  %v", b.NotificationIds, walked)
	}
}
