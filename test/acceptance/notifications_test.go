//go:build integration

package acceptance

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// R1: Create notification requests with recipient, channel, content, and priority
// (POST returns an id; GET shows the same fields and a status).
func TestR1CreateAndRead(t *testing.T) {
	s := startStack(t, stackOptions{})

	t.Run("fields round-trip, default priority normal, status pending", func(t *testing.T) {
		r := s.createResp(t, msg("sms", "Your code is 4821"), corrID("r1-corr-given"))
		if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
			t.Fatalf("create: want 201, got %s", show(r))
		}
		if got := r.HTTPResponse.Header.Get("X-Correlation-ID"); got != "r1-corr-given" {
			t.Errorf("X-Correlation-ID echoed: want r1-corr-given, got %q", got)
		}
		created := *r.JSON201
		if created.Id == uuid.Nil {
			t.Fatalf("no id in %s", show(r))
		}
		got := s.get(t, created.Id)
		for _, n := range []apiclient.Notification{created, got} {
			if n.Id != created.Id || n.To != smsTo || n.Channel != "sms" || n.Content != "Your code is 4821" {
				t.Errorf("fields: got id=%q to=%q channel=%q content=%q", n.Id, n.To, n.Channel, n.Content)
			}
			if n.Priority != "normal" {
				t.Errorf("default priority: want normal, got %q", n.Priority)
			}
			if n.Status != "pending" {
				t.Errorf("status: want pending, got %q", n.Status)
			}
			if n.CorrelationId != "r1-corr-given" {
				t.Errorf("correlationId: want r1-corr-given, got %q", n.CorrelationId)
			}
			if n.Attempts != 0 {
				t.Errorf("attempts: want 0, got %d", n.Attempts)
			}
		}
	})

	t.Run("priority given is kept", func(t *testing.T) {
		for _, p := range []apiclient.Priority{apiclient.High, apiclient.Normal, apiclient.Low} {
			body := msg("push", "prio "+string(p))
			body.Priority = ptr(p)
			n := s.create(t, body)
			if g := s.get(t, n.Id); g.Priority != p {
				t.Errorf("priority %s: GET shows %q", p, g.Priority)
			}
		}
	})

	t.Run("correlation id generated when absent", func(t *testing.T) {
		r := s.createResp(t, msg("email", "hello"))
		if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
			t.Fatalf("create: want 201, got %s", show(r))
		}
		h := r.HTTPResponse.Header.Get("X-Correlation-ID")
		if h == "" {
			t.Fatal("no X-Correlation-ID on the response")
		}
		if got := s.get(t, r.JSON201.Id); got.CorrelationId != h {
			t.Errorf("stored correlationId %q differs from the generated header %q", got.CorrelationId, h)
		}
	})
}

// R3: Query notification status by ID
// (GET by id returns status, attempts and last error).
func TestR3GetByID(t *testing.T) {
	s := startStack(t, stackOptions{})
	n := s.create(t, msg("sms", "status please"))

	r, err := s.API.GetNotificationWithResponse(t.Context(), n.Id)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
		t.Fatalf("GET: want 200, got %s", show(r))
	}
	got := *r.JSON200
	// Presence is read from the body: the generated type decodes an absent
	// field to its zero value.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(r.Body, &obj); err != nil {
		t.Fatalf("GET: body is not a JSON object: %s (%v)", show(r), err)
	}
	if got.Status != "pending" {
		t.Errorf("status: want pending, got %v", got.Status)
	}
	if _, present := obj["attempts"]; !present || got.Attempts != 0 {
		t.Errorf("attempts: want 0, got %s", obj["attempts"])
	}
	if _, present := obj["deliveryAttempts"]; !present {
		t.Errorf("deliveryAttempts missing on GET by id: %s", show(r))
	} else if got.DeliveryAttempts == nil || len(*got.DeliveryAttempts) != 0 {
		t.Errorf("deliveryAttempts before any send: want [], got %s", obj["deliveryAttempts"])
	}

	t.Run("unknown id is 404 not_found", func(t *testing.T) {
		wantError(t, answered(t)(s.API.GetNotificationWithResponse(t.Context(), uuid.New())), http.StatusNotFound, "not_found")
	})
}

// R5: Cancel pending notifications
// (cancel before sending: never reaches the provider; cancel after sending: refused).
func TestR5Cancel(t *testing.T) {
	s := startStack(t, stackOptions{})

	marker := "r5-cancelled-" + newUUID()
	n := s.create(t, msg("sms", marker))
	c := s.cancel(t, n.Id)
	if c.Status != "cancelled" {
		t.Fatalf("cancel response status: want cancelled, got %q", c.Status)
	}
	if g := s.get(t, n.Id); g.Status != "cancelled" {
		t.Fatalf("GET after cancel: want cancelled, got %q", g.Status)
	}

	t.Run("a cancelled notification never reaches the provider", func(t *testing.T) {
		s.StartWorker()
		consistently(t, 4*time.Second, func() bool {
			if len(s.Provider.Requests(fakeprovider.Filter{Key: n.Id.String()})) != 0 {
				return false
			}
			for _, rq := range s.Provider.Requests(fakeprovider.Filter{}) {
				if rq.Content == marker {
					return false
				}
			}
			return s.storedStatus(t, n.Id) == "cancelled"
		}, "cancelled notification %s reached the provider or left cancelled (status %s)", n.Id, s.storedStatus(t, n.Id))
	})

	t.Run("cancel twice is 409 not_cancellable", func(t *testing.T) {
		wantError(t, answered(t)(s.API.CancelNotificationWithResponse(t.Context(), n.Id)), http.StatusConflict, "not_cancellable")
	})

	t.Run("a scheduled notification can be cancelled", func(t *testing.T) {
		body := msg("email", "later")
		body.ScheduledAt = ptr(time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second))
		sch := s.create(t, body)
		if got := s.cancel(t, sch.Id); got.Status != "cancelled" {
			t.Errorf("cancel scheduled: want cancelled, got %q", got.Status)
		}
	})

	t.Run("unknown id is 404 not_found", func(t *testing.T) {
		wantError(t, answered(t)(s.API.CancelNotificationWithResponse(t.Context(), uuid.New())), http.StatusNotFound, "not_found")
	})
}

// list fetches one page of GET /v1/notifications, requires 200.
func (s *stack) list(t *testing.T, p apiclient.ListNotificationsParams) apiclient.NotificationPage {
	t.Helper()
	r, err := s.API.ListNotificationsWithResponse(t.Context(), &p)
	if err != nil {
		t.Fatalf("GET /v1/notifications?%s: %v", listString(p), err)
	}
	if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
		t.Fatalf("GET /v1/notifications?%s: want 200, got %s", listString(p), show(r))
	}
	return *r.JSON200
}

// listAll walks every page of a filtered list.
func (s *stack) listAll(t *testing.T, p apiclient.ListNotificationsParams) []apiclient.Notification {
	t.Helper()
	var out []apiclient.Notification
	p.Limit = ptr(200)
	for range 100 {
		page := s.list(t, p)
		out = append(out, page.Items...)
		if deref(page.NextCursor) == "" {
			return out
		}
		p.Cursor = page.NextCursor
	}
	t.Fatal("more than 100 pages")
	return nil
}

// listString renders the parameters that are set, for failure messages.
func listString(p apiclient.ListNotificationsParams) string {
	var parts []string
	add := func(k string, v any) { parts = append(parts, fmt.Sprintf("%s=%v", k, v)) }
	if p.Status != nil {
		add("status", *p.Status)
	}
	if p.Channel != nil {
		add("channel", *p.Channel)
	}
	if p.BatchId != nil {
		add("batchId", *p.BatchId)
	}
	if p.CreatedFrom != nil {
		add("createdFrom", p.CreatedFrom.Format(time.RFC3339Nano))
	}
	if p.CreatedTo != nil {
		add("createdTo", p.CreatedTo.Format(time.RFC3339Nano))
	}
	if p.Limit != nil {
		add("limit", *p.Limit)
	}
	if p.Cursor != nil {
		add("cursor", *p.Cursor)
	}
	return strings.Join(parts, "&")
}

func idSet(ns []apiclient.Notification) map[uuid.UUID]bool {
	m := map[uuid.UUID]bool{}
	for _, n := range ns {
		m[n.Id] = true
	}
	return m
}

// R6: List notifications with filtering (status, channel, date range) and pagination
// (each filter narrows the list; pages do not repeat or skip rows while new rows arrive).
func TestR6ListFilters(t *testing.T) {
	s := startStack(t, stackOptions{})

	var all []apiclient.Notification
	for i := range 4 {
		all = append(all, s.create(t, msg("sms", fmt.Sprintf("sms %d", i))))
		all = append(all, s.create(t, msg("email", fmt.Sprintf("email %d", i))))
		all = append(all, s.create(t, msg("push", fmt.Sprintf("push %d", i))))
	}
	cancelled := s.cancel(t, all[0].Id)
	future := msg("push", "scheduled one")
	future.ScheduledAt = ptr(time.Now().UTC().Add(3 * time.Hour).Truncate(time.Second))
	scheduled := s.create(t, future)
	b := s.createBatch(t, []apiclient.NotificationCreate{msg("sms", "b0"), msg("email", "b1"), msg("push", "b2")})
	all = append(all, scheduled)
	for _, id := range b.NotificationIds {
		all = append(all, s.get(t, id))
	}
	total := len(all) // 12 singles + 1 scheduled + 3 batch items

	if got := s.listAll(t, apiclient.ListNotificationsParams{}); len(got) != total {
		t.Fatalf("unfiltered list: want %d items, got %d", total, len(got))
	}

	t.Run("status", func(t *testing.T) {
		for _, tc := range []struct {
			status apiclient.Status
			want   []uuid.UUID
		}{
			{apiclient.Cancelled, []uuid.UUID{cancelled.Id}},
			{apiclient.Scheduled, []uuid.UUID{scheduled.Id}},
		} {
			got := s.listAll(t, apiclient.ListNotificationsParams{Status: ptr(tc.status)})
			if len(got) != len(tc.want) || (len(got) > 0 && got[0].Id != tc.want[0]) {
				t.Errorf("status=%s: want %v, got %v", tc.status, tc.want, ids(got))
			}
		}
		pend := s.listAll(t, apiclient.ListNotificationsParams{Status: ptr(apiclient.Pending)})
		if len(pend) != total-2 {
			t.Errorf("status=pending: want %d (all but the cancelled and the scheduled), got %d", total-2, len(pend))
		}
		for _, n := range pend {
			if n.Status != "pending" {
				t.Errorf("status=pending returned %s with status %s", n.Id, n.Status)
			}
		}
	})

	t.Run("channel", func(t *testing.T) {
		for _, ch := range []apiclient.Channel{apiclient.Sms, apiclient.Email, apiclient.Push} {
			want := 0
			for _, n := range all {
				if n.Channel == ch {
					want++
				}
			}
			got := s.listAll(t, apiclient.ListNotificationsParams{Channel: ptr(ch)})
			if len(got) != want {
				t.Errorf("channel=%s: want %d, got %d", ch, want, len(got))
			}
			for _, n := range got {
				if n.Channel != ch {
					t.Errorf("channel=%s returned %s on %s", ch, n.Id, n.Channel)
				}
			}
		}
	})

	t.Run("batchId", func(t *testing.T) {
		got := s.listAll(t, apiclient.ListNotificationsParams{BatchId: ptr(b.Id)})
		gs := idSet(got)
		if len(got) != len(b.NotificationIds) {
			t.Errorf("batchId: want %d items, got %d", len(b.NotificationIds), len(got))
		}
		for _, id := range b.NotificationIds {
			if !gs[id] {
				t.Errorf("batchId filter misses %s", id)
			}
		}
	})

	t.Run("createdFrom inclusive, createdTo exclusive", func(t *testing.T) {
		// Pick a boundary that some rows are strictly before.
		var boundary time.Time
		for _, n := range all {
			if n.CreatedAt.After(all[0].CreatedAt) {
				boundary = n.CreatedAt
				break
			}
		}
		if boundary.IsZero() {
			t.Fatal("every row has the same createdAt; cannot pick a boundary")
		}
		var wantFrom, wantTo int
		for _, n := range all {
			if !n.CreatedAt.Before(boundary) {
				wantFrom++
			} else {
				wantTo++
			}
		}
		ts := boundary.UTC()
		from := s.listAll(t, apiclient.ListNotificationsParams{CreatedFrom: ptr(ts)})
		to := s.listAll(t, apiclient.ListNotificationsParams{CreatedTo: ptr(ts)})
		if len(from) != wantFrom {
			t.Errorf("createdFrom=%s: want %d, got %d", ts.Format(time.RFC3339Nano), wantFrom, len(from))
		}
		if len(to) != wantTo {
			t.Errorf("createdTo=%s: want %d, got %d", ts.Format(time.RFC3339Nano), wantTo, len(to))
		}
		for _, n := range from {
			if n.CreatedAt.Before(boundary) {
				t.Errorf("createdFrom returned %s created %s", n.Id, n.CreatedAt)
			}
		}
		for _, n := range to {
			if !n.CreatedAt.Before(boundary) {
				t.Errorf("createdTo returned %s created %s", n.Id, n.CreatedAt)
			}
		}
	})

	t.Run("filters combine", func(t *testing.T) {
		got := s.listAll(t, apiclient.ListNotificationsParams{Channel: ptr(apiclient.Push), Status: ptr(apiclient.Scheduled)})
		if len(got) != 1 || got[0].Id != scheduled.Id {
			t.Errorf("channel=push&status=scheduled: want [%s], got %v", scheduled.Id, ids(got))
		}
	})
}

// R6: List notifications with filtering (status, channel, date range) and pagination
// (pages do not repeat or skip rows while new rows arrive).
func TestR6PaginationWhileRowsArrive(t *testing.T) {
	s := startStack(t, stackOptions{})

	var initial []apiclient.Notification
	for i := range 25 {
		initial = append(initial, s.create(t, msg([]string{"sms", "email", "push"}[i%3], fmt.Sprintf("page row %d", i))))
	}

	seen := map[uuid.UUID]int{}
	var walked []apiclient.Notification
	q := apiclient.ListNotificationsParams{Limit: ptr(7)}
	pages := 0
	for {
		p := s.list(t, q)
		pages++
		if len(p.Items) > 7 {
			t.Fatalf("page %d: limit 7 but %d items", pages, len(p.Items))
		}
		for _, n := range p.Items {
			seen[n.Id]++
		}
		walked = append(walked, p.Items...)
		// New rows arrive between pages.
		for j := range 3 {
			s.create(t, msg("sms", fmt.Sprintf("arrived after page %d #%d", pages, j)))
		}
		if deref(p.NextCursor) == "" {
			break
		}
		if pages > 50 {
			t.Fatal("pagination does not end")
		}
		q.Cursor = p.NextCursor
	}

	if pages < 4 {
		t.Errorf("25 rows at limit 7: want at least 4 pages, got %d", pages)
	}
	for id, c := range seen {
		if c > 1 {
			t.Errorf("row %s appeared on %d pages", id, c)
		}
	}
	for _, n := range initial {
		if seen[n.Id] != 1 {
			t.Errorf("row %s that existed at the first page was seen %d times", n.Id, seen[n.Id])
		}
	}
	for i := 1; i < len(walked); i++ {
		if !newerFirst(walked[i-1], walked[i]) {
			t.Errorf("not newest first at %d: %s (%s) then %s (%s)", i,
				walked[i-1].Id, walked[i-1].CreatedAt.Format(time.RFC3339Nano),
				walked[i].Id, walked[i].CreatedAt.Format(time.RFC3339Nano))
			break
		}
	}
	if len(walked) > 0 && walked[0].Id != initial[len(initial)-1].Id {
		t.Errorf("first item of the first page: want newest %s, got %s", initial[len(initial)-1].Id, walked[0].Id)
	}

	t.Run("limit out of range is 400", func(t *testing.T) {
		for _, l := range []int{0, 201} {
			r := answered(t)(s.API.ListNotificationsWithResponse(t.Context(), &apiclient.ListNotificationsParams{Limit: ptr(l)}))
			if r.StatusCode() != http.StatusBadRequest {
				t.Errorf("limit=%d: want 400, got %s", l, show(r))
			}
		}
	})

	t.Run("garbage cursor is 400", func(t *testing.T) {
		r := answered(t)(s.API.ListNotificationsWithResponse(t.Context(), &apiclient.ListNotificationsParams{Cursor: ptr(strings.Repeat("z", 40))}))
		if r.StatusCode() != http.StatusBadRequest {
			t.Errorf("garbage cursor: want 400, got %s", show(r))
		}
	})
}
