//go:build integration

package acceptance

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

// R1: Create notification requests with recipient, channel, content, and priority
// (POST returns an id; GET shows the same fields and a status).
func TestR1CreateAndRead(t *testing.T) {
	s := startStack(t, stackOptions{})

	t.Run("fields round-trip, default priority normal, status pending", func(t *testing.T) {
		r := s.postJSON(t, "/v1/notifications", msg("sms", "Your code is 4821"), "X-Correlation-ID", "r1-corr-given")
		if r.Status != http.StatusCreated {
			t.Fatalf("create: want 201, got %s", r)
		}
		if got := r.Header.Get("X-Correlation-ID"); got != "r1-corr-given" {
			t.Errorf("X-Correlation-ID echoed: want r1-corr-given, got %q", got)
		}
		var created apiNotification
		r.decode(t, &created)
		if created.ID == "" {
			t.Fatalf("no id in %s", r)
		}
		got := s.get(t, created.ID)
		for _, n := range []apiNotification{created, got} {
			if n.ID != created.ID || n.To != smsTo || n.Channel != "sms" || n.Content != "Your code is 4821" {
				t.Errorf("fields: got id=%q to=%q channel=%q content=%q", n.ID, n.To, n.Channel, n.Content)
			}
			if n.Priority != "normal" {
				t.Errorf("default priority: want normal, got %q", n.Priority)
			}
			if n.Status != "pending" {
				t.Errorf("status: want pending, got %q", n.Status)
			}
			if n.CorrelationID != "r1-corr-given" {
				t.Errorf("correlationId: want r1-corr-given, got %q", n.CorrelationID)
			}
			if n.Attempts != 0 {
				t.Errorf("attempts: want 0, got %d", n.Attempts)
			}
		}
	})

	t.Run("priority given is kept", func(t *testing.T) {
		for _, p := range []string{"high", "normal", "low"} {
			body := msg("push", "prio "+p)
			body["priority"] = p
			n := s.create(t, body)
			if g := s.get(t, n.ID); g.Priority != p {
				t.Errorf("priority %s: GET shows %q", p, g.Priority)
			}
		}
	})

	t.Run("correlation id generated when absent", func(t *testing.T) {
		r := s.postJSON(t, "/v1/notifications", msg("email", "hello"))
		if r.Status != http.StatusCreated {
			t.Fatalf("create: want 201, got %s", r)
		}
		h := r.Header.Get("X-Correlation-ID")
		if h == "" {
			t.Fatal("no X-Correlation-ID on the response")
		}
		var n apiNotification
		r.decode(t, &n)
		if got := s.get(t, n.ID); got.CorrelationID != h {
			t.Errorf("stored correlationId %q differs from the generated header %q", got.CorrelationID, h)
		}
	})
}

// R3: Query notification status by ID
// (GET by id returns status, attempts and last error).
func TestR3GetByID(t *testing.T) {
	s := startStack(t, stackOptions{})
	n := s.create(t, msg("sms", "status please"))

	r := s.getJSON(t, "/v1/notifications/"+n.ID)
	if r.Status != http.StatusOK {
		t.Fatalf("GET: want 200, got %s", r)
	}
	obj := r.object(t)
	if obj["status"] != "pending" {
		t.Errorf("status: want pending, got %v", obj["status"])
	}
	if a, ok := obj["attempts"].(float64); !ok || a != 0 {
		t.Errorf("attempts: want 0, got %v", obj["attempts"])
	}
	da, present := obj["deliveryAttempts"]
	if !present {
		t.Errorf("deliveryAttempts missing on GET by id: %s", r)
	} else if arr, ok := da.([]any); !ok || len(arr) != 0 {
		t.Errorf("deliveryAttempts before any send: want [], got %v", da)
	}

	t.Run("unknown id is 404 not_found", func(t *testing.T) {
		wantError(t, s.getJSON(t, "/v1/notifications/"+newUUID()), http.StatusNotFound, "not_found")
	})
}

// R5: Cancel pending notifications
// (cancel before sending: never reaches the provider; cancel after sending: refused).
func TestR5Cancel(t *testing.T) {
	s := startStack(t, stackOptions{})

	marker := "r5-cancelled-" + newUUID()
	n := s.create(t, msg("sms", marker))
	c := s.cancel(t, n.ID)
	if c.Status != "cancelled" {
		t.Fatalf("cancel response status: want cancelled, got %q", c.Status)
	}
	if g := s.get(t, n.ID); g.Status != "cancelled" {
		t.Fatalf("GET after cancel: want cancelled, got %q", g.Status)
	}

	t.Run("a cancelled notification never reaches the provider", func(t *testing.T) {
		s.StartWorker()
		consistently(t, 4*time.Second, func() bool {
			if len(s.Provider.Requests(fakeprovider.Filter{Key: n.ID})) != 0 {
				return false
			}
			for _, rq := range s.Provider.Requests(fakeprovider.Filter{}) {
				if rq.Content == marker {
					return false
				}
			}
			return s.storedStatus(t, n.ID) == "cancelled"
		}, "cancelled notification %s reached the provider or left cancelled (status %s)", n.ID, s.storedStatus(t, n.ID))
	})

	t.Run("cancel twice is 409 not_cancellable", func(t *testing.T) {
		wantError(t, s.postJSON(t, "/v1/notifications/"+n.ID+"/cancel", nil), http.StatusConflict, "not_cancellable")
	})

	t.Run("a scheduled notification can be cancelled", func(t *testing.T) {
		body := msg("email", "later")
		body["scheduledAt"] = time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339)
		sch := s.create(t, body)
		if got := s.cancel(t, sch.ID); got.Status != "cancelled" {
			t.Errorf("cancel scheduled: want cancelled, got %q", got.Status)
		}
	})

	t.Run("unknown id is 404 not_found", func(t *testing.T) {
		wantError(t, s.postJSON(t, "/v1/notifications/"+newUUID()+"/cancel", nil), http.StatusNotFound, "not_found")
	})
}

// list fetches one page of GET /v1/notifications, requires 200.
func (s *stack) list(t *testing.T, q url.Values) notificationPage {
	t.Helper()
	path := "/v1/notifications"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	r := s.getJSON(t, path)
	if r.Status != http.StatusOK {
		t.Fatalf("GET %s: want 200, got %s", path, r)
	}
	var p notificationPage
	r.decode(t, &p)
	return p
}

// listAll walks every page of a filtered list.
func (s *stack) listAll(t *testing.T, q url.Values) []apiNotification {
	t.Helper()
	var out []apiNotification
	q = cloneValues(q)
	q.Set("limit", "200")
	for range 100 {
		p := s.list(t, q)
		out = append(out, p.Items...)
		if p.NextCursor == "" {
			return out
		}
		q.Set("cursor", p.NextCursor)
	}
	t.Fatal("more than 100 pages")
	return nil
}

func cloneValues(q url.Values) url.Values {
	out := url.Values{}
	for k, v := range q {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func idSet(ns []apiNotification) map[string]bool {
	m := map[string]bool{}
	for _, n := range ns {
		m[n.ID] = true
	}
	return m
}

// R6: List notifications with filtering (status, channel, date range) and pagination
// (each filter narrows the list; pages do not repeat or skip rows while new rows arrive).
func TestR6ListFilters(t *testing.T) {
	s := startStack(t, stackOptions{})

	var all []apiNotification
	for i := range 4 {
		all = append(all, s.create(t, msg("sms", fmt.Sprintf("sms %d", i))))
		all = append(all, s.create(t, msg("email", fmt.Sprintf("email %d", i))))
		all = append(all, s.create(t, msg("push", fmt.Sprintf("push %d", i))))
	}
	cancelled := s.cancel(t, all[0].ID)
	future := msg("push", "scheduled one")
	future["scheduledAt"] = time.Now().UTC().Add(3 * time.Hour).Format(time.RFC3339)
	scheduled := s.create(t, future)
	b := s.createBatch(t, []map[string]any{msg("sms", "b0"), msg("email", "b1"), msg("push", "b2")})
	all = append(all, scheduled)
	for _, id := range b.NotificationIDs {
		all = append(all, s.get(t, id))
	}
	total := len(all) // 12 singles + 1 scheduled + 3 batch items

	if got := s.listAll(t, nil); len(got) != total {
		t.Fatalf("unfiltered list: want %d items, got %d", total, len(got))
	}

	t.Run("status", func(t *testing.T) {
		for _, tc := range []struct {
			status string
			want   []string
		}{
			{"cancelled", []string{cancelled.ID}},
			{"scheduled", []string{scheduled.ID}},
		} {
			got := s.listAll(t, url.Values{"status": {tc.status}})
			if len(got) != len(tc.want) || (len(got) > 0 && got[0].ID != tc.want[0]) {
				t.Errorf("status=%s: want %v, got %v", tc.status, tc.want, ids(got))
			}
		}
		pend := s.listAll(t, url.Values{"status": {"pending"}})
		if len(pend) != total-2 {
			t.Errorf("status=pending: want %d (all but the cancelled and the scheduled), got %d", total-2, len(pend))
		}
		for _, n := range pend {
			if n.Status != "pending" {
				t.Errorf("status=pending returned %s with status %s", n.ID, n.Status)
			}
		}
	})

	t.Run("channel", func(t *testing.T) {
		for _, ch := range []string{"sms", "email", "push"} {
			want := 0
			for _, n := range all {
				if n.Channel == ch {
					want++
				}
			}
			got := s.listAll(t, url.Values{"channel": {ch}})
			if len(got) != want {
				t.Errorf("channel=%s: want %d, got %d", ch, want, len(got))
			}
			for _, n := range got {
				if n.Channel != ch {
					t.Errorf("channel=%s returned %s on %s", ch, n.ID, n.Channel)
				}
			}
		}
	})

	t.Run("batchId", func(t *testing.T) {
		got := s.listAll(t, url.Values{"batchId": {b.ID}})
		gs := idSet(got)
		if len(got) != len(b.NotificationIDs) {
			t.Errorf("batchId: want %d items, got %d", len(b.NotificationIDs), len(got))
		}
		for _, id := range b.NotificationIDs {
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
		ts := boundary.UTC().Format(time.RFC3339Nano)
		from := s.listAll(t, url.Values{"createdFrom": {ts}})
		to := s.listAll(t, url.Values{"createdTo": {ts}})
		if len(from) != wantFrom {
			t.Errorf("createdFrom=%s: want %d, got %d", ts, wantFrom, len(from))
		}
		if len(to) != wantTo {
			t.Errorf("createdTo=%s: want %d, got %d", ts, wantTo, len(to))
		}
		for _, n := range from {
			if n.CreatedAt.Before(boundary) {
				t.Errorf("createdFrom returned %s created %s", n.ID, n.CreatedAt)
			}
		}
		for _, n := range to {
			if !n.CreatedAt.Before(boundary) {
				t.Errorf("createdTo returned %s created %s", n.ID, n.CreatedAt)
			}
		}
	})

	t.Run("filters combine", func(t *testing.T) {
		got := s.listAll(t, url.Values{"channel": {"push"}, "status": {"scheduled"}})
		if len(got) != 1 || got[0].ID != scheduled.ID {
			t.Errorf("channel=push&status=scheduled: want [%s], got %v", scheduled.ID, ids(got))
		}
	})
}

// R6: List notifications with filtering (status, channel, date range) and pagination
// (pages do not repeat or skip rows while new rows arrive).
func TestR6PaginationWhileRowsArrive(t *testing.T) {
	s := startStack(t, stackOptions{})

	var initial []apiNotification
	for i := range 25 {
		initial = append(initial, s.create(t, msg([]string{"sms", "email", "push"}[i%3], fmt.Sprintf("page row %d", i))))
	}

	seen := map[string]int{}
	var walked []apiNotification
	q := url.Values{"limit": {"7"}}
	pages := 0
	for {
		p := s.list(t, q)
		pages++
		if len(p.Items) > 7 {
			t.Fatalf("page %d: limit 7 but %d items", pages, len(p.Items))
		}
		for _, n := range p.Items {
			seen[n.ID]++
		}
		walked = append(walked, p.Items...)
		// New rows arrive between pages.
		for j := range 3 {
			s.create(t, msg("sms", fmt.Sprintf("arrived after page %d #%d", pages, j)))
		}
		if p.NextCursor == "" {
			break
		}
		if pages > 50 {
			t.Fatal("pagination does not end")
		}
		q.Set("cursor", p.NextCursor)
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
		if seen[n.ID] != 1 {
			t.Errorf("row %s that existed at the first page was seen %d times", n.ID, seen[n.ID])
		}
	}
	for i := 1; i < len(walked); i++ {
		if !newerFirst(walked[i-1], walked[i]) {
			t.Errorf("not newest first at %d: %s (%s) then %s (%s)", i,
				walked[i-1].ID, walked[i-1].CreatedAt.Format(time.RFC3339Nano),
				walked[i].ID, walked[i].CreatedAt.Format(time.RFC3339Nano))
			break
		}
	}
	if len(walked) > 0 && walked[0].ID != initial[len(initial)-1].ID {
		t.Errorf("first item of the first page: want newest %s, got %s", initial[len(initial)-1].ID, walked[0].ID)
	}

	t.Run("limit out of range is 400", func(t *testing.T) {
		for _, l := range []string{"0", "201"} {
			r := s.getJSON(t, "/v1/notifications?limit="+l)
			if r.Status != http.StatusBadRequest {
				t.Errorf("limit=%s: want 400, got %s", l, r)
			}
		}
	})

	t.Run("garbage cursor is 400", func(t *testing.T) {
		r := s.getJSON(t, "/v1/notifications?cursor="+url.QueryEscape(strings.Repeat("z", 40)))
		if r.Status != http.StatusBadRequest {
			t.Errorf("garbage cursor: want 400, got %s", r)
		}
	})
}
