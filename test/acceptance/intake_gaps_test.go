//go:build integration

package acceptance

// Gap tests named by the intake verifier (.agents/reviews/2026-11-13-intake-verifier.md,
// "Test gaps") and the 1.1.0 contract additions (ASCII Idempotency-Key, 8 MiB body limit).
// Every name here starts with "gap" / "TestGap" so it cannot collide with other files.
//
// Not covered, on purpose: R2 batch atomicity when the database refuses a row
// mid-transaction. It needs fault injection inside the store; black-box, the
// tests cannot make MySQL refuse the second chunk of a valid batch.

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/internal/notification"
)

// gapWalk walks a filtered list page by page at the given limit. It fails on
// a page larger than limit or a row seen twice, and returns the rows in the
// order the pages gave them.
func (s *stack) gapWalk(t *testing.T, q url.Values, limit int) []apiNotification {
	t.Helper()
	q = cloneValues(q)
	q.Set("limit", strconv.Itoa(limit))
	seen := map[string]bool{}
	var out []apiNotification
	for page := 1; ; page++ {
		if page > 200 {
			t.Fatalf("%s: pagination does not end", q.Encode())
		}
		p := s.list(t, q)
		if len(p.Items) > limit {
			t.Fatalf("%s page %d: limit %d but %d items", q.Encode(), page, limit, len(p.Items))
		}
		for _, n := range p.Items {
			if seen[n.ID] {
				t.Errorf("%s: row %s appears on more than one page (again on page %d)", q.Encode(), n.ID, page)
			}
			seen[n.ID] = true
		}
		out = append(out, p.Items...)
		if p.NextCursor == "" {
			return out
		}
		q.Set("cursor", p.NextCursor)
	}
}

// gapNewestFirst returns the ids of ns sorted newest first (createdAt desc, id desc).
func gapNewestFirst(ns []apiNotification) []string {
	c := slices.Clone(ns)
	sort.SliceStable(c, func(i, j int) bool { return newerFirst(c[i], c[j]) })
	return ids(c)
}

func gapFilter(ns []apiNotification, keep func(apiNotification) bool) []apiNotification {
	var out []apiNotification
	for _, n := range ns {
		if keep(n) {
			out = append(out, n)
		}
	}
	return out
}

// R6: List notifications with filtering (status, channel, date range) and pagination
// ("Each filter narrows the list; pages do not repeat or skip rows" — a filter
// combined with a cursor, walked page by page at a small limit).
func TestGapR6FilterWithCursor(t *testing.T) {
	s := startStack(t, stackOptions{})

	// 27 rows: channels rotate sms/email/push; i%5==2 is scheduled 3 h ahead;
	// i%4==3 (when not scheduled) is cancelled. Expected status per row.
	chans := []string{"sms", "email", "push"}
	var all []apiNotification
	want := map[string]string{} // id -> expected API status
	for i := range 27 {
		body := msg(chans[i%3], fmt.Sprintf("gap r6 row %d", i))
		status := "pending"
		if i%5 == 2 {
			body["scheduledAt"] = time.Now().UTC().Add(3 * time.Hour).Format(time.RFC3339)
			status = "scheduled"
		}
		n := s.create(t, body)
		if i%4 == 3 && status == "pending" {
			s.cancel(t, n.ID)
			status = "cancelled"
		}
		all = append(all, n)
		want[n.ID] = status
	}

	check := func(t *testing.T, q url.Values, keep func(apiNotification) bool) {
		t.Helper()
		exp := gapNewestFirst(gapFilter(all, keep))
		if len(exp) <= 4 {
			t.Fatalf("test setup: %s selects %d rows, need more than one page of 4", q.Encode(), len(exp))
		}
		got := ids(s.gapWalk(t, q, 4))
		if !slices.Equal(got, exp) {
			t.Errorf("%s at limit 4: want exactly %d rows newest first\n want %v\n  got %v", q.Encode(), len(exp), exp, got)
		}
	}

	t.Run("channel=email", func(t *testing.T) {
		check(t, url.Values{"channel": {"email"}}, func(n apiNotification) bool { return n.Channel == "email" })
	})

	for _, st := range []string{"pending", "scheduled", "cancelled"} {
		t.Run("status="+st, func(t *testing.T) {
			check(t, url.Values{"status": {st}}, func(n apiNotification) bool { return want[n.ID] == st })
		})
	}

	t.Run("channel and status together", func(t *testing.T) {
		// sms rows that are pending: enough for two pages.
		check(t, url.Values{"channel": {"sms"}, "status": {"pending"}}, func(n apiNotification) bool {
			return n.Channel == "sms" && want[n.ID] == "pending"
		})
	})

	t.Run("createdFrom and createdTo with a cursor", func(t *testing.T) {
		from, to := all[4].CreatedAt, all[22].CreatedAt
		if !from.Before(to) {
			t.Fatalf("test setup: createdAt of rows 4 and 22 are not ordered: %s, %s", from, to)
		}
		in := func(n apiNotification) bool { return !n.CreatedAt.Before(from) && n.CreatedAt.Before(to) }
		q := url.Values{
			"createdFrom": {from.UTC().Format(time.RFC3339Nano)},
			"createdTo":   {to.UTC().Format(time.RFC3339Nano)},
		}
		check(t, q, in)

		qe := cloneValues(q)
		qe.Set("channel", "email")
		// email rows in the window may be few; walk at limit 2 so it still spans pages.
		exp := gapNewestFirst(gapFilter(all, func(n apiNotification) bool { return in(n) && n.Channel == "email" }))
		if got := ids(s.gapWalk(t, qe, 2)); !slices.Equal(got, exp) {
			t.Errorf("%s at limit 2:\n want %v\n  got %v", qe.Encode(), exp, got)
		}
	})
}

// R6: List notifications with filtering (status, channel, date range) and pagination
// (status=sent and status=failed after real delivery: each lists exactly its rows).
func TestGapR6SentAndFailedFilters(t *testing.T) {
	const total, k = 12, 4
	s := startStack(t, stackOptions{Workers: 1, Env: dlFastEnv(nil)})
	// The next k provider calls answer 400: permanent, failed after one call.
	s.dlFaults(t, fakeprovider.Fault{Mode: fakeprovider.ModeStatus, Status: http.StatusBadRequest, Count: k})

	created := map[string]bool{}
	for i := range total {
		created[s.create(t, msg([]string{"sms", "email", "push"}[i%3], fmt.Sprintf("gap r6 delivery %d", i))).ID] = true
	}
	dlEventually(t, 60*time.Second, "every notification ends sent or failed", func() (bool, string) {
		c := s.dlCounts(t, "1=1")
		return c["sent"]+c["failed"] == total, fmt.Sprintf("%v", c)
	})

	// Ground truth from the provider double, not from the API under test: the
	// notification id is the Idempotency-Key of each call.
	wantSent, wantFailed := map[string]bool{}, map[string]bool{}
	for _, r := range s.Provider.Requests(fakeprovider.Filter{}) {
		switch {
		case r.Status == http.StatusBadRequest:
			wantFailed[r.IdempotencyKey] = true
		case r.Delivery:
			wantSent[r.IdempotencyKey] = true
		}
	}
	if len(wantFailed) != k || len(wantSent) != total-k {
		t.Fatalf("test setup: the double saw %d refused and %d delivered, want %d and %d", len(wantFailed), len(wantSent), k, total-k)
	}

	for _, tc := range []struct {
		status string
		want   map[string]bool
	}{{"sent", wantSent}, {"failed", wantFailed}} {
		t.Run("status="+tc.status, func(t *testing.T) {
			got := s.gapWalk(t, url.Values{"status": {tc.status}}, 3)
			gotSet := idSet(got)
			for id := range tc.want {
				if !gotSet[id] {
					t.Errorf("status=%s misses %s", tc.status, id)
				}
			}
			for _, n := range got {
				if !tc.want[n.ID] {
					t.Errorf("status=%s lists %s (status %q), which the provider did not %s", tc.status, n.ID, n.Status, tc.status)
				}
				if n.Status != tc.status {
					t.Errorf("status=%s lists %s with status %q", tc.status, n.ID, n.Status)
				}
				if !created[n.ID] {
					t.Errorf("status=%s lists unknown id %s", tc.status, n.ID)
				}
			}
		})
	}
}

// R5: Cancel pending notifications
// ("Cancel after sending: refused" — 409 not_cancellable, and it stays sent).
func TestGapR5CancelSent(t *testing.T) {
	s := startStack(t, stackOptions{Workers: 1, Env: dlFastEnv(nil)})
	n := s.create(t, msg("sms", "gap r5 already sent"))
	dlEventually(t, 30*time.Second, "the notification is sent", func() (bool, string) {
		st := s.storedStatus(t, n.ID)
		return st == "sent", "stored status " + st
	})

	wantError(t, s.postJSON(t, "/v1/notifications/"+n.ID+"/cancel", nil), http.StatusConflict, "not_cancellable")
	if got := s.get(t, n.ID); got.Status != "sent" {
		t.Errorf("after a refused cancel: want status sent, got %q", got.Status)
	}
	if st := s.storedStatus(t, n.ID); st != "sent" {
		t.Errorf("after a refused cancel: stored status %q", st)
	}
}

// R11: Idempotency support to prevent duplicate sends
// ("The same request sent twice creates one notification" — also when the
// template changed in between, across single/batch, and with malformed keys).
func TestGapR11Idempotency(t *testing.T) {
	s := startStack(t, stackOptions{})

	t.Run("replay after the template was deleted, then replaced", func(t *testing.T) {
		const want = "Hello Ada, gap r11"
		s.putTemplate(t, "gap-r11", "Hello {{name}}, gap r11")
		key := "gap-r11-tpl-" + newUUID()
		body := withTemplate("email", "gap-r11", map[string]any{"name": "Ada"})

		first := s.postJSON(t, "/v1/notifications", body, "Idempotency-Key", key)
		if first.Status != http.StatusCreated {
			t.Fatalf("first create: want 201, got %s", first)
		}
		var orig apiNotification
		first.decode(t, &orig)

		if r := s.do(t, http.MethodDelete, "/v1/templates/gap-r11", nil); r.Status != http.StatusNoContent {
			t.Fatalf("DELETE template: want 204, got %s", r)
		}
		replay := func(t *testing.T, when string) {
			t.Helper()
			r := s.postJSON(t, "/v1/notifications", body, "Idempotency-Key", key)
			if r.Status != http.StatusCreated {
				t.Fatalf("replay %s: want 201 with the first response, got %s", when, r)
			}
			var got apiNotification
			r.decode(t, &got)
			if got.ID != orig.ID {
				t.Errorf("replay %s: id %q, first was %q", when, got.ID, orig.ID)
			}
			if h := r.Header.Get("Idempotent-Replayed"); h != "true" {
				t.Errorf("replay %s: Idempotent-Replayed want true, got %q", when, h)
			}
			if got.Content != want {
				t.Errorf("replay %s: content %q, want %q", when, got.Content, want)
			}
		}
		replay(t, "after the template was deleted")

		s.putTemplate(t, "gap-r11", "Goodbye {{name}}, changed")
		replay(t, "after the template was replaced")

		if got := s.get(t, orig.ID); got.Content != want {
			t.Errorf("stored content changed to %q", got.Content)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM notifications WHERE content LIKE '%gap r11%' OR content LIKE '%changed%'"); n != 1 {
			t.Errorf("rows: want 1, got %d", n)
		}
	})

	t.Run("same key for a single create, then a batch: 422, no batch stored", func(t *testing.T) {
		key := "gap-r11-cross-" + newUUID()
		s.create(t, msg("sms", "gap r11 single first"), "Idempotency-Key", key)
		batchesBefore := s.count(t, "SELECT COUNT(*) FROM batches")
		items := []map[string]any{msg("sms", "gap r11 cross batch 0"), msg("email", "gap r11 cross batch 1")}
		r := s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": items}, "Idempotency-Key", key)
		wantError(t, r, http.StatusUnprocessableEntity, "idempotency_key_reused")
		if n := s.count(t, "SELECT COUNT(*) FROM batches"); n != batchesBefore {
			t.Errorf("batches: want %d, got %d", batchesBefore, n)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM notifications WHERE content LIKE 'gap r11 cross batch %'"); n != 0 {
			t.Errorf("rows of the refused batch: want 0, got %d", n)
		}
	})

	t.Run("malformed key: 400 invalid_request naming Idempotency-Key", func(t *testing.T) {
		for _, tc := range []struct{ name, key string }{
			{"non-ASCII", "clé"},
			{"space", "gap key"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, path := range []string{"/v1/notifications", "/v1/notifications/batch"} {
					var body any = msg("push", "gap r11 bad key")
					if strings.HasSuffix(path, "/batch") {
						body = map[string]any{"notifications": []map[string]any{msg("push", "gap r11 bad key")}}
					}
					r, err := s.send(http.MethodPost, path, body, "Idempotency-Key", tc.key)
					if err != nil {
						t.Fatalf("POST %s with key %q: the client could not send it: %v", path, tc.key, err)
					}
					if r.Status >= 500 {
						t.Errorf("POST %s with key %q: a server error: %s", path, tc.key, r)
						continue
					}
					wantError(t, r, http.StatusBadRequest, "invalid_request", "Idempotency-Key")
				}
				if n := s.count(t, "SELECT COUNT(*) FROM notifications WHERE content = 'gap r11 bad key'"); n != 0 {
					t.Errorf("refused requests stored %d rows", n)
				}
			})
		}
	})
}

// gapEmailBody returns a single-create JSON body of exactly size bytes.
func gapEmailBody(t *testing.T, size int) string {
	t.Helper()
	head := `{"to":"` + emailTo + `","channel":"email","content":"`
	tail := `"}`
	n := size - len(head) - len(tail)
	if n < 0 {
		t.Fatalf("size %d too small", size)
	}
	return head + strings.Repeat("a", n) + tail
}

// R10: Content validation (character limits, required fields)
// (a request body is at most notification.MaxRequestBodyBytes, 8 MiB: a larger one is
// 413 request_too_large in the Error shape; a large batch under it is accepted).
func TestGapR10RequestBodyLimit(t *testing.T) {
	s := startStack(t, stackOptions{})

	t.Run("one byte over the limit: 413 request_too_large", func(t *testing.T) {
		single := gapEmailBody(t, notification.MaxRequestBodyBytes+1)
		wantError(t, s.postJSON(t, "/v1/notifications", single), http.StatusRequestEntityTooLarge, "request_too_large")

		// A batch envelope of the same size.
		inner := gapEmailBody(t, notification.MaxRequestBodyBytes+1-len(`{"notifications":[]}`))
		batch := `{"notifications":[` + inner + `]}`
		if len(batch) != notification.MaxRequestBodyBytes+1 {
			t.Fatalf("test setup: batch body %d bytes", len(batch))
		}
		wantError(t, s.postJSON(t, "/v1/notifications/batch", batch), http.StatusRequestEntityTooLarge, "request_too_large")

		if n := s.count(t, "SELECT COUNT(*) FROM notifications"); n != 0 {
			t.Errorf("oversized requests stored %d rows", n)
		}
	})

	t.Run("a 5 MB batch under the limit: 201", func(t *testing.T) {
		items := make([]map[string]any, 100)
		for i := range items {
			items[i] = msg("email", fmt.Sprintf("%03d", i)+strings.Repeat("b", 50_000-3))
		}
		b := s.createBatch(t, items)
		if len(b.NotificationIDs) != len(items) {
			t.Errorf("batch: want %d ids, got %d", len(items), len(b.NotificationIDs))
		}
	})
}
