//go:build integration

package acceptance

import (
	"net/http"
	"slices"
	"sync"
	"testing"
)

// concurrently runs f n times at once (released together) and returns every
// response; a transport error fails the test.
func concurrently(t *testing.T, n int, f func() (*response, error)) []*response {
	t.Helper()
	res := make([]*response, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Go(func() {
			<-start
			res[i], errs[i] = f()
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent request %d: %v", i, err)
		}
	}
	return res
}

// R11: Idempotency support to prevent duplicate sends
// (client half, decision 0012: the same request sent twice creates one notification).
func TestR11IdempotentCreate(t *testing.T) {
	s := startStack(t, stackOptions{})

	t.Run("same key and body twice: one row, same id, replay header", func(t *testing.T) {
		key := "r11-single-" + newUUID()
		body := msg("sms", "only once please")
		first := s.postJSON(t, "/v1/notifications", body, "Idempotency-Key", key)
		second := s.postJSON(t, "/v1/notifications", body, "Idempotency-Key", key)
		if first.Status != http.StatusCreated || second.Status != http.StatusCreated {
			t.Fatalf("want 201 twice, got %s and %s", first, second)
		}
		var a, b notification
		first.decode(t, &a)
		second.decode(t, &b)
		if a.ID == "" || a.ID != b.ID {
			t.Errorf("ids: first %q, replay %q", a.ID, b.ID)
		}
		if got := second.Header.Get("Idempotent-Replayed"); got != "true" {
			t.Errorf("replay header: want true, got %q", got)
		}
		if got := first.Header.Get("Idempotent-Replayed"); got != "" {
			t.Errorf("first response carries Idempotent-Replayed: %q", got)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM notifications WHERE content = ?", "only once please"); n != 1 {
			t.Errorf("rows: want 1, got %d", n)
		}
	})

	t.Run("20 concurrent copies: every response 201 with one id, one row", func(t *testing.T) {
		key := "r11-race-" + newUUID()
		body := msg("email", "raced body")
		const copies = 20
		res := concurrently(t, copies, func() (*response, error) {
			return s.send(http.MethodPost, "/v1/notifications", body, "Idempotency-Key", key)
		})
		idSeen := map[string]int{}
		for i, r := range res {
			if r.Status != http.StatusCreated {
				t.Errorf("copy %d: want 201, got %s", i, r)
				continue
			}
			var n notification
			r.decode(t, &n)
			idSeen[n.ID]++
		}
		if len(idSeen) != 1 {
			t.Errorf("want one id across %d copies, got %v", copies, idSeen)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM notifications WHERE content = ?", "raced body"); n != 1 {
			t.Errorf("rows: want 1, got %d", n)
		}
	})

	t.Run("same key, different body: 422 idempotency_key_reused", func(t *testing.T) {
		key := "r11-reuse-" + newUUID()
		s.create(t, msg("push", "first body"), "Idempotency-Key", key)
		r := s.postJSON(t, "/v1/notifications", msg("push", "second body"), "Idempotency-Key", key)
		wantError(t, r, http.StatusUnprocessableEntity, "idempotency_key_reused")
		if n := s.count(t, "SELECT COUNT(*) FROM notifications WHERE content = ?", "second body"); n != 0 {
			t.Errorf("the refused body was stored: %d rows", n)
		}
	})

	t.Run("no key: two identical requests are two notifications", func(t *testing.T) {
		body := msg("sms", "twice on purpose")
		a := s.create(t, body)
		b := s.create(t, body)
		if a.ID == b.ID {
			t.Errorf("without a key both requests returned id %s", a.ID)
		}
	})
}

// R11: Idempotency support to prevent duplicate sends
// (client half, decision 0012, for batch creation).
func TestR11IdempotentBatch(t *testing.T) {
	s := startStack(t, stackOptions{})
	items := batchItems(4)

	t.Run("same key and body twice: one batch, same ids, replay header", func(t *testing.T) {
		key := "r11-batch-" + newUUID()
		first := s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": items}, "Idempotency-Key", key)
		second := s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": items}, "Idempotency-Key", key)
		if first.Status != http.StatusCreated || second.Status != http.StatusCreated {
			t.Fatalf("want 201 twice, got %s and %s", first, second)
		}
		var a, b batchCreated
		first.decode(t, &a)
		second.decode(t, &b)
		if a.ID == "" || a.ID != b.ID || !slices.Equal(a.NotificationIDs, b.NotificationIDs) {
			t.Errorf("replay differs: first %s %v, replay %s %v", a.ID, a.NotificationIDs, b.ID, b.NotificationIDs)
		}
		if got := second.Header.Get("Idempotent-Replayed"); got != "true" {
			t.Errorf("replay header: want true, got %q", got)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM batches"); n != 1 {
			t.Errorf("batch rows: want 1, got %d", n)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM notifications"); n != len(items) {
			t.Errorf("rows: want %d, got %d", len(items), n)
		}
	})

	t.Run("20 concurrent copies: every response 201 with one batch id, one set of rows", func(t *testing.T) {
		key := "r11-batch-race-" + newUUID()
		raceItems := batchItems(3)
		for _, it := range raceItems {
			it["content"] = "race " + it["content"].(string)
		}
		beforeBatches := s.count(t, "SELECT COUNT(*) FROM batches")
		const copies = 20
		res := concurrently(t, copies, func() (*response, error) {
			return s.send(http.MethodPost, "/v1/notifications/batch", map[string]any{"notifications": raceItems}, "Idempotency-Key", key)
		})
		batchIDs := map[string]int{}
		for i, r := range res {
			if r.Status != http.StatusCreated {
				t.Errorf("copy %d: want 201, got %s", i, r)
				continue
			}
			var b batchCreated
			r.decode(t, &b)
			batchIDs[b.ID]++
		}
		if len(batchIDs) != 1 {
			t.Errorf("want one batch id across %d copies, got %v", copies, batchIDs)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM batches"); n != beforeBatches+1 {
			t.Errorf("batch rows: want %d, got %d", beforeBatches+1, n)
		}
		if n := s.count(t, "SELECT COUNT(*) FROM notifications WHERE content LIKE 'race %'"); n != len(raceItems) {
			t.Errorf("rows: want %d, got %d", len(raceItems), n)
		}
	})

	t.Run("same key, different body: 422 idempotency_key_reused", func(t *testing.T) {
		key := "r11-batch-reuse-" + newUUID()
		s.createBatch(t, batchItems(2), "Idempotency-Key", key)
		r := s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": batchItems(3)}, "Idempotency-Key", key)
		wantError(t, r, http.StatusUnprocessableEntity, "idempotency_key_reused")
	})
}
