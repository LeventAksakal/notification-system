//go:build integration

package acceptance

import (
	"net/http"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// concurrently runs f n times at once (released together) and returns every
// response; a transport error fails the test.
func concurrently[R any](t *testing.T, n int, f func() (R, error)) []R {
	t.Helper()
	res := make([]R, n)
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
		first := s.createResp(t, body, idemKey(key))
		second := s.createResp(t, body, idemKey(key))
		if first.StatusCode() != http.StatusCreated || second.StatusCode() != http.StatusCreated || first.JSON201 == nil || second.JSON201 == nil {
			t.Fatalf("want 201 twice, got %s and %s", show(first), show(second))
		}
		a, b := *first.JSON201, *second.JSON201
		if a.Id == uuid.Nil || a.Id != b.Id {
			t.Errorf("ids: first %q, replay %q", a.Id, b.Id)
		}
		if got := second.HTTPResponse.Header.Get("Idempotent-Replayed"); got != "true" {
			t.Errorf("replay header: want true, got %q", got)
		}
		if got := first.HTTPResponse.Header.Get("Idempotent-Replayed"); got != "" {
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
		res := concurrently(t, copies, func() (*apiclient.CreateNotificationResponse, error) {
			return s.API.CreateNotificationWithResponse(t.Context(), &apiclient.CreateNotificationParams{IdempotencyKey: &key}, body)
		})
		idSeen := map[uuid.UUID]int{}
		for i, r := range res {
			if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
				t.Errorf("copy %d: want 201, got %s", i, show(r))
				continue
			}
			idSeen[r.JSON201.Id]++
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
		s.create(t, msg("push", "first body"), idemKey(key))
		r := s.postCreate(t, msg("push", "second body"), idemKey(key))
		wantError(t, r, http.StatusUnprocessableEntity, "idempotency_key_reused")
		if n := s.count(t, "SELECT COUNT(*) FROM notifications WHERE content = ?", "second body"); n != 0 {
			t.Errorf("the refused body was stored: %d rows", n)
		}
	})

	t.Run("no key: two identical requests are two notifications", func(t *testing.T) {
		body := msg("sms", "twice on purpose")
		a := s.create(t, body)
		b := s.create(t, body)
		if a.Id == b.Id {
			t.Errorf("without a key both requests returned id %s", a.Id)
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
		first := s.createBatchResp(t, items, idemKey(key))
		second := s.createBatchResp(t, items, idemKey(key))
		if first.StatusCode() != http.StatusCreated || second.StatusCode() != http.StatusCreated || first.JSON201 == nil || second.JSON201 == nil {
			t.Fatalf("want 201 twice, got %s and %s", show(first), show(second))
		}
		a, b := *first.JSON201, *second.JSON201
		if a.Id == uuid.Nil || a.Id != b.Id || !slices.Equal(a.NotificationIds, b.NotificationIds) {
			t.Errorf("replay differs: first %s %v, replay %s %v", a.Id, a.NotificationIds, b.Id, b.NotificationIds)
		}
		if got := second.HTTPResponse.Header.Get("Idempotent-Replayed"); got != "true" {
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
		for i := range raceItems {
			raceItems[i].Content = ptr("race " + deref(raceItems[i].Content))
		}
		beforeBatches := s.count(t, "SELECT COUNT(*) FROM batches")
		const copies = 20
		res := concurrently(t, copies, func() (*apiclient.CreateBatchResponse, error) {
			return s.API.CreateBatchWithResponse(t.Context(), &apiclient.CreateBatchParams{IdempotencyKey: &key}, apiclient.BatchCreate{Notifications: raceItems})
		})
		batchIDs := map[uuid.UUID]int{}
		for i, r := range res {
			if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
				t.Errorf("copy %d: want 201, got %s", i, show(r))
				continue
			}
			batchIDs[r.JSON201.Id]++
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
		s.createBatch(t, batchItems(2), idemKey(key))
		r := s.postBatch(t, batchItems(3), idemKey(key))
		wantError(t, r, http.StatusUnprocessableEntity, "idempotency_key_reused")
	})
}
