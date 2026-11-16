//go:build integration

package intake_test

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/realtime"
	"github.com/LeventAksakal/notification-system/internal/realtime/realtimetest"
	"github.com/LeventAksakal/notification-system/internal/wake"
	"github.com/LeventAksakal/notification-system/test/support"
)

// With Valkey gone, a request waits for at most one bounded publish whatever
// its size; its rows are committed before that (CONTRACT "Events").
func TestCreateAnswersPromptlyWithValkeyDown(t *testing.T) {
	db, _ := support.MySQL(t)
	proxy := realtimetest.NewProxy(t, support.Valkey(t))
	vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{proxy.Addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(vk.Close)
	svc := intake.New(db, realtime.NewPublisher(vk), time.Hour).WithWaker(wake.NewPublisher(vk))
	proxy.Close()

	batch := func() []intake.Request {
		rs := make([]intake.Request, notification.MaxBatchSize)
		for i := range rs {
			c := "outage " + strconv.Itoa(i)
			rs[i] = intake.Request{To: "+905551234567", Channel: notification.ChannelSMS, Content: &c}
		}
		return rs
	}
	count := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(context.Background(), q, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	for _, tc := range []struct {
		name  string
		after time.Duration // idle time before the request
	}{
		{"first batch: the open connection breaks", 0},
		{"second batch: only one probe tries while Valkey is down", 0},
		{"third batch, a second later: still one probe, the client redials", time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			time.Sleep(tc.after)
			start := time.Now()
			b, _, err := svc.CreateBatch(t.Context(), batch(), intake.Meta{CorrelationID: "c"})
			took := time.Since(start)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%d-item batch with Valkey down: %s", notification.MaxBatchSize, took)
			if took > 3*time.Second {
				t.Errorf("batch took %s with Valkey down, want < 3s", took)
			}
			if n := count(`SELECT COUNT(*) FROM notifications WHERE batch_id = ?`, b.ID); n != notification.MaxBatchSize {
				t.Errorf("rows of the batch: want %d, got %d", notification.MaxBatchSize, n)
			}
		})
	}
	t.Run("single create", func(t *testing.T) {
		c := "outage single"
		start := time.Now()
		n, _, err := svc.Create(t.Context(), intake.Request{To: "+905551234567", Channel: notification.ChannelSMS, Content: &c}, intake.Meta{CorrelationID: "c"})
		if err != nil {
			t.Fatal(err)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Errorf("create took %s with Valkey down, want < 2s", took)
		}
		if got := count(`SELECT COUNT(*) FROM notifications WHERE id = ?`, n.ID); got != 1 {
			t.Errorf("row of %s: want 1, got %d", n.ID, got)
		}
	})
}

type nopPublisher struct{}

func (nopPublisher) Publish(context.Context, notification.Event) error { return nil }

// committedWaker records hints, and for each one whether the notifications it
// announces were already committed: a worker it wakes must find them.
type committedWaker struct {
	db interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}
	hints []notification.Wake
	seen  []int // pending rows of the hint's channel and priority visible when it was sent
}

func (w *committedWaker) Wake(ctx context.Context, hints ...notification.Wake) error {
	for _, h := range hints {
		var n int
		if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM notifications WHERE status = 'pending' AND channel = ? AND priority = ?`,
			h.Channel, h.Priority).Scan(&n); err != nil {
			return err
		}
		w.hints = append(w.hints, h)
		w.seen = append(w.seen, n)
	}
	return nil
}

func TestIntakeWakesWorkersAfterEveryCommitThatMakesWorkPending(t *testing.T) {
	db, _ := support.MySQL(t)
	w := &committedWaker{db: db}
	svc := intake.New(db, nopPublisher{}, time.Hour).WithWaker(w)
	content := func(s string) *string { return &s }
	meta := intake.Meta{CorrelationID: "c"}

	t.Run("a create is hinted once, after its commit, with its due time", func(t *testing.T) {
		w.hints, w.seen = nil, nil
		at := time.Now().Add(time.Hour).Truncate(time.Millisecond)
		n, _, err := svc.Create(t.Context(), intake.Request{To: "+905551234567", Channel: notification.ChannelSMS,
			Priority: notification.PriorityHigh, Content: content("x"), ScheduledAt: &at}, meta)
		if err != nil {
			t.Fatal(err)
		}
		if len(w.hints) != 1 || w.seen[0] < 1 {
			t.Fatalf("want one hint sent after the row was visible, got %+v (visible %v)", w.hints, w.seen)
		}
		h := w.hints[0]
		if h.Channel != notification.ChannelSMS || h.Priority != notification.PriorityHigh || !h.Due.Equal(n.DueAt) {
			t.Fatalf("hint %+v, want sms/high due %s", h, n.DueAt)
		}
	})

	t.Run("an idempotent replay is not hinted again", func(t *testing.T) {
		m := intake.Meta{CorrelationID: "c", IdempotencyKey: "wake-replay-key"}
		r := intake.Request{To: "+905551234567", Channel: notification.ChannelSMS, Content: content("y")}
		if _, _, err := svc.Create(t.Context(), r, m); err != nil {
			t.Fatal(err)
		}
		w.hints, w.seen = nil, nil
		if _, replayed, err := svc.Create(t.Context(), r, m); err != nil || !replayed {
			t.Fatalf("second create: replayed=%v err=%v", replayed, err)
		}
		if len(w.hints) != 0 {
			t.Fatalf("a replayed response changed nothing: want no hint, got %+v", w.hints)
		}
	})

	t.Run("a batch sends one hint per channel and priority, after its commit", func(t *testing.T) {
		w.hints, w.seen = nil, nil
		rs := []intake.Request{
			{To: "a@example.com", Channel: notification.ChannelEmail, Content: content("1")},
			{To: "b@example.com", Channel: notification.ChannelEmail, Content: content("2")},
			{To: "device-1", Channel: notification.ChannelPush, Priority: notification.PriorityLow, Content: content("3")},
		}
		if _, _, err := svc.CreateBatch(t.Context(), rs, meta); err != nil {
			t.Fatal(err)
		}
		got := map[string]int{}
		for i, h := range w.hints {
			got[string(h.Channel)+"/"+string(h.Priority)]++
			if w.seen[i] < 1 {
				t.Errorf("hint %+v sent before its rows were visible", h)
			}
		}
		// The recording waker sees the batch's hints before coalescing; the
		// Valkey waker sends one per key. Every key must be covered.
		if got["email/normal"] == 0 || got["push/low"] == 0 || len(got) != 2 {
			t.Fatalf("want hints for email/normal and push/low only, got %v", got)
		}
	})

	t.Run("a replay of a failed notification is hinted, due now", func(t *testing.T) {
		n, _, err := svc.Create(t.Context(), intake.Request{To: "device-2", Channel: notification.ChannelPush, Content: content("z")}, meta)
		if err != nil {
			t.Fatal(err)
		}
		// Through the allowed changes, with the call a failure needs: the
		// database refuses anything else (decision 0005).
		for _, stmt := range []string{
			`UPDATE notifications SET status = 'processing', lease_until = NOW(3) + INTERVAL 1 MINUTE WHERE id = ?`,
			`UPDATE notifications SET status = 'failed', failure_kind = 'permanent', finished_at = NOW(3), lease_until = NULL,
				attempts = 1, budget_spent = 1 WHERE id = ?`,
		} {
			if _, err := db.ExecContext(t.Context(), stmt, n.ID); err != nil {
				t.Fatal(err)
			}
		}
		w.hints, w.seen = nil, nil
		before := time.Now()
		if _, err := svc.Retry(t.Context(), n.ID); err != nil {
			t.Fatal(err)
		}
		if len(w.hints) != 1 || w.seen[0] < 1 || w.hints[0].Channel != notification.ChannelPush || w.hints[0].Due.After(time.Now().Add(time.Second)) {
			t.Fatalf("want one hint for push due about now (%s), sent after the commit; got %+v (visible %v)", before, w.hints, w.seen)
		}
	})
}
