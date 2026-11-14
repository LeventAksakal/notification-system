//go:build integration

package intake_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/core"
	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/realtime"
	"github.com/LeventAksakal/notification-system/internal/realtime/realtimetest"
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
	svc := intake.New(db, realtime.NewPublisher(vk, realtime.OriginAPI), time.Hour)
	proxy.Close()

	batch := func() []intake.Request {
		rs := make([]intake.Request, core.MaxBatchSize)
		for i := range rs {
			c := "outage " + strconv.Itoa(i)
			rs[i] = intake.Request{To: "+905551234567", Channel: core.ChannelSMS, Content: &c}
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
		{"second batch: publishes held off", 0},
		{"third batch: the hold-off is over and the client redials", time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			time.Sleep(tc.after)
			start := time.Now()
			b, _, err := svc.CreateBatch(t.Context(), batch(), intake.Meta{CorrelationID: "c"})
			took := time.Since(start)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%d-item batch with Valkey down: %s", core.MaxBatchSize, took)
			if took > 3*time.Second {
				t.Errorf("batch took %s with Valkey down, want < 3s", took)
			}
			if n := count(`SELECT COUNT(*) FROM notifications WHERE batch_id = ?`, b.ID); n != core.MaxBatchSize {
				t.Errorf("rows of the batch: want %d, got %d", core.MaxBatchSize, n)
			}
		})
	}
	t.Run("single create", func(t *testing.T) {
		c := "outage single"
		start := time.Now()
		n, _, err := svc.Create(t.Context(), intake.Request{To: "+905551234567", Channel: core.ChannelSMS, Content: &c}, intake.Meta{CorrelationID: "c"})
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
