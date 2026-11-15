package delivery_test

import (
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/config"
	"github.com/LeventAksakal/notification-system/internal/delivery"
)

// With the default dials a notification keeps retrying through a provider
// outage of ten minutes on average before it becomes a dead letter, and the
// cap (the longest wait) is reached, so no dial is dead.
func TestTheDefaultRetryBudgetOutlastsATenMinuteOutage(t *testing.T) {
	cfg, err := config.Load(func(k string) string {
		if k == "APP_DATABASE_DSN" {
			return "u:p@tcp(db)/app"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	worst, mean := delivery.RetrySpan(cfg.MaxAttempts, cfg.RetryBase, cfg.RetryCap)
	if mean < 10*time.Minute {
		t.Errorf("average retry span %s, want at least 10m (worst %s)", mean, worst)
	}
	last := delivery.Backoff(cfg.MaxAttempts-1, cfg.RetryBase, cfg.RetryCap, 0, func(n int64) int64 { return n })
	if last < cfg.RetryCap {
		t.Errorf("the last wait can be at most %s, so APP_RETRY_CAP %s is never reached", last, cfg.RetryCap)
	}
}

// The span of a small budget is what the waits add up to: attempts 1 to 4 of
// five wait at most 2, 4, 8 and 16 base units.
func TestRetrySpanSumsTheWaitsBetweenAttempts(t *testing.T) {
	worst, mean := delivery.RetrySpan(5, time.Second, 5*time.Minute)
	if worst != 30*time.Second || mean != 15*time.Second {
		t.Fatalf("RetrySpan(5, 1s, 5m) = %s, %s; want 30s, 15s", worst, mean)
	}
}
