package delivery

import (
	"math"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/config"
)

// The default attempt budget is derived, not picked (decision 0026): the
// smallest budget whose waits reach the cap, ceil(log2(cap/base)) + 1, and at
// least APP_BREAKER_FAILURES, so a lone message survives the detection of its
// outage. Changing a default base, cap or failures without the budget fails
// here.
func TestTheDefaultRetryBudgetIsTheSmallestThatReachesTheCap(t *testing.T) {
	cfg, err := config.Load(func(name string) string {
		if name == "APP_DATABASE_DSN" {
			return "user:pass@tcp(localhost:3306)/db" // required; never connected to here
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	want := int(math.Ceil(math.Log2(float64(cfg.RetryCap)/float64(cfg.RetryBase)))) + 1
	if cfg.MaxAttempts != want {
		t.Errorf("APP_MAX_ATTEMPTS default %d, want ceil(log2(%s/%s))+1 = %d", cfg.MaxAttempts, cfg.RetryCap, cfg.RetryBase, want)
	}
	if cfg.MaxAttempts < cfg.BreakerFailures {
		t.Errorf("APP_MAX_ATTEMPTS default %d below APP_BREAKER_FAILURES %d: a lone message would be dead-lettered before its outage is declared",
			cfg.MaxAttempts, cfg.BreakerFailures)
	}
	// The last wait the budget can draw is the cap: the cap is not dead.
	top := func(n int64) int64 { return n }
	if last := Backoff(cfg.MaxAttempts-1, cfg.RetryBase, cfg.RetryCap, 0, top); last != cfg.RetryCap {
		t.Errorf("largest wait after %d counted failures %s, want the cap %s", cfg.MaxAttempts-1, last, cfg.RetryCap)
	}
	worst, mean := RetrySpan(cfg.MaxAttempts, cfg.RetryBase, cfg.RetryCap)
	if worst != 810*time.Second || mean != 405*time.Second {
		t.Errorf("retry span worst %s mean %s, want 13m30s and 6m45s as stated in decision 0026", worst, mean)
	}
}
