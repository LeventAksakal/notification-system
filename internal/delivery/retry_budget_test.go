package delivery_test

import (
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/delivery"
)

// The default budget's own test is TestTheDefaultRetryBudgetIsTheSmallestThatReachesTheCap
// (retry_numbers_test.go). The ten-minute outage target it replaced is gone:
// an outage no longer spends budget (decision 0026).

// The span of a small budget is what the waits add up to: attempts 1 to 4 of
// five wait at most 2, 4, 8 and 16 base units.
func TestRetrySpanSumsTheWaitsBetweenAttempts(t *testing.T) {
	worst, mean := delivery.RetrySpan(5, time.Second, 5*time.Minute)
	if worst != 30*time.Second || mean != 15*time.Second {
		t.Fatalf("RetrySpan(5, 1s, 5m) = %s, %s; want 30s, 15s", worst, mean)
	}
}
