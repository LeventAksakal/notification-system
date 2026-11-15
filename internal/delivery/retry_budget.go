package delivery

import "time"

// RetrySpan is how long a notification keeps being retried when every
// attempt fails: the sum of the backoff waits between its attempts, at worst
// (every wait at its bound) and on average (full jitter halves each bound).
// Retry-After and the breaker can only lengthen it. Owner, 15 Nov 2026: every
// failure gets the action its meaning calls for, so the budget is stated as a
// time span, not only as a count.
func RetrySpan(maxAttempts int, base, ceiling time.Duration) (worst, mean time.Duration) {
	for attempt := 1; attempt < maxAttempts; attempt++ {
		b := Backoff(attempt, base, ceiling, 0, func(n int64) int64 { return n }) // the largest wait it can draw
		worst += b
		mean += b / 2
	}
	return worst, mean
}
