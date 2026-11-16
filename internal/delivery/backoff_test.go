package delivery

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	top := func(n int64) int64 { return n }
	cases := []struct {
		attempt    int
		retryAfter time.Duration
		want       time.Duration
	}{
		{1, 0, 200 * time.Millisecond},
		{2, 0, 400 * time.Millisecond},
		{4, 0, time.Second}, // capped
		{70, 0, time.Second},
		{1, 2 * time.Second, 2 * time.Second}, // never shorter than Retry-After
	}
	for _, c := range cases {
		if got := Backoff(c.attempt, 100*time.Millisecond, time.Second, c.retryAfter, top); got != c.want {
			t.Errorf("Backoff(%d, retryAfter %s) upper = %s, want %s", c.attempt, c.retryAfter, got, c.want)
		}
	}
	if got := Backoff(3, 100*time.Millisecond, time.Second, 0, func(int64) int64 { return 0 }); got != 0 {
		t.Errorf("full jitter lower bound: got %s", got)
	}
}
