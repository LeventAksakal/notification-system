// Package limiter is the send-rate limit shared by every worker (decision
// 0009): GCRA with burst 1 in Valkey, one key per channel, on Valkey's clock.
// It hands out send slots: moments, exactly one interval apart, at which a
// request may leave.
package limiter

import (
	"math"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// Limiter is the Valkey notification.Limiter.
type Limiter struct {
	client   valkey.Client
	interval time.Duration
	prefix   string
}

var _ notification.Limiter = (*Limiter)(nil)

// New paces each channel at perSecond reduced by headroomPct percent; the
// keys are "<prefix><channel>".
//
// The interval is rounded up to the microsecond, so the slots in any sliding
// second never exceed the paced rate: 95 per second (100 with the default 5%
// headroom) gives 10 527 us, and ceil(1s / 10 527 us) = 95.
func New(client valkey.Client, perSecond, headroomPct int, prefix string) *Limiter {
	return &Limiter{client: client, interval: SlotInterval(perSecond, headroomPct), prefix: prefix}
}

// SlotInterval is the time between two slots on one channel paced at
// perSecond reduced by headroomPct percent, rounded up to the microsecond.
func SlotInterval(perSecond, headroomPct int) time.Duration {
	pace := float64(perSecond) * float64(100-headroomPct) / 100
	if pace <= 0 {
		pace = 1
	}
	return time.Duration(math.Ceil(1e6/pace)) * time.Microsecond
}

// Interval is the time between two slots on one channel.
func (l *Limiter) Interval() time.Duration { return l.interval }
