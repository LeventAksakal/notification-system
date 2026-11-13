// Package limiter is the send-rate limit shared by every worker (decision
// 0009): GCRA with burst 1 in Valkey, one key per channel, on Valkey's clock.
package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/core"
)

// gcra grants at most one permit per call. State is the theoretical arrival
// time (TAT, microseconds on the server clock) of the next permit.
//
// A permit is granted when TAT <= now; the new TAT is max(TAT, now - slack) +
// interval. slack lets a taker that arrives a little after TAT (timer and
// network latency) keep the grid instead of pushing every later permit back
// by its lateness, which would pace below the rate. Successive grants are
// still anchored at least one interval apart, so any one-second window holds
// at most (1s + slack) / interval + 1 grants.
//
// KEYS[1] channel key; ARGV[1] interval us; ARGV[2] slack us.
// Returns {granted (0|1), wait us until the next permit}.
const gcra = `
local interval = tonumber(ARGV[1])
local slack = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local tat = tonumber(redis.call('GET', KEYS[1]) or '0')
if tat > now then
  return {0, tat - now}
end
local nt = math.max(tat, now - slack) + interval
redis.call('SET', KEYS[1], string.format('%.0f', nt), 'PX', math.ceil((nt - now) / 1000) + 1000)
return {1, nt - now}
`

var script = valkey.NewLuaScript(gcra)

// Limiter is the Valkey core.Limiter.
type Limiter struct {
	client   valkey.Client
	interval time.Duration
	prefix   string
}

var _ core.Limiter = (*Limiter)(nil)

// New paces each channel at perSecond reduced by headroomPct percent; the
// keys are "<prefix><channel>".
func New(client valkey.Client, perSecond, headroomPct int, prefix string) *Limiter {
	pace := float64(perSecond) * float64(100-headroomPct) / 100
	if pace <= 0 {
		pace = 1
	}
	return &Limiter{client: client, interval: time.Duration(float64(time.Second) / pace), prefix: prefix}
}

// Interval is the time between two permits on one channel.
func (l *Limiter) Interval() time.Duration { return l.interval }

// Take asks for up to n permits and returns how many were granted now (at
// most one: burst 1). wait is how long until the next permit.
func (l *Limiter) Take(ctx context.Context, channel core.Channel, n int) (int, time.Duration, error) {
	if n <= 0 {
		return 0, 0, nil
	}
	us := l.interval.Microseconds()
	vals, err := script.Exec(ctx, l.client, []string{l.prefix + string(channel)},
		[]string{fmt.Sprint(us), fmt.Sprint(us / 2)}).AsIntSlice()
	if err != nil {
		return 0, 0, fmt.Errorf("rate limiter: %w", err)
	}
	if len(vals) != 2 {
		return 0, 0, fmt.Errorf("rate limiter: unexpected reply %v", vals)
	}
	return int(vals[0]), time.Duration(vals[1]) * time.Microsecond, nil
}
