// Package limiter is the send-rate limit shared by every worker (decision
// 0009): GCRA with burst 1 in Valkey, one key per channel, on Valkey's clock.
// It hands out send slots: moments, exactly one interval apart, at which a
// request may leave.
package limiter

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// gcra reserves up to n consecutive slots. State is the theoretical arrival
// time (TAT, microseconds on the server clock): the next free slot.
//
// A TAT in the past restarts the grid at now (burst 1: unused slots are
// gone). Slots are TAT, TAT+interval, ...; only those at most horizon after
// now are reserved, and the TAT moves past the last one. Slots are therefore
// never closer than one interval, whoever takes them, and any one-second
// window holds at most ceil(1s/interval) of them. There is no slack: a taker
// that comes late finds the grid restarted, never compressed.
//
// KEYS[1] channel key; ARGV[1] interval us; ARGV[2] horizon us; ARGV[3] n;
// ARGV[4] keyLinger us.
// Returns {reserved, us from now to the first slot, us until asking again
// can reserve something (0 when all n were reserved), now in us since the
// Unix epoch on Valkey's clock, us the grid had been free (now minus the
// stored TAT when that is past, else 0; a missing key counts as free for
// keyLinger, the least its absence proves)}. The last value is read before the reservation and changes
// nothing in it.
const gcra = `
local interval = tonumber(ARGV[1])
local horizon = tonumber(ARGV[2])
local want = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local stored = redis.call('GET', KEYS[1])
local tat = tonumber(stored or '0')
local linger = tonumber(ARGV[4])
local idle = linger
if stored and tat < now then
  idle = now - tat
elseif stored then
  idle = 0
end
if tat < now then
  tat = now
end
local first = tat - now
local n = 0
if first <= horizon then
  n = math.floor((horizon - first) / interval) + 1
  if n > want then
    n = want
  end
end
if n > 0 then
  tat = tat + n * interval
  redis.call('SET', KEYS[1], string.format('%.0f', tat), 'PX', math.ceil((tat - now + linger) / 1000))
end
local wait = 0
if n < want then
  wait = math.max(tat - now - horizon, 0)
end
return {n, first, wait, now, idle}
`

var script = valkey.NewLuaScript(gcra)

// keyLinger is how long a channel's key outlives its TAT. A missing key so
// proves only that the grid has been free that long, and that is the
// Grant.Idle reported for it: a caller comparing Idle with a longer bound
// (SendAlign plus an interval at a rate under one per second) must not read
// an expired key as idle enough.
const keyLinger = time.Second

// Limiter is the Valkey notification.Limiter.
type Limiter struct {
	client   valkey.Client
	interval time.Duration
	horizon  time.Duration
	prefix   string
}

var _ notification.Limiter = (*Limiter)(nil)

// New paces each channel at perSecond reduced by headroomPct percent and
// reserves slots at most horizon ahead; the keys are "<prefix><channel>".
//
// The interval is rounded up to the microsecond, so the slots in any sliding
// second never exceed the paced rate: 95 per second (100 with the default 5%
// headroom) gives 10 527 us, and ceil(1s / 10 527 us) = 95.
func New(client valkey.Client, perSecond, headroomPct int, horizon time.Duration, prefix string) *Limiter {
	pace := float64(perSecond) * float64(100-headroomPct) / 100
	if pace <= 0 {
		pace = 1
	}
	us := math.Ceil(1e6 / pace)
	return &Limiter{client: client, interval: time.Duration(us) * time.Microsecond, horizon: max(horizon, 0), prefix: prefix}
}

// Interval is the time between two slots on one channel.
func (l *Limiter) Interval() time.Duration { return l.interval }

// Take reserves up to n consecutive slots on channel, none further ahead
// than the horizon. Grant.At is Valkey's clock when the script ran. With
// n <= 0 the script still runs, reserves nothing and writes nothing: a
// reading of Valkey's clock that costs no slot.
func (l *Limiter) Take(ctx context.Context, channel notification.Channel, n int) (notification.Grant, error) {
	n = max(n, 0)
	vals, err := script.Exec(ctx, l.client, []string{l.prefix + string(channel)},
		[]string{fmt.Sprint(l.interval.Microseconds()), fmt.Sprint(l.horizon.Microseconds()), fmt.Sprint(n), fmt.Sprint(keyLinger.Microseconds())}).AsIntSlice()
	if err != nil {
		return notification.Grant{}, fmt.Errorf("rate limiter: %w", err)
	}
	if len(vals) != 5 {
		return notification.Grant{}, fmt.Errorf("rate limiter: unexpected reply %v", vals)
	}
	return notification.Grant{
		N:        int(vals[0]),
		At:       time.UnixMicro(vals[3]),
		First:    time.Duration(vals[1]) * time.Microsecond,
		Interval: l.interval,
		Wait:     time.Duration(vals[2]) * time.Microsecond,
		Idle:     time.Duration(vals[4]) * time.Microsecond,
	}, nil
}
