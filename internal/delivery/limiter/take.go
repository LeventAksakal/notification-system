package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// gcra reserves up to n consecutive slots. State is the theoretical arrival
// time (TAT, microseconds on the server clock): the next free slot.
//
// A TAT not after now (a free grid) restarts the grid at now+lead (burst 1:
// unused slots are gone, and the time up to now+lead is the taker's claim, in
// which nobody else is sending). A TAT after now (a busy grid) continues
// where it is, even when that is sooner than now+lead: other takers are
// using the grid, and skipping ahead would leave a gap they cannot fill,
// while the taker's claim may well be quicker than its lead (a slot it then
// misses is given up, never sent late). Slots are first, first+interval, ...;
// only those at most lead+horizon after now are reserved, and the TAT moves
// past the last one. The TAT only ever moves forward, so slots are never
// closer than one interval, whoever takes them and whatever lead each taker
// asks with, and any one-second window holds at most ceil(1s/interval) of
// them. There is no slack: a taker that comes late finds the grid
// restarted, never compressed.
//
// KEYS[1] channel key; ARGV[1] interval us; ARGV[2] lead us; ARGV[3]
// horizon us; ARGV[4] n; ARGV[5] keyLinger us.
// Returns {reserved, us from now to the first slot, us until asking again
// can reserve something (0 when all n were reserved), now in us since the
// Unix epoch on Valkey's clock, us the grid had been free (now minus the
// stored TAT when that is past, else 0; a missing key counts as free for
// keyLinger, the least its absence proves)}. The last value is read before the reservation and changes
// nothing in it.
const gcra = `
local interval = tonumber(ARGV[1])
local lead = tonumber(ARGV[2])
local horizon = tonumber(ARGV[3])
local want = tonumber(ARGV[4])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local stored = redis.call('GET', KEYS[1])
local tat = tonumber(stored or '0')
local linger = tonumber(ARGV[5])
local idle = linger
if stored and tat < now then
  idle = now - tat
elseif stored then
  idle = 0
end
if tat <= now then
  tat = now + lead
end
local first = tat - now
local last = lead + horizon
local n = 0
if first <= last then
  n = math.floor((last - first) / interval) + 1
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
  wait = math.max(tat - now - last, 0)
end
return {n, first, wait, now, idle}
`

var script = valkey.NewLuaScript(gcra)

// keyLinger is how long a channel's key outlives its TAT. A missing key so
// proves only that the grid has been free that long, and that is the
// Grant.Idle reported for it: a caller comparing Idle with a longer bound
// (one interval, at a rate under one per second) must not read
// an expired key as idle enough.
const keyLinger = time.Second

// Take reserves up to r.N consecutive slots on channel, none later than
// r.Lead+r.Horizon after Valkey's now; on a free grid the first is r.Lead
// after it.
// Grant.At is Valkey's clock when the script ran. With r.N <= 0 the script
// still runs, reserves nothing and writes nothing: a reading of Valkey's
// clock that costs no slot.
func (l *Limiter) Take(ctx context.Context, channel notification.Channel, r notification.SlotRequest) (notification.Grant, error) {
	n := max(r.N, 0)
	vals, err := script.Exec(ctx, l.client, []string{l.prefix + string(channel)},
		[]string{fmt.Sprint(l.interval.Microseconds()), fmt.Sprint(max(r.Lead, 0).Microseconds()),
			fmt.Sprint(max(r.Horizon, 0).Microseconds()), fmt.Sprint(n), fmt.Sprint(keyLinger.Microseconds())}).AsIntSlice()
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
