package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// giveBack moves the TAT back to ARGV[2] if it is still ARGV[1], the end of
// the grant whose unused slots are given back: no one reserved after it, so
// nothing is scheduled at or past ARGV[2] (the caller vouches that its own
// requests leave before it). The key then lives keyLinger past the new TAT,
// as after a Take.
//
// When someone did reserve after the grant (the TAT is past its end), the
// unused range is remembered instead (KEYS[2]: grant end -> first free
// moment) and returned later: whenever a give-back moves the TAT back to a
// remembered end, it moves on to that range's start, and so on down. So the
// order of give-backs does not matter. Workers woken together reserve one
// after another, and the first one's (the flex group's) give-back used to
// fail behind a later grant that was itself given back a moment after.
//
// Why a remembered range is never handed out twice: it is recorded only when
// the TAT is at or past its end, and the TAT can come back below that end
// only through it (a give-back moves the TAT to a moment inside the top
// grant, a chain moves it to a remembered start, and every grant reserved
// after the range lies above it), which deletes it. So while remembered, none
// of its slots can be reserved again, and its own grant sent nothing at or
// after its start. A range whose moment passed is harmless: a Take that finds
// the TAT not after now restarts at now+lead. The hash lives as long as the
// TAT key did when the range was recorded, so it never outlives its use by
// more than keyLinger.
//
// KEYS[1] channel key; KEYS[2] its given-back ranges; ARGV[1] the grant's
// end, us; ARGV[2] the moment the slots are free again, us; ARGV[3]
// keyLinger us. Returns 1 if the TAT moved back, 2 if the range was
// remembered, else 0.
const giveBack = `
local stored = redis.call('GET', KEYS[1])
if not stored then
  return 0
end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local linger = tonumber(ARGV[3])
local tat = tonumber(stored)
local last = tonumber(ARGV[1])
if tat ~= last then
  if tat < last then
    return 0
  end
  redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
  local ttl = math.ceil((tat - now + linger) / 1000)
  if redis.call('PTTL', KEYS[2]) < ttl then
    redis.call('PEXPIRE', KEYS[2], ttl)
  end
  return 2
end
tat = tonumber(ARGV[2])
local back = redis.call('HGET', KEYS[2], string.format('%.0f', tat))
while back do
  redis.call('HDEL', KEYS[2], string.format('%.0f', tat))
  tat = tonumber(back)
  back = redis.call('HGET', KEYS[2], string.format('%.0f', tat))
end
redis.call('SET', KEYS[1], string.format('%.0f', tat), 'PX', math.ceil((math.max(tat - now, 0) + linger) / 1000))
return 1
`

var giveBackScript = valkey.NewLuaScript(giveBack)

// GiveBack returns g's slots from the moment from on (notification.Limiter).
// from is rounded up to the microsecond, never earlier than the caller asked.
// It reports true only when the next free slot moved back now; a range kept
// for later (see giveBack) reports false.
func (l *Limiter) GiveBack(ctx context.Context, channel notification.Channel, g notification.Grant, from time.Time) (bool, error) {
	if g.N <= 0 || g.At.IsZero() {
		return false, nil
	}
	end := g.At.UnixMicro() + g.First.Microseconds() + int64(g.N)*g.Interval.Microseconds()
	us := (from.UnixNano() + 999) / 1000
	if us >= end {
		return false, nil
	}
	key := l.prefix + string(channel)
	n, err := giveBackScript.Exec(ctx, l.client, []string{key, key + ":returned"},
		[]string{fmt.Sprint(end), fmt.Sprint(us), fmt.Sprint(keyLinger.Microseconds())}).AsInt64()
	if err != nil {
		return false, fmt.Errorf("rate limiter: %w", err)
	}
	return n == 1, nil
}
