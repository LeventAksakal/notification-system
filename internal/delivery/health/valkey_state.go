package health

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// step is the whole state machine (package doc) as one script, so every step
// is atomic across workers and runs on Valkey's clock. State: hash KEYS[1]
// with gen, fails (consecutive faults while closed), open (0/1), probe_after
// (us: no probe before) and probe_until (us: the probe's lease end; 0 none).
// A missing hash is closed; its first step writes gen = now, so a wiped
// state starts above every generation handed out before. A state restored
// from an older snapshot (or a replica behind) can hold a generation below
// one a worker has seen: admit raises it above the asker's, or that worker
// would never find its tickets current again.
//
// ARGV[1] is the op:
//
//	admit  ARGV[2] probe lease us, ARGV[3] the newest generation the asker has seen
//	report ARGV[2] gen, ARGV[3] probe 0|1, ARGV[4] fault 0|1, ARGV[5] failures,
//	       ARGV[6] cool-down us, ARGV[7] Retry-After us
//	cancel ARGV[2] gen
//
// Returns {kind, gen, wait us, transition}; kind and transition are the Kind
// and Transition constants.
const step = `
local k = KEYS[1]
local op = ARGV[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local h = redis.call('HMGET', k, 'gen', 'fails', 'open', 'probe_after', 'probe_until')
local gen = tonumber(h[1])
local fails = tonumber(h[2]) or 0
local open = h[3] == '1'
local probeAfter = tonumber(h[4]) or 0
local probeUntil = tonumber(h[5]) or 0
local function us(x) return string.format('%.0f', x) end
if not gen then
  gen = now
  redis.call('HSET', k, 'gen', us(gen))
end
local function bump() gen = math.max(now, gen + 1) end
local function save()
  redis.call('HSET', k, 'gen', us(gen), 'fails', fails, 'open', open and 1 or 0,
    'probe_after', us(probeAfter), 'probe_until', us(probeUntil))
end
local function opened(tr) return {3, gen, math.max(probeAfter - now, 0), tr} end
local function current()
  if open then return opened(0) end
  return {0, gen, 0, 0}
end

if op == 'admit' then
  local seen = tonumber(ARGV[3]) or 0
  if gen < seen then
    gen = math.max(now, seen + 1)
    redis.call('HSET', k, 'gen', us(gen))
  end
  if not open then return {0, gen, 0, 0} end
  if probeUntil > now then return {2, gen, probeUntil - now, 0} end
  if now < probeAfter then return {2, gen, probeAfter - now, 0} end
  bump()
  probeUntil = now + tonumber(ARGV[2])
  save()
  return {1, gen, 0, 0}
end

if op == 'report' then
  local probe = ARGV[3] == '1'
  local fault = ARGV[4] == '1'
  local pause = math.max(tonumber(ARGV[6]), tonumber(ARGV[7]))
  if probe then
    if tonumber(ARGV[2]) ~= gen or not open or probeUntil == 0 then return current() end
    bump()
    probeUntil = 0
    if fault then
      probeAfter = now + pause
      save()
      return opened(0)
    end
    open = false
    fails = 0
    probeAfter = 0
    save()
    return {0, gen, 0, 2}
  end
  if open then return opened(0) end
  if not fault then
    if fails > 0 then redis.call('HSET', k, 'fails', 0) end
    return {0, gen, 0, 0}
  end
  fails = fails + 1
  if fails >= tonumber(ARGV[5]) then
    bump()
    open = true
    fails = 0
    probeAfter = now + pause
    probeUntil = 0
    save()
    return opened(1)
  end
  redis.call('HSET', k, 'fails', fails)
  return {0, gen, 0, 0}
end

if op == 'cancel' then
  if open and tonumber(ARGV[2]) == gen and probeUntil > 0 then
    bump()
    probeUntil = 0
    probeAfter = now
    save()
  end
  return current()
end
return redis.error_reply('health: unknown op ' .. tostring(op))
`

var stepScript = valkey.NewLuaScript(step)

// ValkeyState is the State shared by every worker. Keys are
// "<prefix><channel>".
type ValkeyState struct {
	client valkey.Client
	cfg    Config
	prefix string
}

var _ State = (*ValkeyState)(nil)

// NewValkey returns the shared state on client.
func NewValkey(client valkey.Client, cfg Config, prefix string) *ValkeyState {
	return &ValkeyState{client: client, cfg: cfg, prefix: prefix}
}

// Admit implements State.
func (s *ValkeyState) Admit(ctx context.Context, ch notification.Channel, probeLease time.Duration, seen int64) (Reply, error) {
	return s.run(ctx, ch, "admit", us(probeLease), strconv.FormatInt(seen, 10))
}

// Report implements State.
func (s *ValkeyState) Report(ctx context.Context, ch notification.Channel, gen int64, probe, fault bool, retryAfter time.Duration) (Reply, error) {
	return s.run(ctx, ch, "report", strconv.FormatInt(gen, 10), bit(probe), bit(fault),
		strconv.Itoa(s.cfg.Failures), us(s.cfg.Cooldown), us(retryAfter))
}

// Cancel implements State.
func (s *ValkeyState) Cancel(ctx context.Context, ch notification.Channel, gen int64) (Reply, error) {
	return s.run(ctx, ch, "cancel", strconv.FormatInt(gen, 10))
}

func (s *ValkeyState) run(ctx context.Context, ch notification.Channel, args ...string) (Reply, error) {
	vals, err := stepScript.Exec(ctx, s.client, []string{s.prefix + string(ch)}, args).AsIntSlice()
	if err != nil {
		return Reply{}, fmt.Errorf("provider health %s: %w", args[0], err)
	}
	if len(vals) != 4 {
		return Reply{}, fmt.Errorf("provider health %s: unexpected reply %v", args[0], vals)
	}
	return Reply{Kind: Kind(vals[0]), Gen: vals[1], Wait: time.Duration(vals[2]) * time.Microsecond, Transition: Transition(vals[3])}, nil
}

func us(d time.Duration) string { return strconv.FormatInt(max(d, 0).Microseconds(), 10) }

func bit(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
