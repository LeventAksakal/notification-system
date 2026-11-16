# Shared provider health: design digest (15 Nov 2026)

Working digest for the agents on branch `claude/feat-shared-provider-health-d8f5a2`. Facts and rules, no verdicts. The PR description (`.agents/pr/provider-health.md`) is written from it at the end.

## Problem (owner, 15 Nov 2026)
One attempt counter mixes a provider-wide outage (nothing wrong with the message: pause, spend nothing) with a message-specific failure (count, back off, dead-letter). The breaker was per process (each worker learnt an outage separately, about 10 failed sends each, and probed separately) and never tripped under 1 call per second (`APP_BREAKER_MIN_REQUESTS=10` in a 10 s window). Owner: "you must explain or state every magic number in general".

## Rule
1. One health state per channel lives in Valkey, read and changed by every worker in one Lua script per step (atomic, on Valkey's clock). Package `internal/delivery/health`.
2. Trip rule: `APP_BREAKER_FAILURES` (K, default 5) consecutive provider-fault results on the channel, fleet-wide, with no success in between, declare an outage. A message's own retries count toward K, so a lone message can trip it (the low-traffic answer). Any success while healthy resets the count. Provider fault = `provider.Verdict.ProviderFault` (no response, timeout, 408, 425, 429, 5xx). Every other answer (2xx, a permanent 4xx) is a success for health: the provider answered properly.
3. While an outage is declared, no worker claims for the channel except the one holding the probe. One probe per cool-down fleet-wide: after `max(APP_BREAKER_COOLDOWN, Retry-After of the failure that opened or kept it open)` the first worker to ask gets the probe (one claimed row, one call). The probe is held for a probe lease; if its holder dies the lease expires and the next worker may probe. A probe result from an older generation is ignored.
4. Probe succeeds: health closes, every worker resumes (the probe's worker at once, the others on their next gate check, within `IdleMax` = 250 ms). Probe fails: open again, next probe after the cool-down.
5. Attribution (when a failure counts against the message's budget): at the time of the failure, atomically with recording it. The report script records the result and returns the health state after recording it. A provider-fault failure counts only if health is still believed healthy after recording it. So: the K-th failure (the one that declares the outage) is excused, every failure of a call that was in flight when the outage was declared is excused, every probe failure is excused. The K-1 failures before declaration count (one budget unit each, usually on different messages; on the same message when it is alone). A permanent failure always counts (it ends the message anyway).
6. Every call is still recorded in `delivery_attempts` and increments `notifications.attempts` (calls made). A new column `notifications.budget_spent` counts only the failures that count; exhaustion is `budget_spent >= APP_MAX_ATTEMPTS` after a counted failure; the backoff exponent is `budget_spent` (after the increment). Replay resets both to 0. CHECK `budget_spent <= attempts`.
7. An excused failure reschedules the row (attempt recorded, budget unchanged) due at `now + max(time until the next probe may go, Retry-After)`. Its due time is newer than the waiting backlog's, so the next probe takes another row (rotation; a poison row cannot hold the channel shut while other rows wait).
8. Valkey unreachable: the gate fails closed (claims nothing; the limiter lives there too and is down anyway), logged once per outage of Valkey like the limiter. A result whose report cannot reach Valkey is attributed by its admission: a probe's failure is excused, a normal call's failure counts. A probe that cannot be cancelled or reported is freed by its lease expiry.
9. Valkey restart (key wiped) mid-outage: health reads as closed; workers claim and send; K failures re-declare the outage. Cost: K failed calls and K-1 counted budget units fleet-wide per wipe. Generation numbers are Valkey's clock in microseconds at the state change (`max(now, gen+1)`), so they keep growing across a wipe and a worker's cached view (newest generation wins) never sticks on a pre-wipe state.

## Valkey state (hash `health:<channel>`)
Fields: `gen` (int, Valkey µs at the last state change), `fails` (consecutive faults while closed), `open` (0/1), `probe_after` (µs: no probe before), `probe_until` (µs: the current probe's lease end; 0 none).
Ops (one script, op in ARGV[1]); every reply is `{state, gen, wait_us, transition}`; state ∈ closed | probe | wait | open; transition ∈ none | opened | closed:
- admit(probe_lease_us): closed → `closed`; open and a probe out (`probe_until > now`) → `wait` (until probe_until); open and `now < probe_after` → `wait` (until probe_after); else grant: gen bump, `probe_until = now + lease` → `probe`.
- report(ticket_gen, probe, fault, K, cooldown_us, retry_after_us):
  - probe ticket: stale (gen differs, or not open, or no probe out) → current state, no change. fault → gen bump, `probe_after = now + max(cooldown, retry_after)`, `probe_until = 0` → `open`. success → gen bump, closed, fails 0 → `closed`, transition closed.
  - normal ticket: open → `open` (excused; a success changes nothing). success → fails 0 → `closed`. fault → fails+1; at K: gen bump, open, fails 0, `probe_after = now + max(cooldown, retry_after)` → `open`, transition opened; else `closed`.
  - wait_us on an `open` reply = `max(probe_after - now, 0)`.
- cancel(ticket_gen): open, gen matches, probe out → gen bump, `probe_until = 0`, `probe_after = now` (the cool-down is spent; the next asker probes). Else no change.
A missing hash is closed with `gen = now` written on first sight.

## Worker side
- `health.Gate` per channel and worker: wraps the state, caches the newest reply (largest gen) to answer `Current(ticket)` locally before each send (a ticket is current while the cached gen equals its gen and, for a normal ticket, health is closed). Every reply (admit, report, cancel) refreshes the cache.
- The claim loop asks the gate once per reservation (`Allow`), extra slots of a group reuse the first ticket while it is current. Probe: want = 1 slot.
- `delivery.ProbeLease = Horizon + SendAlign + interval + ProviderTimeout + leaseMargin` (0.2 s + 0.2 s + 10.5 ms + 5 s + 0.25 s ≈ 5.66 s): the longest a live probe can take from admission to its report (slot at most Horizon ahead, sent at slot + SendAlign, up to one interval late, call cut at ProviderTimeout, report round trip within leaseMargin).
- Waiters (outage, probe out) sleep `min(wait, IdleMax)`.
- Logs: the worker whose result declares an outage logs a warning `provider outage declared` (channel, failures K); the one whose probe closes it logs `provider recovered` (channel, outage_ms if known).

## Numbers (each with where it comes from)
| Number | Value | Source |
|---|---|---|
| `APP_BREAKER_FAILURES` (K) | 5 | Envoy outlier detection's `consecutive_5xx` default. Targets it meets: an outage is declared within 5 failed calls fleet-wide (at 95/s about 53 ms; a lone message with base 1 s: 15 s mean, 30 s worst); an outage costs the fleet at most K-1 = 4 budget units. False trips at full rate (95/s), from P(run of 5) ≈ (1-p)p^5 per call: a provider failing p = 10 % of calls at random trips about once per 20 min, p = 20 % about every 40 s, p = 30 % about every 6 s; each trip costs at most one cool-down of sending |
| `APP_BREAKER_COOLDOWN` | 5 s | Target: sending resumes within 5 s plus one probe round trip of recovery. Hystrix's default sleep window (5 s). Cost: one probe per 5 s fleet-wide = 720 calls per outage hour, 0.2 % of the channel's 342 000 per hour |
| probe lease | ≈ 5.66 s, computed | `ProbeLease` above: longest a live probe can take; a dead holder delays the next probe by at most that |
| `probeWait` 20 ms | removed | waiters poll at `min(wait, IdleMax)` |
| window, min requests, failure ratio | removed | replaced by K |
| `APP_RETRY_BASE` | 1 s | AWS SDK "standard" retry mode: full jitter, base 1 s. First retry within 2 s |
| `APP_RETRY_CAP` | 5 min | Target: once a message-specific failure clears, the message goes out within 5 min; time-sensitive notifications (one-time codes) commonly expire in 5 to 10 min |
| `APP_MAX_ATTEMPTS` (B) | 10 | Smallest budget whose waits reach the cap: ceil(log2(cap/base)) + 1 = ceil(8.23) + 1 = 10 (with 9 the largest wait is 256 s and the cap is dead). Also ≥ K, so a lone message survives its outage's detection (spends ≤ 4, keeps 6). Span of a message-specific failure: 810 s (13.5 min) worst, 405 s (6.75 min) mean |

## Low-traffic case (one message, 30-minute outage)
Fails at t ≈ 0, 1, 3, 7, 15 s (mean waits); the 5th failure declares the outage and is excused; budget spent 4 of 10. Then it is probed once per cool-down (about 360 recorded calls, budget unchanged), and is sent by the first probe after recovery: within 5 s plus a round trip. Not dead-lettered. A lone message whose failure is its own (indistinguishable) stays pending and is probed once per cool-down until any other message's probe succeeds; from then its failures count again and it is dead-lettered after its remaining budget.

## Not chosen
- Active health endpoint: webhook.site (the real target) has none, and a health endpoint can answer while sends fail.
- Falling back to the retry budget: a 30-minute outage dead-letters the lone message (its budget spans 13.5 min at most).
- Refunding the K-1 pre-declaration failures after the fact: needs the failing ids in Valkey, relative writes, and cannot revive a message already exhausted.
- Ratio over a window (the old rule): needs MIN_REQUESTS, which is what made it blind under 1 call per second.

## Acceptance tests drafted, not written (held: agentguard denies test-writer in worktrees, owner item 0002)
Drafted by the test-writer on 15 Nov 2026; to be written by test-writer once the guard decision is made. The env patch for the existing tests (`.agents/pr/provider-health-acceptance-env.patch`) is unapplied, for the coordinator's review.
- **Long outage, one probe per cool-down fleet-wide (B1)**, `TestDeliveryB1SharedHealthLongOutage`: `APP_BREAKER_FAILURES=5`, `APP_BREAKER_COOLDOWN=1s`, base 100 ms, cap 1 s, `APP_MAX_ATTEMPTS=5`; 3 workers; 300 sms + 600 email + 600 push, one sms every 500 ms during a 12 s sms outage (5 retry spans of 0.2+0.4+0.8+1.0 = 2.4 s; 12 cool-downs). Asserts: nothing failed, then all sent; sum of `budget_spent` ≤ 4; sms requests after a 1.5 s settle ≤ ⌊window/1 s⌋+1 (settle = 53 ms detection + 200 ms horizon + 200 ms send alignment + 500 ms timeout + 250 ms idle poll, rounded up); first accepted sms ≤ 1 s cool-down + 1 s slack after recovery; sms busiest second ≤ 100; email and push during the outage ≥ the suite's rate floor (88/s).
- **Message-specific failure is dead-lettered (R12)**, `TestDeliveryR12MessageSpecificFailureDeadLetters`: `Fault.To` on one recipient (503 always), an sms stream every 50 ms, `APP_MAX_ATTEMPTS=10`. Asserts: that notification ends failed, exhausted, `budget_spent` = 10; every other one sent.
- **Lone message outlives an outage (B1)**, `TestDeliveryB1LoneMessageOutlivesOutage`: as the long-outage dials with `APP_MAX_ATTEMPTS=5` (= K, the edge), one sms, a 12 s outage. Asserts: never failed; sent within 2 s of recovery; `budget_spent` ≤ 4; `attempts` equals its `delivery_attempts` rows.
- **Measurement**: superseded by `internal/delivery/provider_health_measure_test.go` (tag `integration`, `MEASURE_PROVIDER_HEALTH=1`), which runs on main with only its `measureVariant` block changed.
- Existing tests that need the new dial names (in the patch): `harness_test.go` default env, `observe_metrics_test.go`, `delivery_crash_test.go` (`APP_BREAKER_FAILURES=1000` to keep the breaker out), `delivery_gaps_test.go` (R11, R12 accounting: 1000; hung channel: 10000; `dgB1Env`: 5 and 1 s; `BreakerBurnsNoAttempts` asserts `budget_spent`, not `attempts`, against `APP_MAX_ATTEMPTS`), `delivery_retry_test.go` (`TestDeliveryR12Retry` keeps `APP_MAX_ATTEMPTS=3` below `APP_BREAKER_FAILURES=5`, since a lone always-failing message is otherwise an outage by design). Until they change, those tests set removed dials that `config.Load` ignores, so the default K = 5 applies to them.
