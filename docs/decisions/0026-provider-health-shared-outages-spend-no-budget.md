---
status: accepted
date: 2026-11-15
decision-makers: owner (direction, in chat, 15 Nov 2026); coordinator (delegated: attribution and numbers)
revisit: when a provider's real error mix is measured (the false-trip numbers below assume random failures), or when one Valkey is no longer enough
---
# Provider health is shared in Valkey per channel, and a failure counts against a message only while its provider is believed healthy (0026)

## Question
When a channel's provider goes down, how does the whole fleet learn it once, stop sending, probe once per cool-down, and resume together, and how does an outage of any length dead-letter nothing while a message whose own sends keep failing is still dead-lettered?

## Rule
Every worker gates claims on one health state per channel in Valkey (`internal/delivery/health`); a failed call counts against its notification's budget (`notifications.budget_spent`, bounded by `APP_MAX_ATTEMPTS`) only if the state is still closed after the failure is recorded; every call is still recorded in `delivery_attempts` and `notifications.attempts`.

## Context
Owner, 15 Nov 2026, reviewing 0007 and 0008: "why a magic number again? ... I feel like we should be caching the health and readiness of upstream services we depend on in valkey or somewhere else no? isn't that done in the industry?" and "you must explain or state every magic number in general".

What was wrong with 0008 as built: one attempt counter mixed a provider-wide outage (nothing wrong with the message: pause, spend nothing) with a message-specific failure (count, back off, dead-letter). The breaker lived in each process, so each worker learnt an outage separately (about 10 failed sends each) and probed separately (three probes per cool-down), and it never tripped under one call per second (`APP_BREAKER_MIN_REQUESTS=10` in a 10 s window): a lone message met an outage with only its retry budget.

## Options
1. **Shared state in Valkey, consecutive faults, attribution at failure time.** One Lua script per step (admit, report, cancel) on Valkey's clock. `APP_BREAKER_FAILURES` consecutive provider-fault results fleet-wide, with no success between, declare an outage; a message's own retries count, so a lone message can declare it. A failure counts only if the state after recording it is closed. Cost: the K-1 failures before the declaration count (one unit each, usually on different messages); one Valkey round trip per result.
2. **As 1, but refund the K-1 after the fact.** Keep the failing notification ids in Valkey; the worker that declares the outage subtracts one from each. Every outage costs no budget at all; but it needs relative writes racing with leases, and cannot revive a message already exhausted by one of those K-1.
3. **As 1, but a ratio over a window** (the old rule, shared). Tolerates a provider that fails a share of calls at random; needs a minimum number of calls, which is what made the old breaker blind under one call per second.
4. **Active health probe** of a provider health endpoint instead of probing with a real message. Probes cost no message; but webhook.site (the real target) has none, and a health endpoint can answer while sends fail.
5. **Keep per-process breakers and the budget as the only defence at low traffic.** Nothing to build; a 30-minute outage dead-letters a lone message (its budget spans at most 13.5 min).

## Decision
Option 1. Option 2's refund saves at most 4 budget units per outage at the price of a racy multi-row write; option 3 fails the low-traffic case the owner raised; option 4 does not apply to the real provider; option 5 is the problem.

How it runs (state hash `health:<channel>`; `internal/delivery/health/valkey_state.go`):
- **Closed.** Calls flow. Each result is reported: a fault adds 1 to `fails`, anything else (2xx, a permanent 4xx: the provider answered properly) resets it. The K-th consecutive fault opens the state: no probe before `max(cool-down, Retry-After)`.
- **Open.** The claim loop of every worker asks the state once per reservation (`Gate.Allow`) and claims nothing, polling at most every `IdleMax` (250 ms). After the cool-down the first worker to ask gets the probe, held for the probe lease: one slot, one claimed row, one call. Probe succeeds: closed, every worker resumes within `IdleMax`. Probe fails: open, next probe a cool-down later. A probe whose holder died is freed by its lease; its late result is from an older generation and changes nothing.
- **Attribution.** The report's reply is the state after recording the result, atomically. Counted: failures while it stays closed (the K-1 before declaration), and permanent failures. Not counted: the declaring failure, every failure of a call in flight when the outage was declared, every probe failure. A failure that does not count reschedules its row due when the next probe may go (or after Retry-After), plus a random share of one cool-down (`HealthCooldown`, set to `APP_BREAKER_COOLDOWN`; no dial of its own), so rows excused together do not come due together, and newer than the waiting backlog, so the next probe takes another row: a poison row cannot keep the channel shut while others wait. The probe goes to the priority lane the weighted round robin picks, so a high-priority poison row gets about 70 % of the probes and a good lower-priority row can wait several cool-downs, not one.
- **Message-specific failures that look like outages.** A provider that answers one recipient with a 503 reports its own fault; K of those in a row among sparse traffic declare an outage, and can again after each recovery. Each declaration takes K-1 counted units, so N such rows declare at most N·B/(K-1) outages before all are dead-lettered (`TestMessageSpecificFailuresStillExhaustTheirBudget`); each costs the channel up to a cool-down.
- **Before each send** the worker checks its ticket against its newest view of the state (`Gate.Current`, no round trip): a row claimed before an outage was declared is released unsent once any of this worker's results said so.
- **Valkey unreachable.** The gate fails closed: nothing is claimed (the limiter is in the same Valkey and fails closed too, 0009). A result whose report cannot reach Valkey is attributed by its admission: a probe's failure does not count, a normal call's does.
- **Valkey restart that loses the state.** The channel reads closed; workers send; K failed reports re-declare the outage. Cost per loss: K-1 counted units fleet-wide, and the calls already in flight when the K-th report lands (up to Horizon + SendAlign worth of slots, about 38 at 95/s), which fail uncounted. Generations are Valkey's clock in microseconds at each change (never less than the previous plus 1), so they keep growing across a loss. A state restored from an older snapshot, or a replica behind, can hold a generation below one a worker has seen: each admit carries the asker's newest generation and the script raises the stored one above it, so no worker's cached view sticks.
- **What an outage can still dead-letter.** Nothing, except: the K-1 counted failures can land on a message with that few units left; a failure whose report cannot reach Valkey counts; and with `APP_MAX_ATTEMPTS` below K (allowed as a deliberate fail-fast choice; the worker warns at start) a lone message spends its budget before its outage can be declared.

**Low traffic: one message, a 30-minute outage.** It fails at about 0, 1, 3, 7 and 15 s (the mean waits with base 1 s); the 5th failure declares the outage and does not count: budget spent 4 of 10. It is then the probe each time it comes due: a cool-down plus a random share of another after its last failure, every 5 to 10 s (at most 360 recorded calls in 30 minutes, about 240 on average; budget unchanged). It is sent by its first call after recovery, within two cool-downs (10 s) plus a round trip. A lone message whose failure is its own is indistinguishable from an outage: it stays pending, probed once per cool-down, until any other message's probe succeeds; from then its failures count again and it is dead-lettered after its remaining budget.

## Numbers
| Number | Value | Where it comes from |
|---|---|---|
| `APP_BREAKER_FAILURES` (K) | 5 | Envoy outlier detection's `consecutive_5xx` default. Targets it meets: an outage is declared within 5 failed reports fleet-wide (at 95/s about 53 ms, with up to about 38 more calls already in flight, excused; a lone message with base 1 s, 15 s mean and 30 s worst); an outage costs the fleet at most K-1 = 4 budget units. False trips at the paced 95/s, from P(a run of 5) ≈ (1-p)·p⁵ per call: a provider failing p = 10 % of calls at random trips about once per 20 min, 20 % about every 40 s, 30 % about every 6 s; each costs at most one cool-down of sending, no budget beyond 4 units |
| `APP_BREAKER_COOLDOWN` | 5 s | Target: sending resumes within 5 s plus one probe round trip of recovery. Hystrix's default sleep window. Cost: one probe per 5 s fleet-wide, 720 calls per hour of outage, 0.2 % of the channel's 342 000 per hour |
| probe lease | AlignCeiling + max(Horizon, AlignCeiling) + one interval + provider timeout + leaseMargin = 3.75 + 3.75 + 0.0105 + 5 + 0.5 = 13.01 s at the defaults, computed (`probeLease`; the alignment's ceiling is lease/8 since adaptive alignment, the margin 500 ms since decision 0018's "As built") | The longest a live probe can take from admission to report; a holder that dies delays the next probe by at most this |
| `HealthCooldown` (spread of excused due times) | = `APP_BREAKER_COOLDOWN`, 5 s | Rows excused by one declaration come due spread over the cool-down after the next probe, not together at it; no dial of its own |
| `probeWait` (20 ms) | removed | Waiters ask again at `min(time left, IdleMax)`; `IdleMax` is the idle poll's existing dial |
| `APP_BREAKER_WINDOW`, `_MIN_REQUESTS`, `_FAILURE_RATIO` | removed | Replaced by K (option 3 not chosen) |
| `APP_RETRY_BASE` | 1 s | AWS SDK "standard" retry mode: full jitter, base 1 s. The first retry comes within 2 s |
| `APP_RETRY_CAP` | 5 min | Target: once a message-specific failure clears, the message goes out within 5 min; time-sensitive notifications (one-time codes) commonly expire in 5 to 10 min |
| `APP_MAX_ATTEMPTS` (B) | 10 | The smallest budget whose waits reach the cap: ceil(log2(cap/base)) + 1 = ceil(8.23) + 1 = 10 (with 9 the largest wait is 256 s and the cap is dead). Also ≥ K, so a lone message survives its outage's detection with 6 left; a smaller value is allowed and warned at start. A message-specific failure is retried for 405 s (6.75 min) on average, 810 s (13.5 min) at most |
| 10-minute outage target of 0007 | gone | An outage no longer spends budget, so the budget no longer has to outlast one |

## Confirmation
- `internal/delivery/health/state_contract_test.go`: the rules above against `MemoryState`, and against the Valkey script (`valkey_state_integration_test.go`, tag `integration`): threshold, reset, one probe per cool-down under 20 concurrent askers, a dead probe freed by its lease and its late result ignored, cancel, Retry-After, generations above the old ones after a wipe, and above the asker's after a restore from an older snapshot. `gate_test.go`: a gate keeps sending after the state goes back to an older generation; a probe ticket stops being current when its lease ends.
- `internal/delivery/failure_attribution_test.go`: the outcome table; excused due times spread over a cool-down; a worker over an outage 20 cool-downs long that spends at most K-1 units, dead-letters nothing and probes about once per cool-down; 8 message-specific 503 rows among sparse traffic declare at most N·B/(K-1) outages and are all dead-lettered.
- `internal/delivery/retry_numbers_test.go`: the default budget is ceil(log2(cap/base)) + 1, at least K, and reaches the cap.
- The database: `budget_within_attempts` CHECK (`budget_spent <= attempts`).
- Acceptance (rows B1 and R12): four tests drafted in `.agents/pr/provider-health-design.md`, held until owner item 0002 settles who may write `test/acceptance/` from a worktree.
