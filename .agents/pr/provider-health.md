# feat(delivery): provider health shared in Valkey; an outage spends no attempt budget

Branch `claude/feat-shared-provider-health-d8f5a2`, from `main`. Record: `docs/decisions/0026-provider-health-shared-outages-spend-no-budget.md`. It supersedes 0008 and rewrites the numbers of 0007. Design digest and the four drafted acceptance tests: `.agents/pr/provider-health-design.md`. Verifier report: `.agents/reviews/2026-11-15-provider-health.md`.

## What changes
- **One health state per channel, shared by every worker** (`internal/delivery/health`). One Lua script with three operations (admit, report, cancel) runs atomically on Valkey's clock. It keeps the hash `health:<channel>` with the fields gen, fails, open, probe_after and probe_until. The per-process `internal/delivery/breaker` is deleted.
- **Trip rule.** `APP_BREAKER_FAILURES` (K = 5) consecutive provider-fault reports fleet-wide declare an outage. A success, or a permanent 4xx, resets the count. A message's own retries count toward K, so a lone message can declare an outage.
- **One probe per cool-down, fleet-wide.** After `max(cool-down, Retry-After)`, the first worker to ask gets the probe. It is held for the probe lease (Horizon + SendAlign + one interval + min(provider timeout, Lease) + 250 ms ≈ 5.66 s). If its holder dies, the lease frees it. Its late result belongs to an older generation and is ignored. A probe ticket also stops being current on its own clock when the lease ends.
- **Generations** are Valkey µs at each change, `max(now, gen+1)`. Each admit carries the asker's newest generation, and the script raises the stored generation above it. So a wiped state, a state restored from a snapshot, or a replica behind never strands a worker's cached view.
- **Gate** (one per channel per worker). It caches the newest reply. Before each send, `Current(ticket)` checks the ticket locally with no round trip: a row claimed before a declaration this worker has seen is released unsent.
- **Valkey unreachable: the gate fails closed.** Nothing is claimed, and the limiter in the same Valkey fails closed too (0009). The worker logs it once and polls every `IdleMax`. A result whose report cannot reach Valkey is attributed by its admission: a probe's failure is excused, a normal call's failure counts.
- **Schema.** `notifications.budget_spent INT UNSIGNED NOT NULL DEFAULT 0`, with `CHECK (budget_spent <= attempts)`. It was generated from `schema/` with `task migrate:diff` (`migrations/20261115193607_provider_health_budget.sql`). Replay resets it. It does not touch the retention branch's columns. `attempts` and `delivery_attempts` still record every call. `delivery_attempts` gains nothing; `Attempt.Counted` drives the queue write only.
- **Config.** `APP_BREAKER_WINDOW`, `_MIN_REQUESTS` and `_FAILURE_RATIO` are removed. `APP_BREAKER_FAILURES` is added (default 5, range 1..10000). The `APP_MAX_ATTEMPTS` default goes from 12 (0007 as built) to 10. At start the worker logs its retry span, and it warns when `APP_MAX_ATTEMPTS < APP_BREAKER_FAILURES`.

## Attribution: how a failure is judged (decided at failure time)
The report's reply is the state right after the result is recorded, read atomically. **A failure counts only if that state is still closed.**

| Failure | Counts? | Why |
|---|---|---|
| The K-1 faults before the declaration | yes, 1 unit each, usually on different messages | They are indistinguishable from message-specific failures until the K-th arrives. Refunding them would need a racy multi-row write and saves at most 4 units per outage (option 2, not chosen) |
| The declaring (K-th) failure | no | Its report opens the state |
| Calls in flight when the outage was declared (up to about 38 at 95/s) | no | Their reports find the state open |
| Probe failures | no | The provider is believed down |
| Permanent (4xx) | yes, and the message fails | It is about the message, whatever the health |
| Report cannot reach Valkey | probe: no; normal call: yes | Attributed by the admission the call held |

An excused row is rescheduled to `max(time until the next probe, Retry-After) + rand(0, HealthCooldown)`. `HealthCooldown` is set to `APP_BREAKER_COOLDOWN`. The random share keeps rows excused together from coming due together, and they land after the waiting backlog, so the next probe takes a different row.

Options considered (0026): shared state with consecutive faults and failure-time attribution (chosen); refunding the pre-declaration units; a fleet-wide failure ratio over a window (fails the low-traffic case); provider status pages (the real provider has none); keeping per-process breakers (the problem).

## Low traffic: one message, a 30-minute outage (chosen behaviour)
The message fails at about 0, 1, 3, 7 and 15 s (mean waits with base 1 s). The 5th failure declares the outage and is not counted, so it has spent 4 of its 10 units. From then on it is the probe whenever it comes due: one cool-down plus a random share of another after its last failure, so every 5 to 10 s. That is at most 360 recorded calls in 30 minutes, about 240 on average, and no budget is spent. It is sent by its first call after recovery, within two cool-downs (10 s) plus a round trip.

The alternative was to let the budget outlast the outage, as 0007 as built did with 12 attempts and a 23.5-minute worst span. That still dead-letters the message in a 30-minute outage, so it does not meet the case.

Known cost: a lone message whose failure is its own (a 503 meant for one recipient) cannot be told apart from an outage. It stays pending, probed every 5 to 10 s, until any other message's probe succeeds; after that its failures count again.

## Numbers (each derived; the same rows are in 0026 and the config doc lines)
| Number | Value | Where it comes from |
|---|---|---|
| `APP_BREAKER_FAILURES` (K) | 5 | Envoy outlier detection's `consecutive_5xx` default. An outage is declared within 5 failed reports, about 53 ms at 95/s; a lone message takes 15 s on the mean waits, 30 s worst. An outage costs the fleet at most K-1 = 4 units. False trips at 95/s, from (1-p)·p⁵ per call: at p = 10 % about every 20 min, at 20 % about every 40 s, at 30 % about every 6 s. Each costs at most a cool-down of sending |
| `APP_BREAKER_COOLDOWN` | 5 s | Target: sending resumes within 5 s plus one probe round trip after recovery. Hystrix's default sleep window. Cost: 720 probes per hour of outage, 0.2 % of the channel's 342 000 calls per hour |
| probe lease | ≈ 5.66 s, computed | Horizon 0.2 + SendAlign 0.2 + interval 0.0105 + provider timeout 5 + leaseMargin 0.25 s: the longest a live probe takes from admission to report |
| `HealthCooldown` (spread of excused due times) | = cool-down, 5 s | Rows excused by one declaration come due spread over a cool-down, not together; no dial of its own |
| `APP_RETRY_BASE` | 1 s | AWS SDK "standard" mode: full jitter, base 1 s |
| `APP_RETRY_CAP` | 5 min | Target: once a message-specific failure clears, the message goes out within 5 min; one-time codes commonly expire in 5 to 10 min |
| `APP_MAX_ATTEMPTS` (B) | 10 | The smallest budget whose waits reach the cap: ceil(log2(300/1)) + 1 = 10 (with 9 the cap is never reached). It is ≥ K, so a lone message survives detection with 6 units left. A message-specific failure is retried for 405 s on average and 810 s at most (`RetrySpan`; `retry_numbers_test.go`) |
| 0007's 10-minute outage target | removed | An outage no longer spends budget |

## Before / after: a 10-minute sms outage
Setup: `internal/delivery/provider_health_measure_test.go`, tag `integration`, with `MEASURE_PROVIDER_HEALTH=1`. Three workers share MySQL, Valkey and the fake provider; a stream sends one sms every 100 ms; the provider returns 503 for 10 minutes, then accepts. Only the `measureVariant` block differs between runs.
- **Before** = `main` at `ebedafb`: per-process breakers, `APP_MAX_ATTEMPTS` 5, budget counted in `attempts`.
- **After** = this branch.

| Metric | Before (main) | After, no spread | After, with spread (final) |
|---|---|---|---|
| Failed calls per worker before it stopped | 10, 7, 7 | 1, 2, 3 | 2, 1, 3 |
| sms calls per cool-down in the steady outage | 2.33 (one probe per worker) | 0.96 | 0.96 |
| Budget spent per notification, max / mean | 5 / 1.068 | 1 / 0.0007 | 1 / 0.0007 |
| Calls made per notification, max / mean | 5 / 1.068 | 3 / 1.02 | 3 / 1.02 |
| Notifications dead-lettered | 0 | 0 | 0 |
| Recovery to first accepted sms | 4 159 ms | 4 783 ms | 2 219 ms |
| Drain: busiest second / mean rate / duration | 97 / 27.3 per s / 168 s | 97 / 91.5 per s / 70 s | 97 / 32.0 per s / 187 s (see the tail below) |
| Notifications created before recovery | 4 496 | 5 997 | 5 960 |

How to read it:
- **Before.** Each worker learnt the outage by itself and probed by itself. Its retries were scattered over backoffs of up to 5 minutes, so the drain crawled at 27/s. On main, 10 minutes is shorter than the worst span (5 attempts), so nothing was dead-lettered in this run. A 30-minute outage would have dead-lettered.
- **After.** The fleet stops within 6 failed calls in total. It probes about once per cool-down. The backlog drains at the paced rate (the busiest second, 97, is under the 100/s limit).
- **The drain of the final run has an unexplained tail.** About 5 580 rows went out at 63 to 91 per second within 80 s of recovery. Then about 320 rows that had never been called (attempt 1, sms normal) sat unsent twice, for 40 s and then 50 s. Each wait matches one lease period (30 s lease plus the 5 s reap). Nothing was logged: no failed release, no lost lease, no claim error, no warning for sms. The spread cannot cause this, because it only moves excused rows and those rows had never been called. The run without the spread had no tail (70 s drain). It was seen once and is **not reproduced and not explained**. I suspect a claim that committed after its caller gave up, under host load, but have not checked.
- **Row counts differ** (4 496 vs 5 997) because the "before" intake ran slower beside the busier retry traffic. Compare per-row means, not totals.

Also measured, before the verifier fixes: a 40 s smoke outage. Failed calls per worker 1, 5, 1; 1.0 calls per cool-down; budget max 1, sum 4; first accepted 2.2 s after recovery; drain mean 70/s.

## Verifier findings and how each was handled
Fresh-context `verifier` on `6264432` and `d30265c`. Verdict FAIL; report saved verbatim.

| # | Finding | Handling |
|---|---|---|
| 1 HIGH | A state restored to an older generation (RDB reload, a replica behind) stopped a channel for good: the gate kept the newer cached generation, so no ticket was ever current | **Fixed** in `1c5421d`. Admit sends the asker's newest generation, and the script and `MemoryState` raise the stored one above it. Tests: contract case "a state restored from an older snapshot" (memory and Valkey) and `TestAGateKeepsSendingAfterTheStateGoesBackToAnOlderGeneration` |
| 2 HIGH | The B1/R12 acceptance tests are only drafted, and the existing suite's env still sets removed dials | **Held** by the coordinator for owner item 0002 (agentguard denies test-writer in a worktree). The four tests are drafted in `provider-health-design.md`; the env patch is in `provider-health-acceptance-env.patch`, unapplied, for the coordinator's review. `test/acceptance/` is unchanged from main |
| 3 MEDIUM | Rows excused together came due together and could re-declare an outage again and again | **Fixed in part** in `10b2322`. Excused due times are spread over a cool-down (`TestExcusedFailuresComeDueSpreadOverACooldown`). An 8-row scenario showed the spread does not change how many outages message-specific 503s declare among sparse traffic: 14 to 15 with or without it, because their own retries interleave. So the bound that matters is tested: each declaration takes K-1 counted units, so N rows declare at most N·B/(K-1) outages and all are dead-lettered (`TestMessageSpecificFailuresStillExhaustTheirBudget`, 8 rows: ≤ 16). The residue is documented in 0026 |
| 4 MEDIUM | An outage can still dead-letter: K-1 units landing on a nearly spent message, an unreachable report, `MaxAttempts < K` | **Wording fixed** in 0026 ("What an outage can still dead-letter"). `MaxAttempts < K` is **warned at start, not refused**: a small budget is a deliberate fail-fast choice, and the existing R12 acceptance test runs 3 against 5 |
| 5 LOW | Two probes could be out at once after a slow claim or report | **Fixed** in `1c5421d`. A probe ticket stops being current when its lease ends on the local clock, so its send is refused (`TestAProbeTicketExpiresWithItsLease`) |
| 6 LOW | A high-priority poison row gets about 70 % of the probes | **Documented** in 0026; not changed. Probes follow the same weighted lanes as every other send |
| 7 LOW | Wording: reports, not calls; "about 360" is an upper bound; 0007's stale "12 attempts", test name and log | **Fixed** in 0026 and 0007 (`571a2ae`). The retry-span start-up log that 0007 cites now exists (`cmd/app`) |

## Gates
Run on 15 and 16 Nov 2026 on the owner's Windows host. Ten containers from other stacks were running beside the gate.

| Check | Result | Duration |
|---|---|---|
| `go test -race ./internal/delivery/...` (narrow, final change set) | pass | delivery 30.4 s, health 2.0 s |
| `go test -tags integration ./internal/delivery/health/` (Valkey contract, restored-generation case included) | pass | 9.7 s |
| `task check`, run 1 | **fail**: lint (contextcheck on the probe cancel helpers, forbidigo on the measurement's env knobs) | 256 s |
| `task check`, run 2 after `88f3247` | pass: build, vet, lint 0 issues, unit tests, `govulncheck` no vulnerabilities | 134 s |
| `task test:integration` (`./internal/...`; ran before `88f3247`, which changes no behaviour) | **6 failures of 377** | 336 s |
| Rerun of those 6 | 3 pass: the rate test (MySQL had failed to start), intake (Docker provider error), the first clock-step test. 2 still fail, see below | 91 s + 80 s |

Still failing, and what I know:
- **`TestLimiterHoldsTheRateAcrossTakers`** in `internal/delivery/limiter` (93.4/s against a floor of 94.5/s). This package is unchanged from main and has no health code, so the host load is the likely cause.
- **`TestWorkersAcrossSmallLimiterClockSteps`** (one subtest: 102 sends in a sliding second against 101, or pace 3.37 s against 3.16 s). Run alternately with main on the same host: branch passed 2 of 3, main 3 of 3. **Not settled.** It may be host load, or the pacing cost of this branch's extra Valkey round trips (one admit per reservation, one report per result). Rerun it on a quiet host, pinned to two cores, before merging.
- **Acceptance (`test/acceptance/`) was not run.** It is not part of `task test:integration`. Until the held env patch is applied, the suite sets dials that no longer exist and runs with K = 5. Expect the R11 crash test (`delivery_crash_test.go`) and the R12 accounting test (`delivery_gaps_test.go`) to need the patch.
- **The 10-minute measurements** took 828 s (before), 711 s (after, no spread) and 842 s (after, with spread).

## Merge notes for the coordinator
- **Rebased onto integration `d580c4e`** (walkthrough, wake-hint, code-first API). I resolved the conflicts as follows:
  - `config.go` and `config_test.go`: this lane's dials and derivations.
  - `delivery.go`: kept wake-hint's `SafetyPoll`, wake channel and `retryAt`, with the health gate.
  - `delivery_test.go`: kept wake-hint's `claimCalls`, plus the oldest-due claim.
  - CONTRACT, README, 0007 and the superseded 0008: integration's text plus this lane's lines. 0008 keeps its Question, Discussion, Numbers and Follow-up sections.
  - Log: kept both sides.
  - The migration was regenerated with `task migrate:diff` (same SQL, new timestamp). `task gen` changed nothing.
- **`APP_MAX_ATTEMPTS` is 10, not integration's 12.** Integration's 0007 Numbers table derives 12 from the 10-minute outage target, and says that target "goes away if an outage stops spending attempts". 0026 makes outages spend nothing, so the smallest budget that reaches the cap is 10.
- **Retry tests.** I removed integration's `TestTheDefaultRetryBudgetOutlastsATenMinuteOutage` (`retry_budget_test.go`) for the same reason. `TestTheDefaultRetryBudgetIsTheSmallestThatReachesTheCap` replaces it, and `TestRetrySpanSumsTheWaitsBetweenAttempts` stays. `retry_budget.go` is unchanged from integration.
- **Start-up log.** Integration's start line already logs the retry span, so this lane adds only the warning for `APP_MAX_ATTEMPTS < APP_BREAKER_FAILURES`.
- **Record numbers.** 0024 and 0025 are taken by the huma-code-first and retention branches, so this record is 0026.
- **Acceptance.** The held tests and the per-file dial changes are in `provider-health-acceptance.md`. Until those land, the existing suite runs with K = 5 instead of its old dials. Tests at risk: R11 crash (`delivery_crash_test.go`) and R12 accounting (`delivery_gaps_test.go`).
