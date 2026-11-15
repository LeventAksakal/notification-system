# feat(delivery): idle workers wake on Valkey hints and sleep until the next due time

> **Status: not merge-ready.** The owner asked for no push and no PR (the coordinator merges locally). This file is what the PR description would have been. Two things stay open:
>
> - **R8 throughput floor:** not shown green after the verifier's fixes. The machine was saturated by other sessions; in paired runs base itself failed to drain.
> - **Compose re-measure:** the five scenarios were measured before those fixes; they were not measured again.
>
> Re-run both on a quiet machine before merging.

Idle delivery workers no longer poll the database. They wake on a Valkey hint published after every commit that makes a row pending, and otherwise sleep until the next due time or a 1 s safety poll. A group sent after an idle grid leaves right after its claim. A lone notification after idle reached the provider in p50 22.8 ms / p99 46.8 ms; it was 397 / 539 ms. MySQL stays the only queue, and hints are best effort.

## Measurements (compose, before the verifier fixes)

Before = `main` (ebedafb), after = 4fbc9ba, on the same compose stack (`notifier-wake`, moved ports), run one after the other.

- **Load during both runs:** 17 containers on the machine, 80-230% CPU of 1600%.
- **Latency:** provider `receivedAt` − API `createdAt`, both read on the Docker VM clock.
- **Tool:** wakebench, a scratch Go program.

| Scenario | Before (main) | After | Target |
|---|---|---|---|
| 1. Lone send after ≥5 s idle (50 samples) | p50 396.9 / p95 502.3 / p99 538.7 ms | **p50 22.8 / p95 39.8 / p99 46.8 ms** | p50 ≤ 30, p99 ≤ 100: met |
| 2. Scheduled lateness, `receivedAt − dueAt` (30) | p50 378.5 / p99 499.4 ms | p50 24.0 / p95 329.0 / p99 402.4 ms | better; tail explained below |
| 3. Burst of 2000 | first message 881 ms; busiest second 96; steady 94.9/s; drain 21.10 s | first message 163 ms; busiest sliding second 96 (whole 95); steady 94.9/s; drain 21.39 s | ≤ 100/s, no regression: met |
| 4. Idle MySQL `Com_select`/s | 108.1 | 21.6 | lower |
| 5a. Valkey paused 5 s, a notification meanwhile | back → delivered p50 347 ms | p50 314 ms (27-378), nothing lost | no regression: met |
| 5b. Valkey killed, restarted | 1654-2942 ms after `docker start` | 1043-2971 ms, nothing lost | no regression: met |

**What the verifier fixes change in these numbers.** A full group no longer moves earlier (finding 1).

- **Scenario 3:** the burst's first message goes back to about SendAlign plus the claim, roughly 200-250 ms instead of 163 ms. Expected from the code, not measured.
- **Scenarios 1 and 2:** a lone or scheduled row fills less than its group, so they are unchanged in logic. R7 after the fixes, pinned to 2 cores under full machine load: p50 23.4 ms, p95 110.7 ms, p99 132 ms (pass).

**The poll was not the main cost.** Every send waits `SendAlign` (200 ms) after its slot, so a hint alone could never get below about 200 ms. The measurement pointed there, so the change also lets an idle channel skip the alignment. The coordinator agreed to that scope, and the proof is below.

**Scheduled tail (scenario 2).** The bench puts 10 rows per channel at random within 8 s, so about a quarter of them fall within 210 ms of the previous row on their channel. The previous group's unused reservation keeps the limiter grid busy, so such a row is not flex and still pays `SendAlign`. Follow-up: give unused slots back to the limiter.

## What changed

- **Hint after commit.** Intake (create, batch, replay) and the worker (release, reschedule, reap) publish `{"priority","due"}` to `notifier:wake:<channel>` after the commit (`internal/wake`). The publish has a 100 ms timeout (the owner's p99 target) and a hold-off after a failure, so a Valkey outage never slows the API.
- **Workers wake on the hint.** Each worker's `wake.Listener` subscribes and resubscribes after a drop. On every (re)subscribe each priority is claimed at once, so hints lost while the subscription was down cost nothing. A subscription lost after it held for a safety poll is retried at once; any other retry waits a safety poll, so a flapping server cannot cause a loop.
- **Sleep until the next due time.** An empty priority asks `Queue.NextDue`: one `MIN(due_at)` per priority over `claim_idx`. EXPLAIN shows `Select tables optimized away` (tested). The priority sleeps until that time, a hint, or the safety poll.
- **Safety poll, 1 s (`wake.SafetyPoll`).** It bounds what a lost hint costs. A poll reads `NextDue` only and claims nothing unless a row is due.
- **A hint during a round.** A hint that arrives while a claim is running survives that round's "empty" verdict (`selector.beginRound` / `settle`), so a hint that races its commit is not lost.
- **Flex group.** The limiter now returns `Grant.Idle`: how long its grid was free, read on Valkey's clock. A missing key reports the 1 s its absence proves (`limiter.keyLinger`). When `Idle ≥ SendAlign + interval` and the claim found fewer rows than slots (the backlog ended within the group), the group leaves right after its claim, never before its slots. A full group keeps its moments; moving it earlier left a hole before the next group, which keeps the alignment (verifier finding 1).
- **deferClaim.** Found by measurement: every woken worker reserves slots, and the second one gets a single slot just after the flex group (19 × 10.527 ms ends 13 µs past the 200 ms horizon). SKIP LOCKED then gave it the hinted row in about half the samples, which cost about 400 ms. A worker woken by a hint or a due time, whose group is not flex, now claims at its first slot, so the flex worker claims first. It never does this after waiting for rows another claimer held, or after a safety poll that found rows.
- **Unchanged:** the send path, the limiter's slot reservation, the lease and claim_version rules.
- **Numbers.** `IdleMin` and `IdleMax` keep only their non-idle uses, documented. Every new constant states what it is derived from.

### Hint channel: dedicated `notifier:wake:<channel>`, not `realtime.Channel`

- **realtime (rejected):** every worker would decode every status event of every channel, up to 300/s at full rate. Its events carry no channel, priority or due time, so a worker could not sleep until a scheduled row's due time.
- **Dedicated channel (chosen):** a worker subscribes only to its own channels, and the hint carries exactly what the selector needs.
- **Cost:** one more publish per commit (a batch sends its coalesced hints in one round trip), and a second pub/sub topology to know about.

### Why the flex group keeps the cap (also beside `group.flex` in the code)

All times are on Valkey's clock: I is the interval, A is SendAlign, T is the TAT before the grant.

- **Allowed lateness and clock offset.** A worker places a slot with the upper bound of its offset to Valkey (clockMap). On Valkey's clock the slot is therefore never early, and at most that worker's uncertainty u late. The send then leaves within its tolerance, I − u. So a request of slot s leaves in [s+A, s+A+I), whichever worker sends it and whatever its offset, as long as the offset stays within the bounds clockMap keeps. The offset uncertainty fits inside the margin by construction; the verifier checked two workers with different offsets.
- **Earlier grants.** Their last slot is ≤ T−I, so every one of their requests, the latest included, has left before T+A.
- **The seam.** `Idle` is now − T, both read by the limiter, so no worker clock enters it. `Idle ≥ A+I` puts the group's first slot at T+A+I or later, and the group never leaves before its slots, so the gap at the seam is more than one interval. The unbroken grid only guarantees a gap above zero between neighbours, so the seam is sparser than a busy channel. A threshold of A+2I would buy a gap above 2I that nothing needs; the verifier agreed.
- **Within the group and after it.** The group's slots move together and stay I apart. Later grants only see a larger gap, and a later flex grant repeats the argument with this group as the earlier one.

## Verifier (fresh context; full report and handling in `.agents/reviews/2026-11-15-wake-hint.md`)

Verdict on 4fbc9ba: **fail**, because one proof was red (R9 low alone 91.6/s against 93). Findings and handling:

| # | Finding | Handling |
|---|---|---|
| 1 | Hole at the flex seam (High) | **Fixed.** Only a group that ended the backlog moves earlier. The new test fails without the fix (a 170 ms gap). R9 after the fix: low alone 94.98/s, all 3 subtests pass. |
| 2 | A missing key reported Idle = 1 min, which would break the proof at rates under 1/s (Medium) | **Fixed.** `keyLinger` = 1 s sets both the key's lifetime and the Idle reported for a missing key. The integration test asserts exactly 1 s. |
| 3 | deferClaim fires on a busy grid (Low) | **Fixed.** Not after a contended wait (`selector.raced`), nor after a safety poll that found rows. |
| 4 | pause() overwrites a hint during a failed claim round (Low) | **Kept.** It costs at most IdleMax (250 ms), and only after a failed claim. |
| 5 | Listener resubscribe loop (Low) | **Fixed**, with an integration test covering both cases. |
| 6 | Missing tests | **Added.** Hints after reschedule, release and reap; flex under late limiter replies, with a burst right behind the early group. |
| 7 | Process: the test-writer was blocked by agentguard (owner item 0002), so the coordinator placed its draft; its bounds are 2× the owner's targets | **Disclosed.** The owner's targets are met on compose (scenario 1). |
| — | The Grant.Idle doc understated Idle by one interval | **Fixed.** |

The verifier's attack list held: the hint racing its commit, a missed hint, a Valkey reconnect, a scheduled row due earlier than the current sleep, goroutine leaks (goleak under -race), the rate cap under a burst at the default config, and both points of the flex proof.

## Gate (on the code and tests of 873df76)

The machine was saturated throughout by other sessions' stacks and tests. Host CPU stayed at 100% for 20+ minutes; a stray `find.exe` started at 15:53 had used about 7.4 CPU-hours.

| Check | Result | Duration | Notes |
|---|---|---|---|
| `task check` (build, vet, lint, unit tests, govulncheck) | **pass** | 398 s | 486 tests; no vulnerabilities |
| `task test:integration` | **fail (5)** | 413 s | See the breakdown below. |
| `TestDeliveryR7FirstSendAfterIdleIsPrompt`, `-cpu 2` with GOMAXPROCS=2 | **pass** | 131 s | p50 23.4 ms, p95 110.7 ms, p99 132 ms |
| R9 Priority (after the fixes) | **pass** | 63 s | low alone 94.98/s |
| R8 RateLimit (after the fixes) | **not shown green** | — | See the paired runs below. |
| `go test -race ./internal/delivery/` | **pass** | 31 s | |
| The two new flex tests, `-race -count=5` | **pass** | 17 s | |

The 5 integration failures:

- **3 infrastructure.** MySQL testcontainers did not become ready under load (`TestMetricsQueriesUseTheirIndexes`; in the first gate run also `TestQueueNextDue`, `TestIntakeWakes…`, `TestWorkersHoldTheRateAtTheProvider`).
- **1 known flake.** `TestLimiterReservesWithinTheHorizon` assumes two Takes within 10 ms; it predates this change.
- **1 compared against base.** `TestWorkersAcrossSmallLimiterClockSteps` hit 102 against a bound of 101. Run against base *concurrently* (identical load, 3 pairs × 2 counts), both peaked at 101-102 with no failure on either, so it is load, not this change.

**R8 throughput, paired with base** (average rate over the 3 channels):

| Series | Base | Branch |
|---|---|---|
| Sequential, host CPU < 35% at start | 23.0 (load spike) / 90.1 / 90.8 | 79.0 (a ~1 s tail gap) / 92.3 / 82.9 |
| Concurrent | did not drain in 45 s (217 of 2100 sent) | did not drain in 45 s (1585 sent) |

The data cannot tell a tail regression from load. One candidate, untested: under load, a hint publish over its 100 ms timeout starts the publisher's 1 s hold-off, so other workers fall back to the 1 s safety poll, where base polled within 250 ms.

## Known follow-ups

- **R8 and compose on a quiet machine.** Re-run R8 against base, and the five compose scenarios on the final commit. This is what blocks a merge.
- **Sporadic traffic.** Rows less than about 210 ms apart on a channel still pay `SendAlign`, because the previous group's unused reservation keeps the grid busy. Release unused slots to the limiter.
- **Worker-side publisher.** Consider a worker publisher without the API's hold-off, or with a longer timeout, if R8 shows the tail.
- **Flaky limiter test.** `TestLimiterReservesWithinTheHorizon` (10 ms assumption).
- **Process slip.** One `git reset -q` (index only, nothing lost) undid a partial staging; the brief forbids `reset`.

Branch: `claude/feat-worker-wake-hint-ad3352`. Code and tests final at 873df76; the commit after it only fixes this file.
