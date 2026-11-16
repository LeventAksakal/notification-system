---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when a provider publishes its real limit and burst rules
---
# The per-channel limit is global, enforced in Valkey before rows are claimed (0009)

## Question
How do several workers together keep each channel at or under 100 messages per second, when no single worker sees what the others send?

## Rule
A worker asks `notification.Limiter` for send slots and claims at most that many rows. The limiter's state lives in Valkey and its clock is the Valkey server's.

## Context
The brief: "maximum 100 messages per second per channel". With several workers (0002) the limit must hold across processes. "Per second" needs a definition we can test: no more than 100 sends per channel in any one-second window.

## Options
1. `x/time/rate` in each worker at 100 divided by the replica count. No dependency; wrong as soon as a replica stops or is added, and idle workers waste their share.
2. Token bucket in Valkey with capacity C. Simple and common; allows about C + 100 in one second, so it breaks the definition unless C is 1.
3. GCRA in Valkey with burst 1: one send per 10 ms per channel. O(1), one key per channel, perfectly smooth; one Valkey call per message (100 per second per channel).
4. Sliding-window log in Valkey (sorted set of send times). Exact; lets a worker take up to 100 permits in one call, so sends arrive at the provider in bursts; about 100 entries per channel.

## Decision
Option 3. GCRA with burst 1 is a token bucket of capacity one that refills every interval, written as one stored timestamp (the next free slot) instead of a token count and a refill time. It satisfies the definition by construction and is the kindest traffic shape for a provider. Permits are taken before claiming so a claimed row never waits on the limiter while holding a lease. If Valkey is unreachable the workers stop sending (fail closed) and say so in their logs and metrics (readiness checks MySQL only, decision 0013): exceeding a provider's limit gets the whole account throttled, which is worse than a pause. The limit is a dial per channel, default 100.

Where the limit is measured matters (found 13 Nov while building the provider double): the provider counts by arrival time, and network jitter can squeeze 101 sends that left 10 ms apart into one second. A limiter set to exactly 100 will therefore be seen at 101 now and then. Options for the owner: run the limiter slightly under the limit (for example 98 per second), or state the limit as "sent" and assert it at the sender. The limit counts every request to the provider, failed ones included.

## Confirmation
Acceptance test with 3 workers and a backlog on every channel: the provider double's request log (every request, whatever its outcome) shows at most 100 per channel in every sliding one-second window, everywhere. Throughput floors (the limit is used, not just respected) are asserted only where the machine keeps time: 88 per channel per second over the whole drain and 91 in steady state at the 95-per-second pacing (owner item 0010); CI's two-core runner asserts the cap and a sanity floor of 40. The load run reports both numbers. (Reworded 15 Nov 2026: the first text asked for at least 95 on average, which became the ceiling once the pacing moved to 95.)

## As built (14 Nov 2026)
The limiter reserves up to a horizon (200 ms) of consecutive send slots per call instead of one permit per message, and paces 5% under the limit (one slot per 10.53 ms, 95 per second): the answer to the arrival-jitter question above. It was 3% (97 per second) until 14 Nov 2026; a two-core machine then showed the provider 99 to 100 in a second and CI 101 once, and the owner chose 5% (owner item 0013). A grant carries Valkey's clock; each worker learns the offset to its own clock from every call and places slots through it, so a late reply cannot send a group late (before this, 60 ms of reply jitter showed the provider 102 to 103 in a second; `TestGapDeliveryR8RateUnderLimiterReplyJitter`). While the offset is unknown or looser than one interval a worker reads the clock with a call that reserves nothing and sends nothing. When a reading contradicts the earlier ones by a forward step of one interval or more (a clock stepped; smaller corrections only restart the mapping), the worker releases every send it had placed before, because the limiter may have restarted its schedule on top of them (one run showed 106 in a second before this; owner item 0013). A slot whose claim takes longer than 200 ms or whose timer wakes more than one interval late is given up, never sent late, so throughput falls on a starved machine or a slow database while the cap holds (measured: `docs/REQUIREMENTS.md` row R8; owner items 0011 and 0013). The acceptance test asserts the cap everywhere and throughput floors two under what they were at 97 per second: 88 (whole drain) and 91 (steady state), not the paced rate itself (owner item 0010). The rule above still says "at least 95 per channel per second on average"; with the pacing at 95 that figure is the ceiling, not a floor, reworded on acceptance, 15 Nov 2026.

## Numbers (15 Nov 2026)
| Number | Value | Where it comes from |
|---|---|---|
| Limit | 100 per second per channel | The brief; "per second" read as any sliding one-second window, the strictest testable reading |
| Headroom | 5% (95 per second) | Measured: at 3% a two-core machine showed the provider 99 to 100 in a second and CI 101 once; owner's choice (owner item 0013) |
| Interval | 10.53 ms | Derived: one second divided by 95 |
| Horizon | 200 ms, or the send alignment when that is larger | A budget of limiter calls: a busy channel calls every half horizon, 10 per second per channel per worker; it grows to the alignment so one reservation outlasts one claim (as built, 16 Nov) |
| Send alignment | p95 of the newest 20 claims, 200 ms to lease/8 | Measured per worker and channel; floor and ceiling derived in the as-built note below (16 Nov) |
| Allowed lateness | one interval | Derived: keeps every pair of sends at least one interval apart; enforced at the request's first write, not only before the call (as built, 16 Nov) |
| Throughput floors | 88 and 91 | Measured runs minus a margin (owner item 0010) |
| CI floor | 40 | Sanity floor on the two-core runner, where throughput follows CPU (owner item 0011) |

## Follow-up (15 Nov 2026)
The owner called the throughput drop under a slow database (56 to 75 per second instead of 95, owner item 0011) a real concern. Proposed as the clear improvement: replace the fixed 200 ms send alignment with the measured claim latency (a high percentile over a recent window, bounded), so a slot waits as long as claims are really taking instead of being given up when they take longer than a guessed constant; derive the horizon from a stated limiter-call budget. To start once the wake-hint and provider-health branches, which edit the same loop, have landed.

## As built: the send alignment follows the claim time (16 Nov 2026)
The fixed 200 ms alignment ("a slot whose claim takes longer than 200 ms ... is given up" above) is replaced (owner item 0011). A worker passes a lead to the limiter with each reservation: the p95 of its newest 20 claim times on that channel, never below 200 ms (the old fixed value) and never above an eighth of the lease (3.75 s by default). A free grid restarts at now plus that lead, and a busy grid continues where it was. Slots are still the send moments on one shared grid, so the spacing does not depend on any worker's lead. One reservation now covers the larger of `APP_RATE_LIMIT_HORIZON` and the lead, and a group that found fewer rows than slots gives the rest back to the limiter.

Measured on the owner-item-0011 scenario (every claim held 1.6 s): the fast channels drained at 95.0 per second, busiest second 96; with the fixed alignment they managed 1.9 and 5.2. R8 drained at 93.3 to 94.6 per second against 89.2 to 91.9 before. R9 latency was unchanged. The design, the derived numbers and every run are in `.agents/pr/adaptive-align.md` on the branch `claude/feat-adaptive-send-alignment-a40f6f`. Claims slower than the ceiling still lose slots: the database is then several seconds behind on every query.

## As built: lateness checked at the first write (16 Nov 2026)

The question: a send passed the lateness check and still left late (clock and health checks, goroutine scheduling, the transport's connection handoff came after it), so on a loaded machine late sends bunched with on-time ones. `TestDeliveryR8RateLimit`, 20 runs pinned to two cores: busiest one-second window up to 102 before 16 Nov's provider changes and up to 106 after, against the cap of 100.

How it runs now: the slot's late moment travels with the provider call (`provider.WithLeaveBy`). The client refuses a call already past that moment before it takes a connection, and refuses the request's first write when that moment plus the request's own wait for a connection (about zero on a pooled one, the dial on a fresh one) has passed: nothing is written, the call is `ErrNotSent`/`ErrTooLate`, and the worker releases the job as a missed slot (no attempt, no budget, no health report; decision 0005). Only the connection wait is excused: Refusing every late fresh connection would starve a provider whose dial is slower than one interval (a remote provider: nothing sent, nothing recorded), and excusing every fresh connection let late requests leave in a burst after a stall had closed the pooled ones (one run at 103).

What it costs: a request can be late by at most its own wait for a connection; on a starved machine more slots are given up, so throughput drops (86.9 to 95.05 per second over 20 runs). Measured after: 20 of 20 runs at or under 100, busiest windows 96 to 100.
