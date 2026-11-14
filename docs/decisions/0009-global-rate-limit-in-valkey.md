---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when a provider publishes its real limit and burst rules
---
# The per-channel limit is global, enforced in Valkey before rows are claimed (0009)

## Rule
A worker asks `core.Limiter` for permits and claims at most that many rows. The limiter's state lives in Valkey and its clock is the Valkey server's.

## Context
The brief: "maximum 100 messages per second per channel". With several workers (0002) the limit must hold across processes. "Per second" needs a definition we can test: no more than 100 sends per channel in any one-second window.

## Options
1. `x/time/rate` in each worker at 100 divided by the replica count. No dependency; wrong as soon as a replica stops or is added, and idle workers waste their share.
2. Token bucket in Valkey with capacity C. Simple and common; allows about C + 100 in one second, so it breaks the definition unless C is 1.
3. GCRA in Valkey with burst 1: one send per 10 ms per channel. O(1), one key per channel, perfectly smooth; one Valkey call per message (100 per second per channel).
4. Sliding-window log in Valkey (sorted set of send times). Exact; lets a worker take up to 100 permits in one call, so sends arrive at the provider in bursts; about 100 entries per channel.

## Decision
Pending. Recommended: option 3. It satisfies the definition by construction and is the kindest traffic shape for a provider. Permits are taken before claiming so a claimed row never waits on the limiter while holding a lease. If Valkey is unreachable the workers stop sending (fail closed) and readiness reports it: exceeding a provider's limit gets the whole account throttled, which is worse than a pause. The limit is a dial per channel, default 100.

Where the limit is measured matters (found 13 Nov while building the provider double): the provider counts by arrival time, and network jitter can squeeze 101 sends that left 10 ms apart into one second. A limiter set to exactly 100 will therefore be seen at 101 now and then. Options for the owner: run the limiter slightly under the limit (for example 98 per second), or state the limit as "sent" and assert it at the sender. The limit counts every request to the provider, failed ones included.

## Confirmation
Acceptance test with 3 workers and a backlog on every channel: the provider double's request log (every request, whatever its outcome) shows at most 100 per channel in every sliding one-second window, and at least 95 per channel per second on average (the limit is used, not just respected). The load run reports both numbers.

## As built (14 Nov 2026)
The limiter reserves up to a horizon (200 ms) of consecutive send slots per call instead of one permit per message, and paces 3% under the limit (one slot per 10.31 ms, 97 per second): the answer taken to the arrival-jitter question above. A grant carries Valkey's clock; each worker learns the offset to its own clock from every call and places slots through it, so a late reply cannot send a group late (before this, 60 ms of reply jitter showed the provider 102 to 103 in a second; `TestGapDeliveryR8RateUnderLimiterReplyJitter`). While the offset is unknown or looser than one interval a worker reads the clock with a call that reserves nothing and sends nothing. When a reading contradicts the earlier ones (a clock stepped), the worker releases every send it had placed before, because the limiter may have restarted its schedule on top of them (one run showed 106 in a second before this; owner item 0013). A slot whose claim takes longer than 200 ms or whose timer wakes more than one interval late is given up, never sent late, so throughput falls on a starved machine or a slow database while the cap holds (measured: `docs/REQUIREMENTS.md` row R8; owner items 0011 and 0013). The acceptance test asserts the cap everywhere and throughput floors of 90 (whole drain) and 93 (steady state), not 95 (owner item 0010).
