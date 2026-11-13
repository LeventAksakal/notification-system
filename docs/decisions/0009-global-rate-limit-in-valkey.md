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

## Confirmation
Acceptance test with 3 workers and a backlog on every channel: the provider double's arrival log shows at most 100 per channel in every sliding one-second window, and at least 95 per channel per second on average (the limit is used, not just respected). The load run reports both numbers.
