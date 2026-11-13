---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when more than a handful of worker replicas run
---
# A per-channel circuit breaker pauses claiming, so an outage burns no attempts (0008)

## Rule
While a channel's breaker is open, workers do not claim rows for that channel. Only a real provider response or timeout counts as an attempt.

## Context
Bonus item "Failure Handling". With retries alone (0007), a provider outage of ten minutes pushes every in-flight notification through all its attempts and into `failed`, although nothing was wrong with any of them.

## Options
1. No breaker; backoff only. Nothing to build; an outage longer than the retry budget dead-letters healthy messages, and workers keep hammering a provider that is down.
2. A breaker per channel inside each worker process. Opens on a failure ratio over a short window, lets one probe through after a cool-down (half-open), closes on success. Each replica learns of the outage separately, which costs a few failed sends per replica.
3. Breaker state shared in Valkey. All workers stop and resume together and exactly one probes; more Lua and one more thing that fails when Valkey does.

## Decision
Pending. Recommended: option 2, with option 3 named in the README as the next step. The unit is the channel because each channel has its own provider URL in configuration (all three default to the same provider double), so one channel's outage does not stop the others.

## Confirmation
Fault test: the provider double returns 503 for one channel for a fixed period. Assert: sends to that channel stop within the window, the other channels keep their rate, no notification on the failing channel reaches `failed`, and all are `sent` after recovery.
