---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when one MySQL primary can no longer hold the write load
---
# Built for many instances on one database: claims are leases with a fencing token (0018)

## Rule
A claim sets `lease_until` and increments `claim_version`. Every later write by that worker carries `WHERE claim_version = ?`; zero rows affected means the lease was lost and the result is discarded.

## Context
The brief: "millions of notifications daily", "high throughput", "reliable delivery". Workers crash, get paused and get restarted while holding work.

## Options
1. Assume one worker. No leases; a crash strands its rows in `processing` for ever, and a second worker is unsafe.
2. Many stateless instances on one MySQL. A claim is a lease: a reaper returns rows whose lease expired to `pending`; the version check stops a worker that was paused past its lease from overwriting the new owner's result. Shared limits live in Valkey (0009). No lock service: the row being worked on is the lock.
3. Option 2 plus a shard resolver and date-partitioned tables now. Ready for data beyond one node; code paths that nothing in this repository can exercise or test.

## Decision
Pending. Recommended: option 2, with option 3 written up in the README as the next step (shard by recipient hash, partition by day, drop old partitions). A lease that expires during a provider call can produce a second send; 0006 covers that case with the idempotency key.

## Confirmation
Fault tests: kill a worker mid-send, and pause a worker container past its lease then resume it. Assert every notification ends `sent`, none stays `processing`, and the paused worker's late write changed nothing.
