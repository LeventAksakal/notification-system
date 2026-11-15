---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when one MySQL primary can no longer hold the write load
---
# Built for many instances on one database: claims are leases with a fencing token (0018)

## Question
The brief asks for millions of notifications a day with reliable delivery: how do many workers share the work safely when any of them can crash, pause or restart while holding messages?

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

## Owner review (15 Nov 2026)
- Accepted. One primary is enough: the provider ceiling is 3 channels × 95/s = 285/s (24.6 M a day), at about 5 writes per notification, about 1,400 writes/s. The rate limit binds before the database does.
- Every dial (lease, reap interval, the lease cut, worker concurrency, pool size, and every other runtime setting) gets a derivation: Little's law for concurrency, measured write times for the pool and the cut, lease ≥ provider timeout + horizon + margin. `config.Load` refuses impossible combinations, such as a lease shorter than the provider timeout.
- Next step at scale, written up and not built: daily partitions, so retention is `DROP PARTITION` instead of `DELETE`. The primary key would become `(id, created_at)`.
- Built in session dials-derived (branch `feat-derived-dials`), merged with the rest.
