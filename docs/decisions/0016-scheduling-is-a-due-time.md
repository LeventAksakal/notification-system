---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when recurring schedules are asked for
---
# Scheduling is a future due time on the queue row; no scheduler process (0016)

## Rule
Every notification has `due_at`. The claim query takes only rows with `due_at <= NOW(3)`, using the database clock. Nothing moves a scheduled notification into the queue: it is already there.

## Context
Bonus item: "Scheduled Notifications: allow scheduling notifications for future delivery". Retries (0007) already need "not before time T" on a row.

## Options
1. One mechanism for both: `scheduled_at` in the request becomes `due_at`; an immediate notification gets `due_at = now`; a retry gets `due_at = now + backoff`. No extra process, no leader, and cancel works the same until the row is claimed. The database clock is the only clock, so workers with skewed clocks agree.
2. A scheduler loop that promotes due notifications from a `scheduled` state to `pending`. An explicit stored state; a second moving part that must run exactly once across instances, or be made idempotent.
3. Delayed messages in a broker. Native where a broker exists; we have none (0003).

## Decision
Pending. Recommended: option 1. A `scheduled_at` in the past is treated as now; one further than a configured horizon (proposed 30 days) is refused.

## Confirmation
Acceptance test with an injected clock at the database boundary: a notification scheduled for T is not at the provider double before T, is there within 2 seconds after T, and can be cancelled before T.
