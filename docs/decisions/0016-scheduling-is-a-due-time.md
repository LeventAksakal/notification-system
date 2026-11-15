---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when recurring schedules are asked for
---
# Scheduling is a future due time on the queue row; no scheduler process (0016)

## Question
How does a notification wait until its scheduled time, and what moves it to delivery when the time comes?

## Rule
Every notification has `due_at`. The claim query takes only rows with `due_at <= NOW(3)`, using the database clock. Nothing moves a scheduled notification into the queue: it is already there.

## Context
Bonus item: "Scheduled Notifications: allow scheduling notifications for future delivery". Retries (0007) already need "not before time T" on a row.

## Options
1. One mechanism for both: `scheduled_at` in the request becomes `due_at`; an immediate notification gets `due_at = now`; a retry gets `due_at = now + backoff`. No extra process, no leader, and cancel works the same until the row is claimed. The database clock is the only clock, so workers with skewed clocks agree.
2. A scheduler loop that promotes due notifications from a `scheduled` state to `pending`. An explicit stored state; a second moving part that must run exactly once across instances, or be made idempotent.
3. Delayed messages in a broker. Native where a broker exists; we have none (0003).

## Decision
Option 1. A `scheduled_at` in the past is treated as now; one further than a configured horizon (proposed 30 days) is refused.

## Confirmation
Acceptance test with an injected clock at the database boundary: a notification scheduled for T is not at the provider double before T, is there within 2 seconds after T, and can be cancelled before T.

## Numbers
| Number | Value | Where it comes from |
|---|---|---|
| Scheduling horizon | 30 days (`notification.ScheduleHorizon`) | Arbitrary. It bounds how long a row sits in the queue table and how long an idempotent replay must still make sense. Tune it from how far ahead real clients schedule; it is a product number, not a performance one |
| Delivery after the due time, in the acceptance test | within 2 s | Arbitrary test slack, about 8 times the worst idle poll (250 ms). Tighten it with the measured claim latency |
| Precision | milliseconds (`DATETIME(3)`, input truncated) | The column's precision. Pacing is about 10 ms per send, so finer scheduling would mean nothing |

## Owner review (15 Nov 2026)
- A burst scheduled for one moment drains at the channel's rate: 100,000 messages at 09:00 take about 17.5 min at 95/s. That is how the system works, not a defect. "Scheduled for T" means "eligible from T".
- Rescheduling: `PATCH /notifications/{id}` with a new `scheduledAt`. Allowed while the notification is pending and has never been tried, as one conditional UPDATE. Not asked for by the brief, but it is how this would be built for real.
- One clock: the horizon check moves to the database clock, the same one `due_at` uses.
- Recurring schedules are backlog; the brief does not ask for them.
- Both changes are built in session reschedule (branch `feat-reschedule-single-clock`, on top of the code-first API branch), merged with the rest.
