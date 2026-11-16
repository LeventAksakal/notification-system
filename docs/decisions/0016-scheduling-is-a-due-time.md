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
Option 1. A `scheduled_at` in the past is treated as now; one further than a fixed horizon (`notification.ScheduleHorizon`, 30 days) is refused.

## Confirmation
Acceptance test with an injected clock at the database boundary: a notification scheduled for T is not at the provider double before T, is there within 2 seconds after T, and can be cancelled before T.

## Numbers
| Number | Value | Where it comes from |
|---|---|---|
| Scheduling horizon | 30 days (`notification.ScheduleHorizon`) | Arbitrary. It bounds how long a row sits in the queue table and how long an idempotent replay must still make sense. Tune it from how far ahead real clients schedule; it is a product number, not a performance one |
| Delivery after the due time, in the acceptance test | within 2 s | Arbitrary test slack: twice the worst idle wait (`wake.SafetyPoll`, 1 s, when a wake hint is lost; 0003). Tighten it with the measured claim latency |
| Precision | milliseconds (`DATETIME(3)`, input truncated) | The column's precision. Pacing is about 10 ms per send, so finer scheduling would mean nothing |
| Horizon in the database | `INTERVAL 30 DAY` in the `scheduled_within_horizon` CHECK | Repeats `notification.ScheduleHorizon`; `TestTheDatabaseRefusesASchedulePastTheHorizon` fails if they differ |
| Database clock reads per request | 0 without a schedule, 1 with one (single or batch) | the owner's limit: at most one extra round trip per request, never one per item |
| Race test rounds | 100, start staggered 0 to 9 × 300 µs | arbitrary: enough rounds that both orders occur on a laptop (14 claim wins, 86 reschedule wins measured); the step is about one claim round trip here, so rounds land on both sides of the race |
| Measurement samples | 2000 creates, 60 batches of 1000, 10 warm-up runs each | arbitrary: enough that p99 is not the single worst sample, few enough to run in minutes (`TestMeasureIntakeLatency -measure`) |

## Owner review (15 Nov 2026)
- A burst scheduled for one moment drains at the channel's rate: 100,000 messages at 09:00 take about 17.5 min at 95/s. That is how the system works, not a defect. "Scheduled for T" means "eligible from T".
- Rescheduling: `PATCH /notifications/{id}` with a new `scheduledAt`. Allowed while the notification is pending and has never been tried, as one conditional UPDATE. Not asked for by the brief, but it is how this would be built for real.
- One clock: the horizon check moves to the database clock, the same one `due_at` uses.
- Recurring schedules are backlog; the brief does not ask for them.
- Both changes are built; see "As built" below.

## As built (16 Nov 2026)
The two changes from the owner review above. Recurring schedules stay backlog (see `revisit`).

### Reschedule: `PATCH /v1/notifications/{id}`
- Body `{"scheduledAt": <RFC 3339 time | null>}`, the field required and nullable; null or a past time means due now, the same rules as create. Answers 200 with the notification as stored.
- Allowed while the row is `pending` with `claim_version = 0`: no worker has ever leased it, so no attempt, retry backoff (0007), provider `Retry-After` or outage release (0026) is waiting on it. That is the API's `scheduled`, plus a due row not yet claimed, minus one case: a row a worker handed back unsent during a declared outage (0026) shows as `scheduled` (attempts 0, due later) and is refused; the spec says so. Making `scheduled` also require `claim_version = 0` would change list and batch counts, so it is left to the owner. `attempts = 0` would not do: a breaker release and a reaped lease put a row back to pending with attempts still 0 and a worker's due time on it.
- One statement, `RescheduleNotification` in `internal/store/queries/intake.sql`: `UPDATE ... SET scheduled_at, due_at WHERE id = ? AND status = 'pending' AND claim_version = 0`. A claim racing it on the same row either leased it first (nothing matches) or leases after it and sees the new `due_at`. No row changed: 404 when the id is absent, else 409 `not_reschedulable` with the current status in the message. MySQL reports changed rows, so the same future schedule written again changes nothing; a row that is still unclaimed afterwards was unclaimed then too (`claim_version` never goes back to 0), so that counts as done.
- No WebSocket event: the stored status does not change, and `StatusEvent` carries neither the schedule nor the `scheduled` view.
- Tests: `internal/intake/reschedule_integration_test.go` (every allowed and refused state; 100 rounds of a reschedule racing `Queue.Claim` on one row, exactly one winner and no lost update, run with `-race`), `internal/httpapi/reschedule_integration_test.go` (over HTTP). Acceptance tests: `TestB21RescheduleMovesAScheduledNotification`, `TestB21RescheduleRefusals`, `TestB21RescheduleRacingAWorkerHasOneWinner`, `TestB22HorizonUsesTheDatabaseClock` (`test/acceptance/schedule_reschedule_test.go`).

### One clock
Every schedule decision now uses the database clock: `due_at` (the INSERT's `NOW(3)`), the claim, the `scheduled` status, and now the horizon, which was measured on the API process clock before.
- Service: a request that carries a schedule reads `NOW(3)` once (`DatabaseNow`), whatever the batch size, and `Validate` measures the horizon from it; a request without a schedule reads nothing. Chosen over deciding in the INSERT alone because a refusal must still name every broken field and, in a batch, the item's index, which a statement that fails as a whole cannot say.
- Database: the CHECK `scheduled_within_horizon`, `scheduled_at IS NULL OR scheduled_at <= updated_at + INTERVAL 30 DAY`. A CHECK cannot call `NOW()`, and the trigger that could would need `SIGNAL`, which sqldef 3.11.30 cannot parse (0023). `updated_at` is the statement's `NOW(3)` on every write that changes the row (the default on insert, `ON UPDATE`), so the CHECK is the horizon on the database clock at write time, and a row that passed once keeps passing while the clock does not step back. If it steps back by d, a write within d to a row scheduled within d of the horizon is refused (a cancel would answer 500); accepted as a clock fault, not handled. The service's read comes before the INSERT, so the CHECK never refuses what the service accepted unless the server clock steps back in between; if it does, the answer is still a 400 on `scheduledAt` (without an index in a batch). The reschedule relies on the CHECK alone: its body has one field, so no read is needed first.
- Deploying the CHECK: `ADD CONSTRAINT` checks every existing row and copies the table, blocking writes for the copy. A row accepted under the old process-clock check while the API host ran ahead of the database can lie up to that skew past `updated_at + 30 days` and fails the migration (error 3819). Before deploying on a live database, run `SELECT id FROM notifications WHERE scheduled_at > updated_at + INTERVAL 30 DAY;` and move any row it finds back inside the horizon. This take-home has no live data, so the migration runs as written.
- Spec: OpenAPI cannot state a time relative to the request, so `scheduledAt` on create and on the reschedule body says in prose that more than 30 days ahead on the server's database clock is refused (the number comes from `notification.ScheduleHorizon`).
- Tests: `TestTheScheduleHorizonIsMeasuredOnTheDatabaseClock` skews the process clock 40 days both ways and the answers do not move; `TestTheDatabaseRefusesASchedulePastTheHorizon` writes past the CHECK directly and asserts its interval equals `notification.ScheduleHorizon`.

### Cost of the one clock
Measured with `go test -tags integration ./internal/intake -run TestMeasureIntakeLatency -measure -v` (one client, sequential, one MySQL 8.4 container, no-op publisher), on the owner's laptop (Windows, Docker Desktop) while sibling sessions loaded it. "Before" ran the same harness in a worktree at the parent commit.

| p50 / p99 | before, run A | before, run B | after |
|---|---|---|---|
| create, no schedule | 21.4 / 149 ms | 29.7 / 227 ms | 27.6 / 169 ms |
| create, scheduled | 24.3 / 215 ms | 34.6 / 302 ms | 20.6 / 123 ms |
| batch of 1000, no schedule | 238 / 650 ms | 269 / 5276 ms | 292 / 765 ms |
| batch of 1000, scheduled | 262 / 807 ms | 366 / 1359 ms | 276 / 1109 ms |
| the clock read alone (`SELECT NOW(3)`) | | 2.1 / 20 ms | 2.1 / 25 ms |

The two "before" runs of the same code differ more (create p50 21 to 30 ms, batch p99 0.65 to 5.3 s) than the change can, so the before and after columns cannot resolve it. The cost is the one added round trip, measured alone: 2.1 ms at p50 and 20 to 25 ms at p99 on this machine, paid once by a request with a schedule (about 7 to 10% of a scheduled create's p50 here, under 1% of a 1000-item batch's) and not at all by one without. A request without a schedule runs the same statements as before. Deciding in the INSERT alone would save those 2 ms and lose the per-field, per-item refusal, so the read stays.
