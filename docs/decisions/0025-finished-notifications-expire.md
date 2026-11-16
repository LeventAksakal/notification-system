---
status: accepted
date: 2026-11-15
decision-makers: owner (in chat, 15 Nov 2026)
revisit: when a consumer needs history longer than the retention (then archive instead of delete)
---
# Finished notifications and their attempts are deleted after APP_NOTIFICATION_RETENTION (0025)

## Rule
Every `app serve` process deletes notifications that finished (sent, failed, cancelled) more than `APP_NOTIFICATION_RETENTION` ago (default 7 days), their delivery attempts first, then batches with no notification left; nothing in flight and no template is ever touched.

## Question
How long are notifications and their attempts kept, and what removes them? Nothing deleted them, so both tables grew with every request; only Idempotency-Keys expired (24 h, `internal/intake/sweep.go`).

## Context
Owner, accepting decision 0005 on 15 Nov 2026: add retention for finished notifications. Constraints: a failed notification can be replayed only while it exists; `/metrics` looks back at most one hour (`MaxMetricsWindow`); an Idempotency-Key names a notification or batch for 24 h, and a keyed retry must find it; several API instances share one database (decision 0018).

## Options
1. Delete by `finished_at` in bounded steps from the API processes, beside the idempotency sweep. One setting; the database clock decides; no new process. Rows are gone for good (no archive).
2. Partition `notifications` and `delivery_attempts` by `finished_at` and drop old partitions. Cheapest delete at large scale; MySQL partitioning forbids the foreign keys the schema uses and every unique key must include the partition column, so the schema would lose its integrity rules.
3. `ON DELETE CASCADE` from notifications to attempts, deleting notifications only. One statement fewer; cascades run row by row inside InnoDB, invisible to `EXPLAIN` and to the statement's own row count, so a step's cost cannot be bounded or measured.
4. Archive to cold storage (a table or object store), then delete. Keeps history; a second store to run, nothing asks for it yet.

## Decision
Option 1. Partitioning would cost the foreign keys and CHECKs decision 0005 relies on; cascade hides the cost the targets measure; an archive is work without a consumer.

- Default 7 days: a failure on a Friday is still replayable on Monday; the metrics window needs one hour. Bounds: 1 hour (the metrics window) to 365 days, and never below `APP_IDEMPOTENCY_RETENTION` (`config.Load` refuses it).
- A step is one READ COMMITTED transaction: `SELECT id ... WHERE finished_at < NOW(3) - retention ... ORDER BY finished_at LIMIT 100 FOR UPDATE SKIP LOCKED` through `finished_idx`, then `DELETE` the attempts of those ids (through an index leading with `notification_id`), then the notifications. After each full step the sweeper pauses as long as the step took, so it keeps the database busy at most half the time; steps repeat until one finds fewer than 100. Then batches older than the retention with no notification left go, 1,000 per statement, through the new `batches_created_idx`.
- 100, not 500: the first measurement with 500 rows per step took 68 to 89 ms per `DELETE` on average (max 476 ms) on the Docker Desktop harness, and lifted the claim p99 from about 150 ms to 513 ms while sweeping.
- Attempts of every outcome go with their notification. A finished notification has no `in_flight` attempt left (decision 0005, "Attempts written before the call"), so no in-flight call loses its row.
- Safe with replay: the step locks its rows, so a replay racing it waits and then finds nothing (404), and a replay holding a row makes the step skip it. Safe with workers: workers lock only pending or processing rows, whose `finished_at` is NULL. Safe with several sweepers: `SKIP LOCKED` hands each row to one of them.
- Runs on the `APP_IDEMPOTENCY_SWEEP_INTERVAL` tick (default 1 minute), so no second interval setting.
- A batch whose items expire at different times is listed with fewer items than its `total` until the last one goes.

## Confirmation
`internal/store/retention_integration_test.go`: expired rows and their attempts go, in-flight, scheduled and recent rows and templates stay, a batch goes with its last item, the three statements read through their indexes (`EXPLAIN`), and two sweepers at once delete 3,000 rows exactly once. `internal/store/sql_integration_test.go` sends the same statements as raw SQL: index use, the delete, the foreign key refusing notifications before their attempts, and a replay's lock skipped. An acceptance test citing a requirement row (proposed O1) is for the test-writer; it could not run in this worktree (see the pull request). Timings (`TestMeasureRetentionUnderLoad -measure`, 100,000 expired rows beside 150,000 pending, 8 claiming workers throughout, 15 Nov 2026, 100-row steps): the sweep took 2 min 9 s. Per statement, avg / p99 / max: candidate `SELECT` 2.5 / 10.5 / 53 ms, attempts `DELETE` 9.9 / 31.6 / 74 ms, notifications `DELETE` 15.1 / 60.3 / 216 ms. The 100 ms target holds at p99 and is missed by one notifications `DELETE` in 1,000 (216 ms, on the loaded harness). Claim p99 was 150 ms during the sweep against 126 and 93 ms before and after.
