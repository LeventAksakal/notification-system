# GET /v1/metrics from per-minute rollups (decision 0014)

Branch `claude/feat-metrics-rollups-2d1ba2`, rebased onto `integration/decision-review` 4a0be06 (16 Nov 2026); built and measured on db-rules-retention's dae271a (see Base).

## What changes

The delivery half of `/v1/metrics` is now read from two per-minute rollup tables instead of every row of the window: sent, failed, attempts, failed and unknown attempts, and latency p50/p95/p99, per channel and in all. Its cost no longer grows with the traffic in the window. The JSON shape is unchanged: no spec change.

- `schema/delivery_rollups.sql` adds two tables, each sharded 4 ways while its minute is written:
  - `attempts_per_minute (minute, channel, outcome, shard) -> n`;
  - `finishes_per_minute (minute, channel, status, latency_le_ms, shard) -> n`.
- `schema/notifications.sql` adds four AFTER triggers, on insert and update of `notifications` and of `delivery_attempts`. In the transaction that changes a row, they take its old contribution out of the first shard of its key that holds one and put its new one in. A take-out that changed nothing runs once more, because a fold may have committed shard 0 behind it. A trigger rather than an upsert in the result write: four code paths write outcomes (the result, the reaper's unknown, a release, a late result), and only a trigger sees each change's old and new value.
- `internal/store/metrics.go` and `queries/metrics.sql`:
  - whole minutes are summed from the rollups;
  - the window's partial first minute is counted from raw rows on its shorter side (added after the start, or subtracted before it), so at most 30 s of raw rows;
  - everything runs in one read-only REPEATABLE READ snapshot.
- `internal/store/latency_buckets.go`: the bucket function and the percentile estimate.
- `internal/store/retention.go` and `queries/retention.sql`:
  - each pass folds the shards of minutes 10 to 2 minutes old into shard 0 (`FoldRollups`);
  - rows and rollup minutes are deleted before one minute bound.
- Indexes, each added after a measured read:
  - `claim_idx` gains `attempts`, so the scheduled count reads the index alone: 6.6 s → 10 ms at 1M rows;
  - `finished_idx` gains `created_at, scheduled_at`, so the partial-minute finishes read the index alone.
- `migrations/20261116015152_delivery_rollups.sql`, written by `task migrate:diff`.

## Design

### Why rollups are exact
A rollup row is a pre-computed view: summed over its shards, it equals a recount of the rows it describes. `TestRollupsEqualARecountOfTheirRows` checks this after every way an outcome is written:
- sent, retried, failed, replayed;
- reaped as unknown, then a late result after the lost lease (unknown → accepted);
- released after the call began;
- rows whose times move;
- 8 concurrent workers, then 3 workers mixing every outcome with a reaper and late results;
- a fold, and changes after the fold;
- retention, with rows kept on both sides of `now - keep`.

`TestMetricsWindowSplitsAtTheMinute` checks the window's edge at offsets 0, 1 ms, 29.999 s, 29.9995 s, 30 s, 30.001 s, 59.999 s and 60 s into the minute.

### The partial minute and reading your own writes
Minute keys truncate the same timestamps the raw queries read: `started_at` for attempts and `finished_at` for notifications.
- A window whose start falls less than 30 s into its minute takes that minute's rollups and subtracts the raw rows before the start.
- Otherwise it adds the raw rows from the start up to the next minute.

The current minute's rollup rows commit with the outcomes they count, so a client reads its own writes.

### Bucket derivation (latencySubBucketBits = 4)
Buckets are log-linear, as in HDR histograms. Every millisecond up to 16 ms has its own bucket. Above that, each power-of-two range splits into 8 equal buckets, so a bucket is at most 1/8 of its lower bound wide.

The percentile takes the nearest rank, finds its bucket, and interpolates linearly inside it, so the estimate is off by less than the bucket width: exact to 16 ms, at most 12.5% above that.

Why 4: at each latency the system adds more noise than a bucket is wide.
- Below 16 ms buckets are exact, so the 30 ms wake-hint target is read to 2 ms.
- From 128 to 512 ms, where a normal send lands (200 ms send alignment, 50 to 250 ms idle poll), buckets are 16 to 32 ms, under the 50 ms poll step.
- At the 5 s provider timeout they are 512 ms. Retry delays carry 100% jitter.

Tuning: one more bit halves the error and doubles the rows a busy minute writes. Tune against `latency_buckets_test.go` and the rollup row count.

Measured: p95 485 → 492 ms (+1.4%) and p99 200,302 → 200,570 ms (+0.13%) over an hour of 1M rows. `TestBucketPercentilesStayWithinTheBucketError` and the integration assertions hold every estimate inside the exact value's bucket.

### Hot rows, shards and the fold
A rollup row stays locked from the write that counts into it until that transaction commits, fsync included, so a row takes one finish at a time. Unsharded, every finish of a channel met on its accepted row.

Four shards come from the counted row's id: the attempt id mod 4, or CRC32 of the notification id mod 4. Shards multiplied an hour's rows by 2.75, so the retention pass folds each minute into shard 0 two minutes after it starts. The minutes still being written stay spread; the minutes a read sums are one row per key.

### Retention
Rollups are kept as long as the rows they count (`APP_NOTIFICATION_RETENTION`, 168 h by default), so a rollup minute always has its rows beside it for checking.
- Cost: about 161 rows a minute once folded, 1.6 million rows a week. That is under 1% of the notifications held over the same week.
- Rows and rollup minutes are deleted before the same minute bound, so a window of up to the retention's 1 h floor never starts in a half-deleted minute.
- Each sweep statement deletes at most 1,000 rollup rows on the primary key.

## Numbers
Measured 16 Nov 2026 on MySQL 8.4 in Docker Desktop, with a 128 MB buffer pool, on a host shared with other sessions. The bench holds 1,000,000 notifications over one hour: 95% sent, 3% failed, 2% pending; 97% of latencies 205 to 510 ms, a 3% tail up to 300 s. Times are medians of 5 runs.

### Read (`TestMeasureMetricsAtScale`)

| Read | Before (raw rows) | After (rollups) | Target |
|---|---|---|---|
| Delivery, 1 h window (start 25 s into its minute) | 15.4 s | 45 ms | well under 50 ms |
| Delivery, 1 h window, by start offset 0 / 15 / 29.999 / 30 / 45 s | — | 21 / 37 / 46 / 45 / 39 ms | well under 50 ms |
| Delivery, 60 s window | 169 ms | 33 ms | no slower than 0.15-0.18 s |
| Counts (sent, failed, attempts, failed and unknown attempts) | exact | identical | exact |
| Scheduled count (claim_idx with attempts) | 6.6 s | 10 ms | — |
| Queue depth (live, claim_idx covering) | 62 ms | unchanged | live read |
| Status totals (scan of every kept row) | 275 ms | unchanged | owner item 0017 |
| Whole `Read`, 1 h window | about 22 s (sum of the parts above, not timed as one) | 338 ms | — |

The parts of the 1 h delivery read:
- rollups: attempts 2.2 ms, finishes 14.2 ms (9,694 folded rows an hour);
- raw partial minute: attempts 8.3 ms, finishes 15.9 ms.

The worst start offsets, just under and at 30 s, read 30 s of raw rows (about 8,300 attempts and 8,200 finishes), at 45 to 46 ms. Before the fold, sharded rows made the 1 h read 42 to 67 ms.

### Write cost and hot rows (`TestMeasureRollupWriteCost`, `TestWorkersHoldTheRateAtTheProvider`)

| Configuration | Flat out, 8 workers, one channel (finishes/s, triggers on vs off) | Row lock waits per 10 s flat out | Paced, 3 workers at 95/s per channel: waits per 20 s | Rate test: average per channel / busiest 1 s / later-claim p99 |
|---|---|---|---|---|
| Base dae271a (no triggers) | — | — | — | 93.3-95.1/s / 96 / 97-149 ms |
| Rollups unsharded | 128-130 vs 344-420 | 1,291-1,323 (64-66 s waited) | 2,793-3,244 (15-19 s) | 58-84/s / 82-96 / 535-886 ms |
| Attempts sharded, finishes not | 80-88 vs 221-240 (loaded host) | 805-875 (70-78 s) | 2,168-2,182 (21-22 s) | 68-84/s, one run failed / 96 |
| Both sharded (as built) | 256-276 vs 364-473 | 2,917-3,154 (39-40 s) | 663-945 (5.4-7.5 s) | 93.7-95.0/s / 96 / 55-102 ms |

There were no deadlocks in any round, and all 500 notifications per channel were sent in every rate-test run. The rate test has no give-up counter; the tests that count slots given up (`clock_test.go`) do not touch the database.

Target: the upsert costs at most a few percent of throughput (busiest second ≤ 100, steady state about 95/s).
- At the deployed rate this holds: 95/s steady state and a busiest second of 96, as on the base.
- Flat out, one channel's ceiling drops by about 35% (to 256-276 finishes/s, 2.7 times the deployed rate).
- The paced harness reaches only 74-88/s on this host even without triggers, so the paced rows compare lock waits, not rates.

## Spec changes for the merge
None. The JSON shape and `api/` are untouched.

## Deploy notes
- The migration drops and re-adds `claim_idx` and `finished_idx` on `notifications`: an index rebuild on a large table.
- No backfill. For an hour after the migration, a window reaching back before it counts only what happened since, and can count below zero on the subtraction side of its first minute (0014 "As built").
- The fold runs in the retention pass (`APP_IDEMPOTENCY_SWEEP_INTERVAL`, 1 m by default). A sweep interval over 8 minutes leaves some minutes sharded: still exact, slower to read.

## Verifier
Report: `.agents/reviews/2026-11-15-metrics-rollup.md`. Round 1 returned FAIL with five findings:
1. Retention deleted a rollup minute before its rows (negative counts): fixed.
2. No backfill: documented.
3. Unsharded finishes become a hot row: fixed.
4. Replay is not retried on a lock conflict: left as is.
5. Stale docs: fixed.

Round 2 returned PASS. Its missing-test finding (the fold never ran while workers wrote) led to a new test, and that test found a real lost count, now fixed:
- a take-out that waited on a fold resumed past the new shard 0 row;
- the fix runs a take-out once more when it changed nothing.

Its other round 2 items:
- clock skew at the retention edge: documented in 0014;
- replay against the fold: left as is;
- the retention plan: a range scan at 1M rows.

## Not done, owner items
- Owner item 0017: status totals scan every kept row (275 ms at 1M rows, about a minute at 180M). Taken as is.
- Replay's rollup decrement is not wrapped in `retryLockConflict` (`internal/intake`, outside this lane).

## Base
Built on db-rules-retention's tip dae271a, the base the coordinator named first. Under the merge queue (`.agents/merge-queue.md`) this lane does not rebase on its own: it reports READY, and on `REBASE ONTO <integration tip>` it runs `git rebase --onto <tip> dae271a`, then regenerates its migration (`task migrate:diff -- delivery_rollups`) and the generated code (`task gen`) on the rebased schema. A trial rebase onto d2c7a67 had conflicts only in docs.

## Checks (scoped, per the merge queue; the coordinator runs the full gate)
Run once on the final change set, 16 Nov 2026, `GOMAXPROCS=2` to imitate the runner.

| Check | Result | Duration |
|---|---|---|
| `go build ./...` | pass | 11 s |
| `go vet -tags integration ./internal/store/... ./internal/intake/... ./migrations/...` | clean | 4 s |
| `go test -race ./internal/store/...` (unit) | pass | 16 s |
| `go test -tags integration ./internal/store/` (whole package; `-measure` tests skip) | pass | 275 s |
| `TestWorkersHoldTheRateAtTheProvider` | pass: 95.0/s on every channel, busiest 1 s 96 | 39 s |
| R13 acceptance (`-tags integration ./test/acceptance/ -run R13`, 3 tests) | pass | 45 s |
| golangci-lint, store and migrations (integration tag) | clean for this lane's files after fixes; 20 findings remain in db-rules' `rules_integration_test.go` (G202) and `sql_integration_test.go` (rowserrcheck, sqlclosecheck), present at dae271a and fixed by db-rules' e2b655c | 63 s |
| Rollup tests rerun after the lint fixes (recount, window split, bucket expression) | pass | 90 s |

After the rebase onto 4a0be06: migration and generated code regenerated (`migrations/20261116015152_delivery_rollups.sql`, `task migrate:check` clean), owner item renumbered 0017 (0016 is the WebSocket feed's). Checks, `GOMAXPROCS=2`:

| Check | Result | Duration |
|---|---|---|
| `go build ./...` | pass | 6 s |
| golangci-lint, `./internal/store/... ./migrations/...` (integration tag) | clean | 47 s |
| `go test -race ./internal/store/... ./internal/httpapi/...` | pass | 45 s |
| `go test -tags integration ./internal/store/` | pass | 150 s |
| Three-worker rollup exactness (`TestRollupsEqualARecountOfTheirRows`, folds under load included) | pass | 42 s |
| `TestWorkersHoldTheRateAtTheProvider` | pass: 95.0 / 95.0 / 93.3 per s, busiest 1 s 96 | 42 s |
| `go test -tags integration ./internal/httpapi/` | pass | 39 s |
| R13 acceptance | not run: `test/acceptance` does not compile on integration (`delivery_wake_test.go:42` `n.ID`, known in the merge queue) | — |

Not run here: `task check` as a whole, `task test:integration` across all packages, `govulncheck`, k6 and fault tests.
