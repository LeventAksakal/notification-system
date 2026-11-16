# Verifier report: metrics from per-minute rollups (decision 0014)

Fresh-context `verifier` subagent, 16 Nov 2026. Round 1 judged `git diff dae271a f32983c`, then the working tree as it moved (finishes briefly unsharded, migration 20261116001859). It edited nothing; the coordinator saved the report.

## Round 1: FAIL

Verdict as given: "the rollups agree with the triggers and every test passes, but at a supported config the read can return undercounted or negative numbers."

| # | Severity | Finding | Failure scenario | What was done |
|---|---|---|---|---|
| 1 | High | Retention deleted a rollup minute up to 60 s before the rows it counts (`Sweep*Rollups` by `minute < NOW(3) - keep`, rows by `finished_at < NOW(3) - keep`). | `APP_NOTIFICATION_RETENTION=1h`; the sweep at 13:00:10 deletes rollup minute 12:00; 100 sends finished 12:00:15, 50 at 12:00:30. A read at 13:00:20, `windowSeconds=3600`: since 12:00:20, m0 12:00, rollups from 12:00 are gone (0), minus the 100 raw rows in [12:00:10, 12:00:20): `sent = -100`, correct 50. About 1 in 8 hour-window reads at keep = 1 h. | Fixed in 0090e49: rows and rollup minutes are deleted before one bound, the start of the minute `now - keep` falls in (keeping the minute while its rows go would inflate the count instead). Test: `TestRollupsEqualARecountOfTheirRows` "after retention" keeps rows on both sides of `now - keep` in that minute. |
| 2 | Medium | No backfill: for an hour after deploy, windows that start before the migration miss earlier traffic, and on the subtract path can go negative; a pre-migration row changed later can take a count out of the migration minute. | Deploy at 10:00; a read at 10:20 with a 1 h window counts 20 minutes. | Documented as a deploy note in 0014 "As built". Not built: a backfill is a data migration, which `migrations/` (sqldef-generated) has no place for. |
| 3 | Medium | `finishes_per_minute` unsharded (the working-tree state at the time): `failed` is one bucket-0 row per channel and minute, so a provider refusing everything puts every finish on one row (the measured ~130/s cap). | Permanent 400s for a whole channel. | Confirmed by measurement before the report arrived (80 to 88 finishes/s flat out with finishes unsharded); both tables sharded again, and minutes folded after they are written (0090e49). |
| 4 | Low | Replay (`ReplayNotification`, autocommit) now decrements a rollup row and `retryLockConflict` does not wrap it; reapers on two instances can deadlock each other on attempt shard rows (retried, `queue.go:251`). | A replay meets a lock conflict and returns an error to the client. | Left as is: replays of old notifications touch past minutes, which no finish writes; listed in the PR notes. |
| 5 | Low | Docs out of date: 0014 shard sentence and `NUMBERS_TABLE`, the PR file's old migration names and `PLACEHOLDER`, `metrics.go:92` "one minute of raw rows", `retention.go:27` rows/min measured before sharding. | — | Fixed in 0090e49 and the docs commit. |

Found sound (round 1, verbatim in substance):
- Triggers: every write path takes out exactly the old contribution and adds the new one (claim with in_flight uncounted; FinishSent, FinishRetry, FinishFailed, FailSpent; Release, which deletes in_flight only; the reaper's unknown; a late result; replay; cancel; moving times). The trigger conditions cover every column the minute key and latency read; `id` never changes.
- Partial minute: the raw queries use the triggers' columns, DATETIME(3) values and [from, until) bounds.
- One read-only REPEATABLE READ snapshot; a client sees its own committed writes.
- Attempt shard `NEW.id % 4` is stable; finish transactions lock the finishes row, then the attempts row, always in that order.
- Go and SQL bucket expressions agree (-2 ms to 70 s and powers of two to 2^31); bucket 0, latency <= 0, BIGINT, nearest rank and interpolation correct.
- No state outside MySQL; the JSON contract and `test/acceptance/` are unchanged since dae271a; the migration matches `schema/`.

Checks the verifier ran (all passed):

| Check | Tree | Result | Duration |
|---|---|---|---|
| Bucket unit tests with `-race` | f32983c | pass | 3.5 s |
| Integration: index use, read counts, expression agreement, recount, window split | f32983c | pass | 155 s |
| R13 acceptance (3 tests) | f32983c | pass | 46 s |
| Same integration set | moved tree | pass | 213 s (ran beside R13) |
| R13 acceptance | moved tree | pass | 79 s (overlap) |
| Bucket unit tests with `-race` | moved tree | pass | 4.5 s |
| `go vet -tags integration ./internal/store/` | moved tree | clean | — |

Not run: fault tests and k6 (not named), SQL probes (the agent guard allowed no file outside `.agents`); finding 1 rests on reading `retention.sql` with `metrics.go`.

## Round 2

Judged `git diff f32983c 0090e49`: shards on both tables, take-out from any shard (`ORDER BY shard LIMIT 1`), `store.FoldRollups`, rows and rollups swept at one minute bound. Verdict: **PASS**, "no lost or doubled count in any scenario".

| # | Severity | Finding | What was done |
|---|---|---|---|
| 1 | Medium (missing test) | The fold was only run with nothing writing; its safety rests on the locking read under REPEATABLE READ, and nothing would catch a change to it. | Added "folded while three workers write, two folders at once" to `TestRollupsEqualARecountOfTheirRows`. It failed 3 of 3 runs: `attempts|…|push|unknown rollups 2 recount 0`. A take-out that waited on a fold's lock resumed past the shard 0 row the fold inserted and found its shards deleted, so a reaped call's late result left its unknown counted. The verifier's reasoning missed this (it judged the fold against inserts, not against a waiting take-out). Fixed: a take-out that changes nothing runs once more and finds shard 0, which folds never delete. Passed 3 of 3 runs after the fix. |
| 2 | Low | Replay (autocommit, not retried) taking a count out of a minute being folded can wait on or deadlock with the fold: an occasional 500 on `POST .../retry`. | Left as is (`internal/intake`, outside this lane); listed in the PR notes. |
| 3 | Low | A worker clock running fast stamps an attempt in the next minute after its notification finished: retention deletes the attempt by its notification's `finished_at` while its rollup minute stays, so a window at exactly the retention can count one attempt too many on the subtraction side. Bounded by the skew times the traffic at that boundary; the old raw read had the mirror of it. | Documented in 0014 "As built". |
| 4 | Low (check) | `EXPLAIN RetentionCandidates` showed `type: index` on an empty table. | Checked on the 1M-row bench: `range` on `finished_idx`, for a literal and a prepared parameter (288,262 rows estimated for the hour boundary). An empty-table plan. |

Found sound: the fold's locking read keeps shards 1 to 3 of its range still until commit; two concurrent folds conflict and one retries to a no-op; `ORDER BY shard LIMIT 1` stays inside its key and never takes n below 0; at window = retention both paths are exact (attempts with the clock caveat above); the migration matches `schema/`.

| Check | Result | Duration |
|---|---|---|
| `TestRollupsEqualARecountOfTheirRows` | pass | 42.7 s |
| `TestRetention*` (two concurrent sweepers included) | pass | 70.6 s |
| `go vet -tags integration ./internal/store/ ./migrations/` | clean | — |
