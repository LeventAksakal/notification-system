# MySQL enforces the state machine and the fixed business rules; finished notifications expire; attempts are written before the call

Not pushed and not opened as a pull request (owner, 15 Nov 2026): the coordinator merges the worktrees locally and pushes once. This file is what the description would have been.

## What changes
1. **The database holds the notification state machine** (record 0005, "As built"). `BEFORE` triggers keep `status_from` (`'created'` on insert, the old status when `status` changes, the old `status_from` otherwise, so a writer cannot forge it). The `status_transition` CHECK allows exactly `internal/notification/status.go` `transitions` plus `created -> pending`, so an insert can only be `pending`. A refusal is MySQL 3819 naming the constraint; `store.RuleViolation` maps it to `notification.ErrInvalidTransition`. An update that leaves `status` alone keeps the pair the row had (no self-pairs). Why not `SIGNAL`, and options B, C, D with their costs: record 0005.
2. **Every fixed business rule is a CHECK** (table below). Rules that stay in Go are listed with the reason.
3. **Retention** (record 0025, accepted). `APP_NOTIFICATION_RETENTION` (default 168h, bounds 1h..365d, never below `APP_IDEMPOTENCY_RETENTION`). Every `app serve` sweeps on the idempotency tick: steps of 100 rows (`SELECT ... FOR UPDATE SKIP LOCKED` on `finished_idx`, attempts deleted first, then notifications), a pause as long as each full step, then empty batches older than the retention (`batches_created_idx`).
4. **Attempts written before the call** (owner's choice A, record 0005). The claim records every claimed row's call as `in_flight`, with its new `claim_version`, in the claim's own transaction: one `INSERT ... SELECT` per group, so the send path gains no round trip. The finish updates the attempt to its result, even after a lost lease. The reaper marks `in_flight` attempts of expired leases `unknown` and counts them, in `budget_spent` too (record 0026's budget: the reaper cannot tell an outage from a message that kills its worker). A release (no call made, including an outage declared before the send) removes the in-flight row. A call aborted by shutdown is an `unknown` result, not counted against the budget. A job whose `budget_spent` already reached `APP_MAX_ATTEMPTS` fails as exhausted without a call. An outage failure (0026) is the same in-flight row finished as `transient` with the budget unchanged. The outcome state machine and one call per claim are enforced in the database.
5. **`tools/schemadiff`** drops removed triggers in Up, prepends the drops to Down, and fails `-check` on stale triggers. It also wraps trigger bodies for goose (nested blocks included) and writes a trigger-free `internal/store/schema.gen.sql` for sqlc.

## Business rules and where each is enforced
| Rule | Go | Database |
|---|---|---|
| Status changes follow `transitions` | `Transition`, conditional UPDATEs | triggers + `status_transition` |
| A notification starts `pending` | intake omits `status` | `status_transition` |
| `finished_at` exactly when sent, failed or cancelled | finish, cancel, replay queries | `finished_exactly_when_terminal` |
| `lease_until` only while processing | claim sets it, every exit clears it | `lease_only_while_processing` |
| `failure_kind` only when failed | `FinishFailed`, replay | `failure_kind_only_when_failed` |
| Provider message id on a notification only when sent | `FinishSent` | `provider_id_only_when_sent` |
| Sent or failed only after an attempt | finish adds 1 | `attempted_before_sent_or_failed` |
| A batch position only with a batch, one row per position | batch insert | `batch_position_with_batch`, unique `batch_position_idx` |
| Attempts numbered from 1 | `attempts + 1` | `attempt_no_from_one` |
| Attempt outcomes follow `attemptTransitions` | `AttemptTransition`, `FinishAttempt` WHERE | triggers + `attempt_outcome_transition` |
| An attempt starts `in_flight`, with its claim | the claim's `BeginAttempts` | `attempt_outcome_transition`, `attempt_claim_known` |
| No response, duration or error before a result | `BeginAttempts`, reaper | `attempt_no_result_yet` |
| A failed or unknown attempt says why; accepted / in flight has no error | `Classify`, reaper | `attempt_error_exactly_when_failed_or_unknown` |
| Attempt provider id only when accepted | provider reads a 2xx body | `attempt_provider_id_only_when_accepted` |
| Accepted means 2xx | `Classify` | `attempt_accepted_is_2xx` |
| Stored HTTP status 100..999 | capped | `attempt_http_status_range` |
| One provider call per claim | the claim inserts once per row | unique `attempts_claim_idx` |
| Content and recipient limits | validator, spec | existing CHECKs (record 0017) |

Kept in Go:
- **Attempt budget and retry delays.** They are settings.
- **Attempt numbering, consecutive across a notification's rows.** That spans rows.
- **An attempt's channel equals its notification's.** That spans tables.
- **Ordering between `created_at` and the database-clock columns.** The clocks differ.
- **Rules relative to now.** A CHECK cannot call `NOW()`.
- **`claim_version` only grows.** The check would need `SIGNAL`.

## Database-level tests: plain SQL from Go, not MyTAP
MyTAP would need:
- `CREATE ROUTINE` plus the same binlog trust as the triggers;
- the exhaustive loops written as SQL procedures;
- a Perl `prove` runner.

It would also duplicate `migrate:check`, and the agreement tests must read the Go maps anyway. So the tests send hand-written SQL through `database/sql` to a real MySQL (testcontainers, tag `integration`), with no store layer in between:
- `rules_integration_test.go` covers:
  - every status pair and every attempt-outcome pair against the Go maps;
  - insert-only-pending and insert-only-in-flight;
  - forging `status_from` and `outcome_from` on insert and on update;
  - each CHECK refusing a breaking row;
  - the unique keys.
- `sql_integration_test.go` sends the real text of `queries/*.sql`, covering:
  - `EXPLAIN` of the four retention statements;
  - the delete;
  - the foreign key refusing notifications before their attempts;
  - `SKIP LOCKED` against a replay's lock.

## Measurements (loaded Docker Desktop harness, 15 Nov 2026)
**Rules on/off.** 8 workers, 150k pending rows, 10 s rounds alternating on, off, on, off:

| | on | off |
|---|---|---|
| Claim p50 | 33.4 / 23.6 ms | 37.0 / 25.2 ms |
| Finish p50 | 47.0 / 36.4 ms | 54.3 / 38.2 ms |
| Claim p99 | 134 / 190 ms | 118 / 46 ms |
| Finish p99 | 230 / 127 ms | 194 / 141 ms |

No cost is visible at p50; p99 follows the machine. Adding the CHECKs copies each table (3.7 s for the whole migration on an empty schema).

**Retention.** 100k expired rows beside 150k pending, 8 claiming workers, 100-row steps. The sweep took 2 min 9 s.

| Statement | avg | p99 | max |
|---|---|---|---|
| `SELECT` (candidates) | 2.5 ms | 10.5 ms | 53 ms |
| attempts `DELETE` | 9.9 ms | 31.6 ms | 74 ms |
| notifications `DELETE` | 15.1 ms | 60.3 ms | **216 ms** |

The target "each DELETE under 100 ms" holds at p99 and is **missed once in 1,000**. Claim p99 was 150 ms during the sweep, against 126 / 93 ms before and after; the target was "within noise". The first try used 500-row steps: DELETE avg 68–89 ms, max 476 ms, claim p99 513 ms. That led to the 100-row steps and the pause.

**Attempt write.** `TestMeasureAttemptWriteCost`; the identical file was run on 6bb0c40 (before). The host is shared with other sessions' stacks (three workers and three scratch databases ran throughout), so the runs vary several-fold.

| | before | claim writes the attempts |
|---|---|---|
| 1,500 sends at 100/s per channel | 15.9 s, 17.3 s; 48.9 s and 81.4 s with the host saturated | 11.2 s; 970 sent in 90 s with the host saturated |
| Slots given up | 3,676 and 4,560 (calm runs) | 2,440 |
| Busiest 1 s window (limit 100) | 76–96 | 88–96 |
| Idle insert → provider, p50 / p99 | 354 / 386 ms, 358 / 592 ms | 352 / 821 ms, 359 / 1,222 ms |
| Claims that returned jobs, p50 / p99 | 209 ms / 1.65 s (saturated) | 102 / 474 ms (rate), 41–57 / 123–399 ms (idle) |

- The cap held in every run, and the idle median did not move.
- How the placement was chosen: the per-job write right before the request is verifier findings 1 and 2. A per-job write before the slot wait made the in-process rate test send 118 of 1,500 in 60 s in the full gate. The claim-time write replaced both.
- **Still owed: a comparison on a quiet host.**

**Crash test.** `internal/delivery/attempt_integration_test.go`, provider dedupe on. A worker sends 10 SMS and dies before recording any result. After the next worker, every notification reads "attempt 1 unknown, attempt 2 accepted": 2 requests and 1 delivery per key.

## Fresh-context verifier
Report: `.agents/reviews/2026-11-15-db-rules-retention.md`. Verdict on 6da52c1: **fail**, with 2 high, 2 medium, 4 low findings and missing tests. All were fixed or documented in e70875e, a912b4c and dae271a:
- **High: the attempt write extended the call past the lease, and could push a send outside the rate tolerance.** Fixed: the claim now writes the attempts, so nothing stands between the slot check and the request.
- **Medium: the API returned an `unknown` outcome its enum forbids.** The store now hides it, and owner item 0015 asks for the contract change.
- **Medium: unknown calls could exceed the attempt budget.** A job whose budget is spent now fails as exhausted without a call.
- **Low:** the NULL `status_from` gap, the Down migration with new outcomes present, and old binaries during a rolling deploy are documented in 0005. The schemadiff nested-block parsing is fixed and tested.
- **Missing tests:** added.

## Gate
**After the rebase onto 156cfcb (provider-health), 16 Nov 2026, scoped checks pinned to two cores (`GOMAXPROCS=2`):**
- Migrations regenerated after provider-health's (`20261115233357_db_rules_retention`, `20261115233413_batch_position_unique`); `task migrate:check` pass; `task gen` changed sqlc only (no API change).
- `go build ./...`, `go vet ./...` and `go vet -tags integration ./internal/... ./test/support/...` pass. `go vet -tags integration ./test/...` fails on integration's own `test/acceptance/delivery_wake_test.go:42` (`n.ID`, the client field is `Id`), not touched here.
- golangci-lint on `internal/delivery`, `internal/intake`, `internal/store`, `internal/notification`: 0 issues.
- Unit, with `-race`: delivery, delivery/health, notification, store, config, tools/schemadiff pass (52 s).
- Integration: `internal/store` and `test/support` pass (117 s; then `TestQueue*` again after a fixture fix, 43 s); `internal/delivery` (including `TestWorkersHoldTheRateAtTheProvider`), `delivery/health`, `delivery/limiter` and `internal/intake` pass (77 s).
- Fixed on the way: two landed fixtures wrote a state the CHECKs refuse (`TestQueueNextDue`, the replay case of `TestIntakeWakesWorkersAfterEveryCommitThatMakesWorkPending`); delivery's goleak now ignores the shared MySQL server's Ryuk goroutine.

**Before the rebase.** Run on 15 and 16 Nov 2026, on a host shared with other sessions' Docker stacks.
- `task check` at 30a2128: **pass**, 240 s. Build, vet, lint (0 issues), 470 unit tests, govulncheck (no vulnerabilities).
- `task test:integration` at 30a2128: 230 s, **5 failures, neither caused by this branch as far as the evidence goes**:
  - `internal/store` pass (3 min 20 s, one shared MySQL server per process), `internal/intake` pass, `internal/delivery/limiter` pass, cached packages pass.
  - `internal/realtime`, 4 tests: "rootless Docker is not supported on Windows, failed to create Docker provider" when starting Valkey. The Docker provider was unavailable at that moment; the package is not touched here.
  - `internal/delivery` `TestWorkersHoldTheRateAtTheProvider`: average 11.3 to 12.6/s against the 40/s floor; the busiest second (89 to 96) stays under the cap. The base tree (6bb0c40) failed the same test alone on this host in earlier runs (20 to 33/s). **Needs a quiet host to decide.**
- After the gate, 121c8bc stopped the finish rewriting `started_at`: `go build ./...`, `go vet -tags integration ./internal/store` and `go test -tags integration ./internal/store -run TestQueue` pass (34 s).

## Files outside this slice's area, and why
- `internal/delivery/delivery.go`: 22 added and 12 removed lines against provider-health's version, in two places, all forced by attempt-first:
  - a 3-line `spent` check at the top of `sendAt` (`budget_spent` reached by unknown calls the reaper counted);
  - `deliver`'s shutdown-abort branch, which now records an unknown result (not counted, health not told) instead of releasing; the health report and `decide` move into the other branch.
- `internal/delivery/attempt.go` (new) and `delivery_test.go`: the `spent` check and its tests.
- `internal/notification/ports.go`: doc comments only (`Claim` writes the attempts; `MarkFailed` with an attempt without an outcome means no call; `Release`). The interface's methods are unchanged. The wake-hint session also edits this file.
- `internal/store/queue.go`: `claimOnce` adds the attempts insert. `queries/queue.sql` (wake-hint's) is not edited; the new statements are in `queries/attempts.sql`.
- `internal/store/queries/intake.sql`, `metrics.sql`: the attempt list and `attempts` count only calls with a result.
- `internal/store/queue_integration_test.go` `TestQueueNextDue` (wake-hint's): its fixture inserted a `sent` row directly, which the new CHECKs refuse; it now inserts pending and walks the allowed changes.
- `internal/intake/publish_integration_test.go` (wake-hint's): its replay case set a pending row to `failed` directly; it now walks through processing with one attempt.
- `internal/delivery/delivery_test.go` `TestMain`: goleak ignores testcontainers' Ryuk connection, which the shared MySQL server keeps for the process.
- `test/support/containers.go`: one MySQL server per test process, a fresh database per test. A container per test took the store package past go test's 10-minute limit; it now runs in about 5 minutes.
- `.golangci.yaml`: gosec G202 excluded for `internal/store/rules_integration_test.go` only, whose SQL fragments are fixed strings.
- Not touched: `api/`, `internal/httpapi`, `internal/realtime`, `internal/intake/service.go`, `test/acceptance/`, `docs/REQUIREMENTS.md`.
- `InsertDeliveryAttempt` in `queries/queue.sql` is now unused. It is left for the owner of that file to delete.

## Open items for the coordinator
- **Acceptance test for retention (proposed row O1).** The test-writer could not write `test/acceptance/` from this worktree: agentguard resolves the path against the main checkout. I did not bypass the guard. The proposed row and the three held tests are in `.agents/pr/db-rules-acceptance.md`.
- **Owner item 0014:** who runs migrations once the schema has triggers. The assumption meanwhile is `log_bin_trust_function_creators=ON`, set in compose and in the test containers.
- **Owner item 0015:** the API has no `unknown` outcome and no `unknownAttempts` field.
- **Record 0005's status** is left `proposed`; flipping it is the coordinator's call.
- **Measurements to re-run on a quiet machine:** the attempt-write cost pair 1, and the one 216 ms DELETE.

Branch: `claude/feat-db-state-machine-retention-e9eec0`
Final commit: the commit that adds this file (rebased onto 156cfcb).
