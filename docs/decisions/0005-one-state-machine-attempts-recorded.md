---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when a provider offers delivery receipts
---
# A notification moves through one explicit state machine and every attempt is recorded (0005)

## Question
What states can a notification be in, who may move it between them, and what is kept about each try to deliver it?

## Rule
Status changes happen only through `notification.Transition(from, to)`, and every change is a conditional `UPDATE … WHERE status = <from>`. Each provider call writes one `delivery_attempts` row.

## Context
The brief asks for "real-time status tracking", "cancel pending notifications" and visibility "for both internal teams and API consumers". The provider answers 202 "accepted"; it never confirms delivery to the end user.

## Options
1. Stored states `pending`, `processing`, `sent`, `failed`, `cancelled`. A retry returns to `pending` with a later due time and `attempts + 1`. The API additionally shows `scheduled` for a pending notification whose due time is in the future and that has never been tried. Success is called `sent`, not `delivered`, because that is all the provider tells us. Attempts (status code, latency, error, time) go to their own table, which also feeds the metrics endpoint (0014).
2. Three states (`pending`, `sent`, `failed`) plus flags. Fewer transitions; an API consumer cannot tell "queued" from "being sent" from "waiting to retry".
3. Add `delivered` and `retrying` as stored states. Reads well; `delivered` would claim something we cannot know, and `retrying` duplicates what `attempts` and the due time already say.

## Decision
Option 1. Cancel is allowed from `pending` only (including scheduled and waiting-to-retry) and is a conditional update, so a cancel racing a claim has exactly one winner; the loser gets 409.

## Confirmation
A table-driven test covers every (from, to) pair. A race test fires cancel and claim at the same row many times and asserts the provider double received the message if and only if the final status is `sent`.

## As built: the database holds the same rules (15 Nov 2026)
The owner accepted this record in chat on 15 Nov 2026 with an addition: every business rule is enforced at every layer ("might seem redundant but every layer achieves something"). The Go map plus conditional `UPDATE ... WHERE status = <from>` stopped every Go writer, but a hand-written `UPDATE` could still move `cancelled` to `sent`.

How MySQL now holds the state machine (`schema/notifications.sql`): sqldef cannot parse `SIGNAL` (decision 0023), so the refusal is a `CHECK`, not the trigger. Two `BEFORE` triggers keep `status_from` up to date: `'created'` on insert, the old status whenever `status` changes, the old `status_from` otherwise (a writer cannot forge it). The `status_transition` CHECK, which MySQL evaluates after the triggers, allows exactly the edges of `transitions` plus `created -> pending`, so an insert can only be `pending`. A refused change is MySQL error 3819 naming `status_transition`; `store.RuleViolation` maps it to `notification.ErrInvalidTransition`, the error the Go gate returns. Rows that existed before the migration have `status_from` NULL ("origin unknown"); their next status change sets it and is checked. NULL is the one gap: the CHECK passes it, and only those older rows (or a writer with DDL rights dropping the triggers) can have it, since the triggers never write NULL.

Why not a trigger that refuses with `SIGNAL SQLSTATE '45000'` naming both states: sqldef 3.11.30 (the latest release) has no `SIGNAL` in its trigger grammar (the token is reserved as `UNUSED`), so such a trigger cannot live in `schema/` (decision 0023). Options weighed with the coordinator on 15 Nov 2026, A chosen:

- A (built): triggers feed `status_from`, a fixed CHECK holds the allowed pairs. Inside sqldef; costs one 1-byte column, and the error names the constraint, not the two states (the store adds that meaning).
- B: teach sqldef `SIGNAL`, by an upstream pull request or a fork pinned with `replace`. Keeps the message with both states; a fork to maintain, or a wait on upstream.
- C: hand-write the `SIGNAL` triggers in a migration and make `schemadiff` ignore triggers. Breaks decision 0023: two sources of truth, and the drift check cannot see the triggers.
- D: a trigger that fails on purpose with a statement sqldef can parse (`SET NEW.status = NULL` raises 1048). Expressible; the error ("column cannot be null") misleads whoever reads it.

An update that leaves `status` alone keeps the pair the row already had (the trigger copies `OLD.status_from`); no self-pairs are allowed, so rewriting the same status is not a transition. Turning the triggers off and on again needs `status_from` reset to NULL first: without them it goes stale and the CHECK refuses to come back (seen in the latency measurement).

Every rule, and where it is enforced:

| Rule | Go | Database |
|---|---|---|
| Status changes follow `transitions` | `Transition`, conditional UPDATEs | triggers + `status_transition` |
| A notification starts `pending` | intake inserts omit `status` | `status_transition` (`created -> pending` only) |
| `finished_at` set exactly when sent, failed or cancelled | the finish, cancel and replay queries | `finished_exactly_when_terminal` |
| `lease_until` set only while processing | claim sets it, every exit clears it | `lease_only_while_processing` |
| `failure_kind` only when failed | `FinishFailed`, replay clears it | `failure_kind_only_when_failed` (existed) |
| `provider_message_id` on a notification only when sent | `FinishSent` | `provider_id_only_when_sent` |
| Sent or failed only after an attempt | finish queries add 1 | `attempted_before_sent_or_failed` |
| A batch position only with a batch, one row per position | batch insert | `batch_position_with_batch`, unique `batch_position_idx` |
| Attempts numbered from 1 | `job.Attempts + 1` | `attempt_no_from_one` |
| An attempt's provider id only when accepted | provider reads it from a 2xx body | `attempt_provider_id_only_when_accepted` |
| Accepted means a 2xx | `provider.Classify` | `attempt_accepted_is_2xx` |
| Stored HTTP status 100..999 | capped at 999 | `attempt_http_status_range` |
| A failed or unknown attempt says why; an accepted or in-flight one has no error | `Classify`, the reaper's and release's text | `attempt_error_exactly_when_failed_or_unknown` |
| Attempt outcomes follow `attemptTransitions` (`in_flight` first, then a result; `unknown` may still get one) | `AttemptTransition`, `FinishAttempt`'s `WHERE outcome IN ('in_flight','unknown')` | triggers + `attempt_outcome_transition` |
| An attempt starts `in_flight`, carrying its claim | the claim's `BeginAttempts` | `attempt_outcome_transition` (`created -> in_flight` only), `attempt_claim_known` |
| No response, duration or error before a result | `BeginAttempts`, the reaper | `attempt_no_result_yet`, `attempt_error_exactly_when_failed_or_unknown` |
| One provider call per claim | the claim inserts once per row | unique `attempts_claim_idx (notification_id, claim_version)` |
| Content and recipient limits | validator, spec | existing CHECKs (decision 0017) |

Kept in Go, and why:
- The attempt budget (`APP_MAX_ATTEMPTS`) and the retry delays are settings; a CHECK cannot follow a setting.
- Attempt numbers are consecutive within a budget and restart after a replay: that depends on the other rows of the notification, and a CHECK sees one row.
- An attempt's channel equals its notification's: cross-table; enforcing it would take a composite foreign key and an extra index on both tables, paid on every insert, for a value the store copies from the claimed job.
- `due_at`, `finished_at` and `created_at` order: `created_at` comes from the API's clock and the others from the database's, so a CHECK would refuse writes whenever the clocks disagree.
- `scheduledAt` within the horizon, and a lease in the future: relative to now, and MySQL CHECKs may not call `NOW()`.
- `claim_version` only grows: comparing old and new values needs `SIGNAL` or another trigger-fed column; the fence it protects already lives in every worker's `WHERE`.

Costs: the migration adds each CHECK with its own `ALTER TABLE`, and MySQL validates a new CHECK by copying the table (2.0 s on an empty schema, mostly fixed cost; proportional to the rows on a populated table, writes blocked meanwhile). The app user creates triggers while binary logging is on, which MySQL 8.4 allows only with `log_bin_trust_function_creators` (deprecated; safe with the default ROW binlog format); compose and the test containers set it, and owner item 0014 asks how production runs migrations. Claim and finish latency with and without the rules (`TestMeasureRulesLatency -measure`: 8 workers, 150,000 pending rows, 10 s rounds alternating on, off, on, off, 15 Nov 2026): claim p50 33.4 / 23.6 ms on against 37.0 / 25.2 ms off, finish p50 47.0 / 36.4 ms against 54.3 / 38.2 ms; p99 (claim 134 / 190 against 118 / 46 ms, finish 230 / 127 against 194 / 141 ms) moved with the machine more than with the rules. No cost is visible at p50.

## As built: attempts written before the call (15 Nov 2026)
Owner's choice on 15 Nov 2026 (option A of three: record before the call, record after it as before, or record before only when the provider has no dedupe). Before, the attempt row was inserted with the result, so a worker that died between the request and the result left no trace of a call the provider may have accepted, and a worker whose lease was lost had its attempt rolled back with the refused status update.

Now each call is two writes:
1. The claim writes them: in the claim's transaction, after the lease, one `INSERT ... SELECT` records every claimed row's call as `in_flight` with the new `claim_version` (`BeginAttempts`). The attempts commit with the lease, before any request can leave, at the cost of one statement per claimed group and no extra round trip on the send path. Two earlier placements failed: a per-job insert right before the request added its latency to the send (outside the rate limiter's tolerance, and past the lease; verifier findings 1 and 2), and a per-job insert before the wait for the slot turned database latency into missed slots under load (the in-process rate test sent 118 of 1,500 in 60 s during the full integration run).
2. After the call, the finish writes the notification and updates the attempt to its result in one transaction. If the lease was lost meanwhile, the attempt is still updated (the result replaces `in_flight` or `unknown`) and committed, and the finish returns `ErrLeaseLost`; only the notification is left to whoever holds it now. A finish with no attempt begun is refused.

A job given back before its request left (slot missed, provider outage declared, no free call, the claim outlasted its slots, shutdown) is released, and the release removes its `in_flight` row: no call, no attempt. A worker that dies between the claim and the send leaves `in_flight` rows the reaper marks `unknown` although no request left; "unknown" means "may have been sent", and the window is the reservation horizon (200 ms by default). A call that may have left is never released: the reaper marks the `in_flight` attempts of expired leases `unknown` and counts them in `attempts` and in `budget_spent` (same statement), and a call aborted by shutdown is written as an `unknown` result, due again at once, counted in `attempts` but not in `budget_spent`. The history of a call whose worker died then reads "attempt 1 unknown, attempt 2 accepted", and the provider's dedupe on the notification id delivers it once (`internal/delivery/attempt_integration_test.go`). The budget is `budget_spent` since provider health (0026): a failure counts only while its provider is believed healthy. The reaper cannot tell an outage from a message that kills its worker, and the second must end, so an unknown call from the reaper counts; a shutdown abort says nothing about the provider or the message, so it does not, and the shared health is not told about it. A claimed job whose `budget_spent` already reached `APP_MAX_ATTEMPTS` fails as exhausted without a call, its in-flight row removed and both counts unchanged (slot given up, reason `attempts_spent`). An outage failure (0026) is the same row: written `in_flight` by the claim, finished as `transient` with `budget_spent` unchanged; a job released because an outage was declared before the send loses its `in_flight` row like any release. `started_at` is the claim's time, at most the reservation horizon before the request; the finish does not rewrite it, which would move the row in `attempts_window_idx` a second time on every send.

The API does not show unknown calls yet: its outcome enum has no `unknown` (owner item 0015). Until it does, the attempt list returns only calls with a result (an unknown call is the gap in `attemptNo`), `/metrics` `attempts` counts only those, and the store's `UnknownAttempts` is computed but not served. Retention needs no new rule: a finished notification has no `in_flight` attempt (the finish or the reaper resolved it).

Deploying and rolling back:
- A worker binary from before this change cannot finish anything once the migration ran: its attempt insert has no `claim_version`, which `attempt_claim_known` refuses. Stop the old workers before migrating (as for any queue change here).
- The migration's Down narrows the outcome enum back, which fails while `in_flight` or `unknown` rows exist; delete or convert those rows first.

Cost of the extra write (`TestMeasureAttemptWriteCost -measure`, the same file run on 6bb0c40, the tree before attempts were written first; 15 to 16 Nov 2026 on a Docker Desktop host shared with other sessions' stacks, so runs vary several-fold):

| | before | claim writes the attempts |
|---|---|---|
| 1,500 sends, 3 workers at 100/s per channel | 15.9 s, 17.3 s; 48.9 s and 81.4 s when the host was saturated | 11.2 s; 970 sent in 90 s when the host was saturated |
| Slots given up | 3,676 and 4,560 (calm runs) | 2,440 |
| Busiest 1 s window at the provider (limit 100) | 76 to 96 | 88 to 96 |
| Idle, insert to provider arrival p50 / p99 | 354 / 386 ms, 358 / 592 ms | 352 / 821 ms, 359 / 1,222 ms |

The cap held in every run, and the idle median did not move; its p99 is the host's. The per-job insert this replaced made the in-process rate test send 118 of 1,500 in 60 s in a full integration run. A comparison on a quiet host is still owed (pull request notes).

Database-level tests: plain SQL from Go against a real MySQL (testcontainers, tag `integration`), not MyTAP. MyTAP installs stored routines (it needs `CREATE ROUTINE` and the same binlog trust as the triggers), the exhaustive pair loops would have to be written as SQL procedures, its TAP output needs a Perl `prove` runner the project does not have, its schema assertions repeat the `migrate:check` drift check, and the agreement tests must read the Go maps anyway. `internal/store/rules_integration_test.go` drives every trigger and CHECK with hand-written SQL, and `internal/store/sql_integration_test.go` sends the real text of `internal/store/queries/*.sql` (sqlc's arguments turned into `?`) for the `EXPLAIN` and retention cases.

Confirmation: `internal/store/rules_integration_test.go` (tag `integration`) tries every (from, to) pair of stored states with hand-written SQL and asserts the database allows exactly what `Transition` allows; changing the Go map alone fails it (checked: adding `cancelled -> pending` fails with the CHECK named). The same test tries every pair of attempt outcomes against `AttemptTransition`. Each CHECK above has a case that the database refuses a breaking row.
