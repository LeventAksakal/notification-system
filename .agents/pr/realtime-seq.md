# Exactly ordered live events (status_seq); readiness on MySQL only (decision 0013)

Branch `claude/feat-realtime-sequence-readiness-647281`. Not pushed; no PR opened.

**Merge as a squash.** The first commit (46aefb6 before the rebase, now 07fc05a) does not build on its own: a `git rm` of `internal/realtime/order.go` was already staged when its path-limited `git add` ran, so the deletion landed one commit early (the old hub still used the sequencer). Amending is not allowed here, so the commits are not atomic.

## What changed

### 1. A per-notification sequence
`notifications.status_seq BIGINT UNSIGNED NOT NULL DEFAULT 1`, raised by one in the statement of every status change by db-rules' `status_from` triggers (1 on insert, old + 1 on a status change, unchanged otherwise, whatever the statement writes), with `CHECK (status_seq >= 1)` (migration `20261116003432_status_seq.sql`, written by `task migrate:diff` on top of db-rules'; MySQL 8.4 adds the column instantly, existing rows get 1). `notification.Event.Seq` carries it to subscribers (`seq` on the Valkey wire), `notification.Job.StatusSeq` carries it to the worker.

How each writer learns the new value without reading it back:

| Change | Query | Choice |
|---|---|---|
| create | `InsertNotification`, `InsertBatchItems` | column default 1; the create event is now always `pending(1)` |
| claim | `ClaimCandidates` + `LeaseNotifications` | `ClaimCandidates` reads it under `FOR UPDATE`; new = read + 1 (lock held to commit), like `claim_version` |
| sent, retry, failed, failed without a call | `FinishSent`, `FinishRetry`, `FinishFailed`, `FailSpent` | the claim's value + 1. The write is fenced by `claim_version`, and only a claim moves a row into processing (bumping `claim_version`), so a write that matches is the first change since the claim. `LAST_INSERT_ID(expr)` would also work but needs the Queue interface to return the value; the fence makes it exact already |
| release | `ReleaseLease` | as above |
| reap | `ReapCandidates` + `ReapLeases` | read under the row lock, + 1 |
| cancel, replay | `CancelNotification`, `ReplayNotification` (now `:execresult`) | `status_seq = LAST_INSERT_ID(status_seq + 1)` (the trigger stores the same value); the value comes back in the OK packet (`sql.Result.LastInsertId`). A read-back here could see a later change (a worker claims a replayed row at once) |

Found on the way: the create event re-read the row and published its *current* status, so when a worker claimed the row first the create published `processing` and `pending` was never announced. It now publishes `pending(1)`.

### 2. Exact ordering, per subscriber
`internal/realtime/seq_reorder.go` (`Reorderer`) replaces `order.go`. Each WebSocket pump owns one, seeded after the hub subscription is taken: a notification subscription from its snapshot's `status_seq`, a batch subscription from every item (`ListBatchStatusSeqs`). Stale and duplicate events are dropped; the next one passes; a later one waits for its predecessors at most `holdForPredecessor` (2 × the publish timeout), then the gap is given up and the late predecessor is dropped when it arrives. Seeded notifications are never forgotten, so a subscriber never sees an older state after a newer one however late an event lands; it can miss a state only when that state's publish was lost or landed after the hold. Unfiltered streams forget a notification 16 min after its last event (bounded memory). The hub is now a plain fan-out (no ordering state, no ticker). Removed: `holdFor`, `rememberFor`, `tickEvery`, `maxTracked`, `Origin`, `Covered`.

Deviation from the brief ("the hub orders by it"): ordering lives with the subscriber because the base it orders against (snapshot, batch seed) is per subscriber; per-subscriber state also removes `rememberFor` and the 25 ms tick. Unfiltered streams have no snapshot: a notification's first event there is its base (documented).

### 3. Readiness without Valkey
`/readyz` checks MySQL only (`cmd/app/main.go`). While the hub's subscription is down, `GET /v1/ws` answers 503 `{"code":"unavailable","message":"live updates are unavailable: ..."}` and open streams close with 1012 (`ErrFeedLost`). Contract (owner item 0016, option 1): `/v1/ws`'s 503 is its own `LiveUpdatesUnavailable` response; the operation description lists the close codes 1008, 1012, 1001; `StatusEvent` gains `seq`. The stream's message type, mapping, refusal and close codes live in `internal/httpapi/stream_event.go` (the AsyncAPI lane generates from it). `task api:breaking`: no breaking change against `main` or the integration tip.

### 4. Publisher hold-off removed
The 100 ms hold-off was shorter than the 1 s publish timeout it guarded: with a hung Valkey, every caller after the first 100 ms still waited the full second. Now, while Valkey is taken as down, one probe at a time tries and the rest fail at once, until a probe succeeds. One number fewer.

### Deploy order
Migrate, then API and workers. An old binary neither raises `status_seq` nor publishes `seq`; events without `seq` pass unordered.

## Numbers
Each reason is also beside its constant and in decision 0013, "As built", Numbers.

| Number | Where | Where it comes from |
|---|---|---|
| hold for a missing predecessor: 2 s | `holdForPredecessor` | derived: 2 × publish timeout (a worker's result or release publish first waits for the claim's publish, then spends its own timeout) |
| unfiltered stream forgets after 16 min | `forgetUnseededAfter` | derived: TCP retransmission give-up, Linux `tcp_retries2 = 15` ≈ 924.6 s, rounded up; filtered streams never forget |
| publish timeout: 1 s | `publishTimeout` | reasoned bound: a healthy PUBLISH takes about 1 ms; far inside the 15 s request and 10 s store timeouts; arbitrary within, tune from the p99.9 publish latency |
| publish hold-off: 100 ms | removed | replaced by one probe at a time while down |
| first resubscribe wait: 100 ms | `retryFirst` | arbitrary; tune from the measured lost-to-live gap on a Valkey restart |
| resubscribe cap: 5 s | `retryMax` | arbitrary within a client's reconnect backoff; bounds the 503 window after Valkey returns |
| send buffer: 256 events | `APP_WS_SEND_BUFFER` | arbitrary; about 140 B per slot per socket; tune with a 1000-item batch under an unfiltered subscriber |
| ping every 20 s | `wsPingEvery` | industry default: under the 60 s idle timeout of proxies and load balancers, with one lost ping to spare |
| write timeout: 10 s | `wsWriteTimeout` | derived: half the ping interval |
| holdFor, rememberFor, tickEvery, maxTracked | removed | served only the rule-based sequencer |

## Tests (all `-race`, integration ones pinned with `-cpu 2`; 16 Nov, loaded shared machine)
- Unit: `TestReorderer*` (every arrival order of a four-step lifecycle; duplicates and stale; interleaved notifications; a lost predecessor; two gaps; unseeded start; forgetting; a seeded notification never forgotten; a replay; unsequenced events), `TestPublisher*` (including a probe answered with an error reply). realtime ok 7.2 s, intake 0.7 s, httpapi 4.1 s; delivery, store, cmd/app, notification and config ok without -race (41 s).
- `TestReleaseAndReclaimRaceReachesTheSubscriberInCommitOrder` (internal/realtime; owner item 0009): two workers, the releasing one behind a proxy that delays its bytes 0.3 s, 1.5 s or 3 s. Arrival is `processing(4) sent(5) pending(3)` each time; passed on: every state in order for 0.3 s and 1.5 s, `processing(4) sent(5)` for 3 s. Package 140 s with race.
- `TestStatusSeq` (internal/store): each status query raises status_seq by one and the writer's value equals the stored one; a fenced-out write changes nothing; reap; create, cancel and replay events; 200 rows under concurrent claims and cancels (171 claimed, 29 cancelled, each with seq 2); the migration on a populated table (rows get 1). 91 s.
- `TestServeStaysReadyWithValkeyDownAndOnlyLiveUpdatesDegrade` (cmd/app, the real `serve`): batch subscription skips a stale event published after its seed; Valkey down: open stream closed 1012, create 201, `/readyz` 200, read 200, cancel 200, `/v1/ws` 503 "live updates are unavailable"; three flaps; then a new subscription gets its snapshot and a live cancel. 82 s.
- `internal/intake` integration (Valkey down, batch create bounded): ok, 67 s.
- Lint (`tools/lintguard`) on every touched package: 0 issues (95 s, then 21 s and 28 s on the two fixed packages).
- **Acceptance tests not changed** (guard, owner item 0002). Expected to fail on this branch until the drafts in `.agents/pr/realtime-seq-acceptance.md` are applied: TestR15HealthAndReadiness, TestR15ReadyzMySQLDown, TestR15ReadyzValkeyDown, the TestGapOutage* tests. The B4 WebSocket acceptance tests were not run here.

## Verifier
Report: `.agents/reviews/2026-11-15-realtime-seq.md`. Blocker (late predecessor after a seeded track was forgotten) fixed; unbounded unfiltered memory fixed; hold derivation fixed (1 s → 2 s); missing tests added; LastInsertId fallback added. Pre-existing: `TestWorkersAcrossSmallLimiterClockSteps` fails under -race on main too (1 of 4 runs on each).

## Gate
Not run by this lane: the merge protocol (`.agents/merge-queue.md`) leaves the full gate to the coordinator after the squash. Scoped checks are above.

## Done at the rebase onto 7387e51 (16 Nov)
- Migration regenerated on top of db-rules' (`20261116003432_status_seq.sql`); the triggers own `status_seq`; `CHECK (status_seq >= 1)`; the explicit `status_seq + 1` dropped from the queue queries.
- `TestStatusSeq` gains "the triggers set status_seq whatever a statement writes" (insert writing 7 stores 1; an update writing 99 without a status change keeps it; a status change not naming the column raises it; old + 5 stores old + 1); the migration case steps down to 20261115233413.
- Huma: `seq` on `StatusEvent`, the `LiveUpdatesUnavailable` 503, the close codes; `stream_event.go`.
- `FailSpent` (db-rules' processing -> failed without a call) publishes the claim's seq + 1.
- Pre-existing on 7387e51, not from this lane: `test/acceptance/delivery_wake_test.go:42` uses `n.ID`, the generated client has `Id`, so `go vet -tags integration ./test/acceptance` fails.
