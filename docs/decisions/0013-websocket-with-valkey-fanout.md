---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when clients need guaranteed, replayable event delivery
---
# Live status updates use WebSocket, fanned out through Valkey pub/sub (0013)

## Question
Workers change statuses but clients are connected to API instances, which are other processes: how does a status change reach a client watching a notification or batch, live and without polling?

## Rule
Every committed status change is published once through `notification.Publisher`. `GET /v1/ws` streams those events to subscribers; the REST API stays the source of truth.

## Context
Bonus item: "WebSocket Updates: real-time status updates via WebSocket". Workers change statuses; API instances hold the client connections; they are different processes (0002).

## Options
1. WebSocket (`github.com/coder/websocket`), subscribe by notification id or batch id. Workers publish to a Valkey channel; each API instance forwards matching events to its sockets. On connect the server first sends the current status, so a client that connects late or reconnects is never behind. Pub/sub can drop an event when an API instance restarts; the snapshot on connect and the REST lookup cover it.
2. Server-Sent Events. One-way like our traffic, resumes natively with `Last-Event-ID`, works with curl; the brief names WebSocket.
3. API instances poll the database for changes and push them. No pub/sub; added load and a delay equal to the polling interval.
4. A durable event log (Valkey Streams or an events table) with replay from a cursor. No lost events; more to build than a bonus feature justifies.

## Decision
Option 1. Slow subscribers are disconnected rather than buffered without bound.

## Confirmation
Acceptance test: a client subscribes, a notification is created, and the client receives `pending`, `processing`, `sent` in that order without polling. A second test connects after completion and receives the final status as the snapshot.

## Discussion with the owner (15 Nov 2026)
- **Does Valkey earn its place for this feature?** It is already required by the rate limiter (0009), so pub/sub costs nothing extra. Without that, a sequence column polled by each API instance (`WHERE status_seq > ?`) would be the cheaper alternative and would not lose events; with Valkey present, push is faster and lighter on MySQL.
- **Improvements accepted, built in session realtime-seq** (branch `feat-realtime-sequence-readiness`): a per-notification sequence number in every event and in the snapshot, so ordering is exact and the rule-based sequencer with its unexplained `holdFor`, `rememberFor` and `tickEvery` goes away (closing owner item 0009's late-`pending` race); and readiness that depends on MySQL only, with `/v1/ws` refusing new subscriptions while Valkey is down, so intake stays in rotation.
- **Numbers** (send buffer 1024 (was 256), ping 20 s, write timeout 10 s, publish timeout 1 s, publish hold-off 100 ms, resubscribe cap 5 s) get their reasons in that branch's "As built" section. Ping 20 s already has one: it stays under the common 60 s idle timeout of proxies and load balancers.

## As built (realtime-seq, 15-16 Nov 2026)
- **Sequence.** `notifications.status_seq` starts at 1 (creation is the first status) and goes up by one in the statement of every status change. db-rules' `status_from` triggers set it (migration `20261116003432_status_seq.sql`): 1 on insert, the old value plus one when `status` changes, unchanged otherwise, whatever the statement writes to the column; `CHECK (status_seq >= 1)` is the backstop for a database without the triggers. So no query can skip or double a step, and the queries no longer mention the column except to learn it. Every event carries it (`notification.Event.Seq`, `seq` on the Valkey wire and in `StatusEvent`); the snapshot carries the stored one. How each writer knows the new value without a second read:

  | Change | Query | How the new `status_seq` is known |
  |---|---|---|
  | create | `InsertNotification`, `InsertBatchItems` | column default 1 |
  | claim (pending to processing) | `ClaimCandidates` + `LeaseNotifications` | read by the `SELECT ... FOR UPDATE`, plus one: the row lock is held to the commit |
  | sent, retry, failed, release, failed without a call | `FinishSent`, `FinishRetry`, `FinishFailed`, `ReleaseLease`, `FailSpent` | the claim's value plus one: the write is fenced by `claim_version`, and only a claim moves a row into processing (bumping `claim_version`), so a matching write is the first change since the claim |
  | reap (lease expired) | `ReapCandidates` + `ReapLeases` | read under the row lock, plus one |
  | cancel, replay | `CancelNotification`, `ReplayNotification` | `SET status_seq = LAST_INSERT_ID(status_seq + 1)`, the same value the trigger stores; it comes back in the OK packet (`sql.Result.LastInsertId`), never from a read that a later change could overtake |

- **Ordering.** Each WebSocket subscriber orders its own events (`realtime.Reorderer`), seeded after the hub subscription is taken: from the notification's snapshot, or from every item of the batch (`ListBatchStatusSeqs`). Events at or below what the subscriber has are dropped (stale or duplicate); the next one is passed on; a later one waits for its predecessors at most `holdForPredecessor`, then the gap is given up and a late predecessor is dropped when it comes. Seeded notifications are never forgotten (one per notification stream, at most 1000 per batch stream), so a subscriber never sees an older state after a newer one, however late an event lands; it misses a state only when that state's publish was lost or landed later than the hold. An unfiltered stream has no snapshot: a notification's first event there is its base, and the notification is forgotten `forgetUnseededAfter` its last event (bounded memory; exact within that window). The hub is a plain fan-out. Removed: the rule-based sequencer (`order.go`), `holdFor`, `rememberFor`, `tickEvery`, `maxTracked`, the publisher `Origin` and `Covered`.
- **Deploy order.** Migrate first, then API and workers: an old binary neither raises `status_seq` nor publishes `seq`, and an event without `seq` is passed on unordered.
- **Readiness.** `/readyz` checks MySQL only. While the hub's Valkey subscription is down, `GET /v1/ws` answers 503 (`live updates are unavailable`) and open streams close with 1012 ("reconnect and read the snapshot"); create, read and cancel keep working. The contract carries both (owner item 0016, option 1): the operation's 503 is its own `LiveUpdatesUnavailable` response, and its description lists the close codes 1008, 1012 and 1001, generated from `internal/httpapi/stream_event.go`, which holds the stream's message type, close codes and refusal in one file for the AsyncAPI document. Proven by `TestServeStaysReadyWithValkeyDownAndOnlyLiveUpdatesDegrade` (cmd/app).
- **Publisher.** The 100 ms hold-off is gone: while Valkey is taken as down (a publish timed out or lost its connection) one probe at a time tries and the rest fail at once, until a probe succeeds. The hold-off was shorter than the 1 s timeout it guarded, so with a hung Valkey every caller after the first 100 ms still waited the full second.

### Numbers

| Number | Where | Where it comes from |
|---|---|---|
| hold for a missing predecessor: 2 s | `holdForPredecessor` (`internal/realtime/seq_reorder.go`) | Derived: twice the publish timeout. A worker publishes a result or release only after the claim's publish is over (up to one timeout), then spends up to one timeout on its own; so a predecessor whose publish succeeds lands within two timeouts of the moment its successor can first arrive. Only completeness depends on it, never order. (The first version used one timeout; the verifier showed a slow but successful release publish would then be skipped.) |
| unfiltered stream forgets a notification after: 16 min | `forgetUnseededAfter` (`internal/realtime/seq_reorder.go`) | Derived: TCP gives up retransmitting unacknowledged data after about 924.6 s with Linux's default `tcp_retries2 = 15` (RFC 1122: at least 100 s), and drops the connection with those bytes, so no publish lands later than that unless Valkey itself stalls that long; rounded up. Bounds the memory of an unfiltered stream to about 120 bytes per notification with an event in the window. Tune with the host's `tcp_retries2`, against memory per unfiltered socket. Filtered streams never forget. |
| publish timeout: 1 s | `publishTimeout` (`internal/realtime/publisher.go`) | Reasoned bound (also sets the hold, above): a PUBLISH to a healthy Valkey on the same network takes about a millisecond, so one second means down or hung, not slow; one second is far inside the API's request timeout (10 s) and the worker's result-write timeout (5 s). Arbitrary within those; tune from the p99.9 of publish latency under load, keeping two orders of magnitude above it. |
| publish hold-off: 100 ms | removed | Replaced by one probe at a time while Valkey is down (no number). |
| first resubscribe wait: 100 ms | `retryFirst` (`internal/realtime/hub.go`) | Arbitrary: short so a blip costs little. Tune by measuring, while restarting Valkey, the gap between "subscription lost" and live again in the logs, and stay well under it. |
| resubscribe cap: 5 s | `retryMax` (`internal/realtime/hub.go`) | Arbitrary within a bound: it caps how long after Valkey returns new subscribers may still get 503, and 5 s is within a client's usual reconnect backoff. Tune by weighing SUBSCRIBE attempts against Valkey (one per API instance per cap) against how long clients see 503 after an outage. |
| send buffer per subscriber: 1024 events (was 256) | `APP_WS_SEND_BUFFER` (`config/config.go`) | Derived in decision 0018, "As built": 1024 holds one batch burst (about 140 KB per socket). Before that it was arbitrary. A notification subscription needs a handful (at most three events per attempt); the risk is a batch or unfiltered stream in a burst (one batch create publishes up to 1000 events in one pipeline). Each slot costs about 140 bytes per socket, allocated up front. Tune by creating a batch of 1000 under an unfiltered subscriber and raising the buffer until it is not dropped as slow, against the memory per socket. |
| ping every 20 s | `wsPingEvery` (`internal/httpapi/ws.go`) | Industry default: under the common 60 s idle timeout of proxies and load balancers (nginx `proxy_read_timeout`, AWS ALB), with room for one lost ping. |
| write timeout: 10 s | `wsWriteTimeout` (`internal/httpapi/ws.go`) | Derived: half the ping interval, so a stuck write is noticed before the next ping is due. Tune against the p99 write duration of slow mobile clients. |
| holdFor 200 ms, rememberFor 10 min, tickEvery 25 ms, maxTracked 50,000 | removed | Needed only by the rule-based sequencer. |
