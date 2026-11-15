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
- **Numbers** (send buffer 256, ping 20 s, write timeout 10 s, publish timeout 1 s, publish hold-off 100 ms, resubscribe cap 5 s) get their reasons in that branch's "As built" section. Ping 20 s already has one: it stays under the common 60 s idle timeout of proxies and load balancers.
