---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when clients need guaranteed, replayable event delivery
---
# Live status updates use WebSocket, fanned out through Valkey pub/sub (0013)

## Rule
Every committed status change is published once through `core.Publisher`. `GET /v1/ws` streams those events to subscribers; the REST API stays the source of truth.

## Context
Bonus item: "WebSocket Updates: real-time status updates via WebSocket". Workers change statuses; API instances hold the client connections; they are different processes (0002).

## Options
1. WebSocket (`github.com/coder/websocket`), subscribe by notification id or batch id. Workers publish to a Valkey channel; each API instance forwards matching events to its sockets. On connect the server first sends the current status, so a client that connects late or reconnects is never behind. Pub/sub can drop an event when an API instance restarts; the snapshot on connect and the REST lookup cover it.
2. Server-Sent Events. One-way like our traffic, resumes natively with `Last-Event-ID`, works with curl; the brief names WebSocket.
3. API instances poll the database for changes and push them. No pub/sub; added load and a delay equal to the polling interval.
4. A durable event log (Valkey Streams or an events table) with replay from a cursor. No lost events; more to build than a bonus feature justifies.

## Decision
Pending. Recommended: option 1. Slow subscribers are disconnected rather than buffered without bound.

## Confirmation
Acceptance test: a client subscribes, a notification is created, and the client receives `pending`, `processing`, `sent` in that order without polling. A second test connects after completion and receives the final status as the snapshot.
