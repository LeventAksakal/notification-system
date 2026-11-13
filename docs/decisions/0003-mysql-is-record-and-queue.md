---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when a channel's delivery limit goes well above 100 per second, or a second service must consume the same events
---
# MySQL 8.4 is the system of record and the queue (0003)

## Rule
Workers claim rows with `SELECT … FOR UPDATE SKIP LOCKED` in a READ COMMITTED transaction, through an index whose order matches the query's `ORDER BY`. No message broker.

## Context
The brief is titled "event-driven" and requires status lookup, cancel before send, and filtered listing, so a database holds every notification whatever moves the work. Delivery is capped at 100 per second per channel: 300 per second in total. A burst of 500,000 is a backlog the table must hold for about half an hour per channel, not a throughput problem.

## Options
1. MySQL only. Priority, due time and cancel live in the row the claim query reads. One store, so no dual write. Throughput ceiling is far above 300 per second. Risk: lock behaviour (see Confirmation).
2. MySQL plus a broker with the features built in (RabbitMQ priorities and delayed messages, NATS JetStream scheduling). Native queueing; but every accept writes to two systems, which needs a transactional outbox, and cancel still needs a database check at send time.
3. MySQL plus Kafka. Best burst absorption and fan-out; no per-message priority, delay or cancel, the heaviest container, and the same outbox.
4. PostgreSQL instead of MySQL. Same `SKIP LOCKED` design; no difference that matters here.

## Decision
Pending. Recommended: option 1 behind the `core.Queue` interface, so a broker can replace it when the revisit trigger fires. "Event-driven" is met by the state change events the workers publish (0013), not by adding a broker the load does not need.

## Confirmation
- A test runs `EXPLAIN` on the claim query and fails on `Using filesort` or a full scan (InnoDB locks every row it scans; MySQL bug 115246).
- A concurrency test with several claimers asserts no row is claimed twice.
- Facts and sources: `.agents/research/2026-11-13-design-facts.md`.
