---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when the Idempotency-Key draft becomes an RFC
---
# Client retries are deduplicated by an Idempotency-Key header (0012)

## Question
A client whose connection dropped after sending a create cannot know whether it was accepted, so it sends again: how do we avoid creating the notification twice?

## Rule
A create request (single or batch) carrying `Idempotency-Key` is stored once. A repeat with the same key and the same payload returns the first response; the same key with a different payload returns 422.

## Context
The brief: "idempotency support to prevent duplicate sends". A client whose connection drops after sending a create request cannot know whether it was accepted, so it sends again.

## Options
1. `Idempotency-Key` request header, following IETF draft-ietf-httpapi-idempotency-key-header-08: the key, a hash of the payload and the first response are stored under a unique index; replay returns the stored response; 422 for a different payload; 409 while the first request is still in flight. Keys expire after a configured time (proposed 24 h). The header is optional.
2. A client reference field on each notification, unique per client. Finer grained, and it deduplicates items inside a batch; it changes the body the brief describes and leaves batch-level retries to the client.
3. Deduplicate by content hash within a time window, with no client key. Nothing for the client to do; two legitimate identical messages (two one-time passwords) are wrongly merged.

## Decision
Option 1. The unique index, not a read-then-write check, decides the race between two identical concurrent requests.

## Confirmation
Acceptance tests: the same request twice returns the same ids and creates one set of rows; twenty concurrent copies create one set of rows; the same key with a different body returns 422.

## As built (14 Nov 2026)
There is no `409` for a request still in flight: the second of two concurrent identical requests loses on the unique index, then reads and replays the first one's stored response (`internal/intake/service.go`). The key must be printable ASCII (contract 1.1.0).

## Numbers (15 Nov 2026)
| Number | Value | Where it comes from |
|---|---|---|
| Key lifetime | 24 h (`APP_IDEMPOTENCY_RETENTION`) | Industry default (Stripe expires keys after 24 h; the IETF draft leaves it to the server); longer than any client retry loop |
| Sweep interval | 1 min (`APP_IDEMPOTENCY_SWEEP_INTERVAL`) | Unexplained; only the table's size depends on it |
| Key length | at most 255 printable ASCII characters | Unexplained beyond fitting a UUID or a client token; ASCII because header values are (contract 1.1.0) |

## Known limits, acknowledged by the owner (15 Nov 2026)
- **Keys are global, not scoped per client.** There is no authentication, so two clients choosing the same key collide: a 422, or with an identical body, the other client's notification. Scoping keys per account (the account in the unique index) belongs in the backlog with authentication; the brief does not require it.
- **A replay answers with the notification as it is now**, not the first response byte for byte: the ids are identical, the status may have moved on. The IETF draft asks for the same response; the current state is kept as more useful to a retrying client.
- **Keys cover creates only.** Cancel and replay of a dead letter are idempotent by their state checks (a second cancel is a 409).
