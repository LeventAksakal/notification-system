---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when the Idempotency-Key draft becomes an RFC
---
# Client retries are deduplicated by an Idempotency-Key header (0012)

## Rule
A create request (single or batch) carrying `Idempotency-Key` is stored once. A repeat with the same key and the same payload returns the first response; the same key with a different payload returns 422.

## Context
The brief: "idempotency support to prevent duplicate sends". A client whose connection drops after sending a create request cannot know whether it was accepted, so it sends again.

## Options
1. `Idempotency-Key` request header, following IETF draft-ietf-httpapi-idempotency-key-header-08: the key, a hash of the payload and the first response are stored under a unique index; replay returns the stored response; 422 for a different payload; 409 while the first request is still in flight. Keys expire after a configured time (proposed 24 h). The header is optional.
2. A client reference field on each notification, unique per client. Finer grained, and it deduplicates items inside a batch; it changes the body the brief describes and leaves batch-level retries to the client.
3. Deduplicate by content hash within a time window, with no client key. Nothing for the client to do; two legitimate identical messages (two one-time passwords) are wrongly merged.

## Decision
Pending. Recommended: option 1. The unique index, not a read-then-write check, decides the race between two identical concurrent requests.

## Confirmation
Acceptance tests: the same request twice returns the same ids and creates one set of rows; twenty concurrent copies create one set of rows; the same key with a different body returns 422.
