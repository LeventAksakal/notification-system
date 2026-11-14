---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when clients ask for batches larger than 1000
---
# A batch is accepted or rejected as a whole (0011)

## Rule
`POST /v1/notifications/batch` validates every item, then inserts all of them and the batch row in one transaction, or inserts nothing.

## Context
The brief: "batch creation (up to 1000 notifications per request)" and "query notification status by ID or batch ID".

## Options
1. Atomic. All valid: 202 with the batch id and the notification ids in request order. Any invalid: 400 listing each bad item by index and field, nothing stored. The client always knows the state: all or none. One multi-row insert.
2. Partial accept: store the valid items, return a per-item result. Friendlier when 3 of 1000 are bad; the client must reconcile which went in, and a retry with the same idempotency key (0012) becomes ambiguous.
3. Accept the batch unvalidated and validate asynchronously, reporting bad items in the batch status. Fastest response and scales past 1000; a new batch state machine, and errors arrive late.

## Decision
Pending. Recommended: option 1. Batch status returns counts per notification status plus the items page by page. More than 1000 items is refused by the spec's `maxItems` before any handler runs.

## Confirmation
Acceptance tests: 1000 valid items create 1000 rows and one batch; 1001 items are refused; 999 valid plus 1 invalid creates no rows and the error names index and field.

## As built (14 Nov 2026)
All valid answers `201` with the batch, not `202` (`api/openapi.yaml`): the rows exist when the response is written.
