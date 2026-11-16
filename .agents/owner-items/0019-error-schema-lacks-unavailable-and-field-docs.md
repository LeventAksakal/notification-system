---
status: closed
created: 2026-11-16
tags: [contract, httpapi, asyncapi, docs]
---
# The `Error` schema omits `unavailable` and leaves four fields undescribed (0019)

## Question
The AsyncAPI document of `/v1/ws` (generated from `internal/httpapi`, `stream_asyncapi.go`) reuses the `Error` and `ErrorDetail` schemas for its handshake refusals, as the OpenAPI document does. Two gaps in `internal/httpapi/contract.go`, a contract file the WebSocket-contract lane may not edit, show in both documents. Should the coordinator close them?

## Context
- `Error.Code`'s `doc` tag lists `invalid_request, not_found, not_cancellable, not_retryable, not_reschedulable, precondition_failed, precondition_required, idempotency_key_reused, request_too_large, internal`. It leaves out `unavailable`, but every 503 carries that code (`errUnavailable`, `errLiveUpdatesUnavailable` and the stream's other 503s in `ws.go`), and the API description's conventions name `503 unavailable`. A client that switches on the documented list has no case for the most common transient refusal.
- `Error.Message`, `Error.Details`, `ErrorDetail.Field` and `ErrorDetail.Reason` have no `doc` tag, so they have no description in either document. The brief for the AsyncAPI document asks for "a description a client author can act on" on every field.

## Options
1. Add `unavailable` to `Error.Code`'s list, and give the four fields a `doc` tag (for example: `message`, "For a person; do not parse"; `details`, "One entry per offending field; absent when the refusal is not about a field"; `field`, "The offending field, parameter or header by its contract name"; `reason`, "What rule it breaks, for a person"). Description-only: not a breaking change (oasdiff), regenerates both `api/openapi.yaml` and `api/asyncapi.yaml`.
2. Leave as is.

## Recommendation
Option 1.

## Assumption taken
Nothing changed in `contract.go`. The AsyncAPI document carries the schemas as they are, and its own refusal list names the `unavailable` code next to the 503.

## Answer

## Answer
Coordinator, 16 Nov 2026: fixed in `internal/httpapi/contract.go`. `Error.code` lists `unavailable`; `message`, `details`, `field` and `reason` have descriptions, and the spec was regenerated.
