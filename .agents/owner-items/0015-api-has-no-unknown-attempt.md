---
status: closed
created: 2026-11-15
tags: [api, contract, attempts, metrics]
---
# The API has no word for a call whose result is unknown (0015)

## Question
Attempts are now written before the provider call (decision 0005, "Attempts written before the call"), and a call whose worker died is stored as outcome `unknown`. The API contract (`api/`, owned by the huma code-first work) lists attempt outcomes `accepted, transient, permanent` only, and `/metrics` has no field for unknown calls. How should the API show them?

## Context
Found by the verifier on 15 Nov 2026: `GET /notifications/{id}` would have returned `unknown`, which the spec's enum forbids. Until the contract changes, the store hides `in_flight` and `unknown` rows from the attempt list (an unknown call shows as a gap in `attemptNo`), and `/metrics` `attempts` counts only calls with a result (as documented: `failedAttempts` is "not accepted"); `store.Delivery.UnknownAttempts` is computed but not served.

## Options
1. Add `unknown` to the attempt outcome enum and `unknownAttempts` to the delivery metrics. Clients see the whole history and can tell a crash from a failure; a contract change, and clients with a closed enum must accept the new value.
2. Keep the API as it is (gap in `attemptNo`, no metric). No contract change; the API hides a call the provider may have accepted.
3. Map `unknown` to `transient` in the API. No contract change; misreports what is known.

## Recommendation
Option 1, in the huma code-first branch that owns `api/` (the store already has the data).

Assumption taken meanwhile: option 2.

## Answer
Owner, 16 Nov 2026: "yeah add unknown too". The attempt `outcome` enum gained `unknown` and `GET /v1/notifications/{id}` lists unknown attempts (`ListDeliveryAttempts`); the contract went to 2.0.0 with 0017's limits.
