---
status: open
created: 2026-11-13
tags: [decisions, process]
---
# Build overnight on decisions that are still proposed (0001)

## Question
Decision records 0002 to 0019 are `proposed` and were to be decided by the owner on 14 Nov. On 13 Nov the owner asked for an unattended overnight run that finishes the project. Which design does that run build?

## Context
- Work log 13 Nov: "nothing architectural merges before the owner accepts its decision record".
- Owner, 13 Nov evening: "go ahead and run and finish this to the end. overnight on your own".
- `CLAUDE.md`: "Never stop to ask; take the assumption and continue."

## Options
1. Build to each record's recommended option; leave every record `proposed` so the owner still accepts or rejects it. Trade-off: code exists for decisions that may be rejected; a rejection means rework in the slice that implements it.
2. Mark the records `accepted` on the owner's behalf. Trade-off: the decisions list would claim an owner decision that was not made.
3. Build only what needs no decision. Trade-off: nearly nothing; every requirement row maps to a proposed record.

## Assumption taken
Option 1. The contracts (`api/openapi.yaml`, `schema/notifications.sql`, `internal/core/`) and `docs/briefs/CONTRACT.md` encode the recommended options. Choices the records left to the owner were set as follows:
- Rate limit jitter (0009): limiter paces 3% under the limit (`APP_RATE_LIMIT_HEADROOM_PERCENT=3`), so the provider sees at most 100 per second by arrival time.
- Go's automatic replay of a keyed POST (0006): left to the delivery slice; see its hand-back.
- SMS content limit (0017): 1600 characters (10 segments).
- Retry dials (0007): base 1 s, cap 5 min, 5 attempts, provider timeout 5 s.
- Priority weights (0010): 70 / 20 / 10.
- Schedule horizon (0016): 30 days.
- Validation status code: 400 for every broken rule, 422 only for a reused idempotency key.
- The example `app_meta` table and `GET /v1/meta/{key}` were removed.

## Answer
