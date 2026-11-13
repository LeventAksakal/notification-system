---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when a real provider's limits are known
---
# Each content limit is one constant, enforced in the spec, the service and the schema (0017)

## Rule
Every limit is declared once in `internal/core/limits.go`. A test fails if the OpenAPI spec, the validator or a database `CHECK` disagrees with it.

## Context
The brief: "content validation (character limits, required fields)". The provider request has three fields: `to`, `channel`, `content`. Limits are per channel and the brief gives no numbers, so they are ours to choose and state.

## Options
1. Constants plus an agreement test. The API refuses bad input with the field named; the database refuses it even if a future code path skips the validator.
2. Validate in the handler only. Least work; the second writer of the table (a batch path, a replay) can store what the first would refuse.
3. Limits as runtime configuration. Changeable without a release; a database `CHECK` cannot follow a dial, so the last line of defence is lost.

## Decision
Pending. Recommended: option 1. The numbers are the owner's call; proposed:

| Channel | `to` | `content` |
|---|---|---|
| sms | E.164: `+` and 7 to 15 digits | 1 to 1600 characters (10 segments); alternative: 160, a single segment |
| email | an address, at most 254 characters | 1 to 100,000 characters |
| push | a device token, 1 to 255 characters, opaque | 1 to 1024 characters |

Required on every notification: `to`, `channel`, `content` (or a template, 0015). `priority` defaults to `normal`. Length is counted in characters, not bytes.

## Confirmation
The agreement test above, and one acceptance test per channel at the limit (accepted) and one past it (refused, field named).
