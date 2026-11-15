---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when a real provider's limits are known
---
# Each content limit is one constant, enforced in the spec, the service and the schema (0017)

## Question
The brief asks for content validation (character limits, required fields) without numbers: what are the limits, and how does every layer enforce the same ones?

## Rule
Every limit is declared once in `internal/notification/limits.go`. A test fails if the OpenAPI spec, the validator or a database `CHECK` disagrees with it.

## Context
The brief: "content validation (character limits, required fields)". The provider request has three fields: `to`, `channel`, `content`. Limits are per channel and the brief gives no numbers, so they are ours to choose and state.

## Options
1. Constants plus an agreement test. The API refuses bad input with the field named; the database refuses it even if a future code path skips the validator.
2. Validate in the handler only. Least work; the second writer of the table (a batch path, a replay) can store what the first would refuse.
3. Limits as runtime configuration. Changeable without a release; a database `CHECK` cannot follow a dial, so the last line of defence is lost.

## Decision
Option 1. The numbers are the owner's call; proposed:

| Channel | `to` | `content` |
|---|---|---|
| sms | E.164: `+` and 7 to 15 digits | 1 to 1600 characters (10 segments); alternative: 160, a single segment |
| email | an address, at most 254 characters | 1 to 100,000 characters |
| push | a device token, 1 to 255 characters, opaque | 1 to 1024 characters |

Required on every notification: `to`, `channel`, `content` (or a template, 0015). `priority` defaults to `normal`. Length is counted in characters, not bytes.

## Confirmation
The agreement test above, and one acceptance test per channel at the limit (accepted) and one past it (refused, field named).

## Owner review (15 Nov 2026)
- Every limit stands on a standard or a provider's documented limit, or is derived from one; no arbitrary numbers. The sources go into this record's Numbers table and beside each constant.
- Correction: 1600 is Twilio's maximum message body, not "10 segments". A segment holds 160 GSM-7 characters (153 when concatenated) or 70 UCS-2 characters (67), so 1600 characters is 11 to 24 billed segments.
- Content and recipients that are blank after trimming are refused at every layer (spec, validator, CHECK).
- Push providers limit payload bytes (APNs and FCM: 4096); the session decides between counting bytes and the worst-case character derivation, and states why.
- Built in session limits-standards (branch `feat-limits-from-standards`, on top of the code-first API branch), merged with the rest.
