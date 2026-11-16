---
status: closed
created: 2026-11-14
tags: [api, metrics, list, scale]
---
# Two read paths grow with the table, not with the page (0012)

## Question
Two reads are served by an index but still walk more rows than they return. Both are fast at today's sizes. Do they need a bound before this is exposed to real clients?

## Context
Measured 14 Nov on the compose stack with 49,000 sent and 24,000 scheduled rows:

| Request | Time | Why |
|---|---|---|
| `GET /v1/metrics` (60 s window) | 0.15 to 0.18 s | reads the last minute of attempts |
| `GET /v1/metrics?windowSeconds=3600` with 45,000 attempts in the hour | 0.9 to 1.8 s | percentiles over every attempt in the window; uncached |
| `GET /v1/notifications?status=pending` with 24,000 scheduled rows | 0.26 to 0.33 s | `scheduled` is a view of `pending`; the filter walks every pending row in `list_status_idx` and fetches it to test the due time |
| every other list filter, and cursor pages | 0.01 to 0.04 s | |

At a sustained 300 messages per second an hour holds a million attempts, which puts the hour-wide metrics call near half a minute. A wider index (`status, id, attempts, due_at`) halved the pending scan in a trial (77 ms to 35 ms for the same rows) but it stays linear.

## Options
1. Leave as built; the README already says to put the metrics endpoint behind authentication and rate limiting. Trade-off: one client asking for an hour-wide window can load the database.
2. Cache the metrics answer for one second per window (what decision 0014 first recommended) and cap `windowSeconds` lower (for example 900). Trade-off: numbers up to a second old; a test that reads its own writes must wait a second.
3. Store `scheduled` as a real status so its filter is an index range. Trade-off: one more state and transition in the state machine and a migration.

## Assumption taken
Option 1. Nothing changed.

## Answer
Owner, 16 Nov 2026: covered in the 15 to 16 Nov walkthrough. The assumption taken above stands as the answer; closed by the coordinator.
