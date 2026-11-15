---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when a provider offers delivery receipts
---
# A notification moves through one explicit state machine and every attempt is recorded (0005)

## Question
What states can a notification be in, who may move it between them, and what is kept about each try to deliver it?

## Rule
Status changes happen only through `notification.Transition(from, to)`, and every change is a conditional `UPDATE … WHERE status = <from>`. Each provider call writes one `delivery_attempts` row.

## Context
The brief asks for "real-time status tracking", "cancel pending notifications" and visibility "for both internal teams and API consumers". The provider answers 202 "accepted"; it never confirms delivery to the end user.

## Options
1. Stored states `pending`, `processing`, `sent`, `failed`, `cancelled`. A retry returns to `pending` with a later due time and `attempts + 1`. The API additionally shows `scheduled` for a pending notification whose due time is in the future and that has never been tried. Success is called `sent`, not `delivered`, because that is all the provider tells us. Attempts (status code, latency, error, time) go to their own table, which also feeds the metrics endpoint (0014).
2. Three states (`pending`, `sent`, `failed`) plus flags. Fewer transitions; an API consumer cannot tell "queued" from "being sent" from "waiting to retry".
3. Add `delivered` and `retrying` as stored states. Reads well; `delivered` would claim something we cannot know, and `retrying` duplicates what `attempts` and the due time already say.

## Decision
Option 1. Cancel is allowed from `pending` only (including scheduled and waiting-to-retry) and is a conditional update, so a cancel racing a claim has exactly one winner; the loser gets 409.

## Confirmation
A table-driven test covers every (from, to) pair. A race test fires cancel and claim at the same row many times and asserts the provider double received the message if and only if the final status is `sent`.
