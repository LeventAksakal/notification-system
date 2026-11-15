---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when a priority gets a delivery deadline of its own
---
# Priority is weighted, not strict: high goes first, low never starves (0010)

## Question
A channel sends at most 95 messages a second; when high, normal and low are all waiting, which ones get those sends?

## Rule
Each second's permits for a channel are split between high, normal and low by configured weights; a share a priority does not use is lent to the next one down, then up.

## Context
The brief: "priority queue support (high, normal, low)" and burst traffic such as flash sales. A channel sends at most 100 per second, so priority decides who gets those 100.

## Options
1. Strict order: `ORDER BY priority, due_at`. One query; a sustained stream of high traffic stops low traffic completely, and nobody is told.
2. Weighted shares with borrowing, for example 70 / 20 / 10. High gets most of the capacity and all of it when the others are empty; low always moves at a known minimum rate. One small claim query per priority, each served by the index `(channel, status, priority, due_at)`.
3. Aging: a waiting notification's effective priority rises with time. Fair in the long run; the ordering is computed, so the claim cannot use the index, and behaviour is hard to predict in a test.
4. A worker pool per priority. Isolated; capacity sits idle whenever one priority is empty.

## Decision
Option 2, split by smooth weighted round robin (the algorithm nginx uses to balance upstreams): a deterministic interleave that gives each priority exactly its share in every cycle of 100 slots, not a random weighted draw, which matches the weights only on average and can leave low with no slot for a while. The weights are dials; whether low should be able to starve is a product choice the owner makes by setting low's weight to 0.

## Confirmation
- 10,000 low notifications queued, then one high: the high one is sent within 2 seconds.
- High traffic saturating a channel for 30 seconds: low still sends at no less than its share.

## As built (14 Nov 2026)
The confirmation run uses 5,000 low notifications, not 10,000 (`TestDeliveryR9Priority`): the high one was sent 213 to 251 ms after its POST.

## Numbers and assumptions (15 Nov 2026)
| Number | Value | Where it comes from |
|---|---|---|
| Weights | 70 / 20 / 10 (`APP_PRIORITY_WEIGHTS`) | **Stated assumption.** The brief names three priorities but no shares. The default gives high most of a channel and guarantees low 10% (9.5 per second at the 95 pacing) during a sustained high burst; it is a dial to tune once real traffic exists. Owner, 15 Nov 2026: fine as a dial with its reasoning stated |
| Low's guaranteed rate | 9.5 per second | Derived: 10% of 95 |
| High-message target | sent within 2 s behind a large low backlog | This record's own target; measured 213 to 251 ms behind 5,000 low (most of it the 200 ms send alignment of 0009, which the adaptive alignment follow-up shrinks) |
| `recentRows` | 1 s | Code constant in `internal/delivery/priority.go`: how soon a priority that just had rows is asked again after the shortest idle poll. Unexplained; the wake-hint change rewrites the idle path |
