---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when a priority gets a delivery deadline of its own
---
# Priority is weighted, not strict: high goes first, low never starves (0010)

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
Pending. Recommended: option 2. The weights are dials; whether low should be able to starve is a product choice the owner makes by setting low's weight to 0.

## Confirmation
- 10,000 low notifications queued, then one high: the high one is sent within 2 seconds.
- High traffic saturating a channel for 30 seconds: low still sends at no less than its share.
