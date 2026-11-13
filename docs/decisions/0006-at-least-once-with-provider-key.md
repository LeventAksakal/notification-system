---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when a provider offers a status lookup that works at sending volume
---
# Delivery is at-least-once, with an idempotency key sent to the provider (0006)

## Rule
A send whose outcome is unknown (timeout, connection reset) is retried. Every provider request carries `Idempotency-Key: "<notification id>"`, identical across attempts.

## Context
The brief leaves delivery and retry design to us and asks for "idempotency support to prevent duplicate sends". When the provider call times out we cannot know whether the provider accepted the message. The stated provider, webhook.site, does not deduplicate. It can search received requests by header, but at 120 lookups per minute, and a free URL accepts only 50 to 100 requests in total.

## Options
1. At-least-once plus the key. Nothing is lost. A provider that honours the key delivers once; one that does not may deliver twice after a timeout.
2. At-most-once: an unknown outcome becomes `failed` and is never resent. No duplicate is possible; a one-time password lost to one slow response is never delivered.
3. An `unknown` state and a reconciler that asks the provider whether it has the message before resending. The only way to get exactly one delivery without provider-side dedupe; needs a lookup API the stated provider offers only at a trickle.
4. Option 1 or 2 chosen per channel or priority by configuration. Flexible; two behaviours to test and explain.

## Decision
Pending. Recommended: option 1, with the limit stated in the README in one sentence: duplicates are prevented on our side (client retries by 0012, worker crashes by 0018) and on the provider's side only if it honours the key. The provider double implements key dedupe as a switch so both cases are measured, not asserted. Compose points at the provider double by default; `.env.example` shows how to point at a webhook.site URL.

## Confirmation
Fault test: the provider double accepts, then delays its response past our timeout. With dedupe on, end-user deliveries per notification equal exactly 1. With dedupe off, the test records the duplicate count and asserts it never exceeds the attempt limit.
