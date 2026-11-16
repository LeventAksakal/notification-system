---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when a provider offers a status lookup that works at sending volume
---
# Delivery is at-least-once, with an idempotency key sent to the provider (0006)

## Question
When we cannot tell whether the provider received a message (a timeout, a dropped connection), do we send it again?

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
Option 1, with the limit stated in the README in one sentence: duplicates are prevented on our side (client retries by 0012, worker crashes by 0018) and on the provider's side only if it honours the key. The provider double implements key dedupe as a switch so both cases are measured, not asserted. Compose points at the provider double by default; `.env.example` shows how to point at a webhook.site URL.

One consequence found while building the provider double (13 Nov): Go's `http.Transport` treats a POST that carries `Idempotency-Key` as safe to replay, and resends it once by itself when a reused connection dies before any response byte. So one attempt in our records can be two requests at the provider. With a provider that honours the key this is harmless; without one it is a further possible duplicate that our attempt count never shows. The sender disables the replay: `internal/provider/send.go` clears `GetBody`, so one attempt is exactly one request, and a lost request surfaces as a transient error that the worker retries and records.

## Confirmation
Fault test: the provider double accepts, then delays its response past our timeout. With dedupe on, end-user deliveries per notification equal exactly 1. With dedupe off, the test records the duplicate count and asserts it never exceeds the attempt limit. "Sent once" is asserted on requests per key at the double, because with dedupe on its deliveries per key are 1 by construction.
