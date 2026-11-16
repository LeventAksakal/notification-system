---
status: closed
created: 2026-11-13
tags: [tests, provider-double]
---
# The provider double does not record the messageId it returned (0005)

## Question
R16 asks that the stored `providerMessageId` equal what the provider returned. `fakeprovider.Request` records the request and its status but not the 202 body (`messageId`), and the double mints a fresh UUID per accepted request. How should an acceptance test learn the returned `messageId`?

## Options
1. Add `MessageID` to `fakeprovider.Request` (filled when the double answers 202). One line in the double; tests read it from `Requests(Filter{Key: id})`.
2. Tests put a recording proxy in front of the double for the channel under test (via `APP_PROVIDER_URL_<CH>`), capturing the raw body sent and the raw 202 answered. No contract change; also proves the body has exactly the keys `to`, `channel`, `content`, which the double's decoder cannot (it ignores unknown fields).
3. Replay the same `Idempotency-Key` to the double with dedupe on and read the remembered body. Works, but adds a request to the counters.

## Assumption taken
Option 2 (`dlRecorder` in `test/acceptance/delivery_helpers_test.go`, used by `TestDeliveryR16ProviderCall`). Option 1 would still be worth having for other tests.

## Answer
Owner, 16 Nov 2026: covered in the 15 to 16 Nov walkthrough. The assumption taken above stands as the answer; closed by the coordinator.
