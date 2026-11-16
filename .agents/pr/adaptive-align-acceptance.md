# Acceptance tests held for adaptive send alignment (guard: owner item 0002)

`test/acceptance/` is written by the test-writer only. These are the tests this lane would add there; the coordinator has the test-writer write them after the lane lands. The same scenarios already run as integration tests inside `internal/delivery` (named below), so the acceptance versions only move the proof to the black-box suite.

## 1. The rate holds while claims are slow (R8, owner item 0011)

- **Name:** `TestGapDeliveryR8RateWhileClaimsAreSlow`
- **Requirement row:** R8.
- **Setup:** the owner-item-0011 scenario: 300 sms, 900 email, 900 push, three workers, the sms provider hanging (fault `delay` 2000 ms, provider timeout 500 ms). Every claim held 1.6 s before it reaches MySQL. The acceptance stack has no hook to slow MySQL, so this needs one of: a proxy in front of MySQL that delays `SELECT ... FOR UPDATE SKIP LOCKED` (toxiproxy, latency toxic on the queue connection only), or a test-only queue decorator in the worker binary behind a build tag. The in-package test uses a decorator: `slowQueue` in `internal/delivery/slow_claim_integration_test.go`.
- **Asserts:** every sliding second at the provider holds at most 100 requests per channel (always); email and push average at least the sanity floor the suite uses for timing-dependent throughput (`ACCEPTANCE_RATE_FLOOR`, default 60 here) from first to last request.
- **Why it matters:** with the fixed 200 ms alignment this scenario sent 0 of 1800 in 60 s.

## 2. A row right behind a lone send is not held behind unused slots (R7 tail)

- **Name:** `TestGapDeliveryR7SecondRowAfterALoneSend`
- **Requirement row:** R7 (first send after idle reaches the provider promptly), the scheduled-tail case named in the wake-hint PR.
- **Setup:** compose or in-process stack, three workers, idle channel. POST one notification; as soon as the provider has it, POST a second on the same channel.
- **Asserts:** the second reaches the provider within 100 ms of its POST (p99 over 30 pairs), where before the give-back it waited behind the first group's unused reservation (about 200 ms plus the alignment).
- **In-package version:** `TestARowRightAfterALoneSendIsNotHeldBehindUnusedSlots` (`internal/delivery/wake_test.go`).

## 3. The lead follows the claim time down after a slow spell (R8, record 0009)

- **Name:** `TestGapDeliveryR8LeadRecoversAfterSlowClaims`
- **Setup:** as test 1, with claims slowed to 1.6 s for the first 10 s and then not slowed.
- **Asserts:** within 5 s of the slowing ending, with a low backlog still draining, a new high-priority notification reaches the provider within 500 ms of its POST (healthy runs: 336 to 414 ms, `TestDeliveryR9Priority`), which needs the lead back at its 200 ms floor (a lone send on an idle channel leaves right after its claim whatever the lead, so it cannot show this); the cap holds throughout.
- **In-package version:** the rule is unit-tested in `TestClaimLatencyLead` ("a slow spell is forgotten after nineteen fast claims"); the Take spacing that makes it about 2 s is `refill(APP_RATE_LIMIT_HORIZON)` in `channelLoop`.
