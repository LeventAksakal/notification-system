---
status: open
created: 2026-11-16
tags: [tests, acceptance, rate-limit, harness]
---
# The acceptance harness cannot slow claims, so two R8 tests stay in-package (0018)

## Question
The adaptive-align lane held three acceptance tests (`.agents/pr/adaptive-align-acceptance.md`). Two of them, `TestGapDeliveryR8RateWhileClaimsAreSlow` and `TestGapDeliveryR8LeadRecoversAfterSlowClaims`, need every claim (`SELECT ... FOR UPDATE SKIP LOCKED`) held 1.6 s before it reaches MySQL, and nothing else slowed. The black-box harness has no such hook. Which one should it get?

## Context
The harness (`test/acceptance`) starts `app worker` as a child process against testcontainers MySQL and can only put a TCP proxy in front of a dependency. A proxy that delays packets slows every query of the worker (attempt writes, results, the reaper), not only claims, so it measures a different scenario. The in-package versions pass: `slowQueue` in `internal/delivery/slow_claim_integration_test.go` (rate while claims are slow) and `TestClaimLatencyLead` (the lead recovers). The third held test, `TestGapDeliveryR7SecondRowAfterALoneSend`, needs no hook and is written.

## Options
1. A test-only queue decorator in the worker binary behind a build tag (`-tags acceptance_slowclaim`), switched on by a variable read through `config`. Exact (claims only); a second binary build in TestMain and a dial that exists only in test builds.
2. A MySQL protocol-aware proxy (toxiproxy cannot tell statements apart) that delays only `FOR UPDATE SKIP LOCKED` statements. No product change; a parser of the MySQL wire protocol in the test tree.
3. Leave both tests in-package. No work; R8's evidence for slow claims stays white-box.

## Recommendation
Option 3 until a slow-claim incident is seen outside tests; option 1 if the owner wants the evidence black-box.

## Assumption taken
Option 3: the two tests are not written in `test/acceptance`.

## Answer
