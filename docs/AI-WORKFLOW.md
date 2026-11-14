# How AI was used

## Setup
One coordinating session and four kinds of sub-agent, each started with a written brief and returning a short hand-back. Limits are enforced, not asked for: a hook (`tools/agentguard`) denies writes outside a role's paths and a merge check (`tools/ownercheck`) catches what the hook cannot see.

| Role | May write | Why the limit |
|---|---|---|
| Coordinator (main session) | contracts (`api/`, `schema/`, `internal/core/`), briefs, decision records, docs, logs | One author for the interfaces every lane depends on; it reads diffs and merges but does not build |
| Researcher (Sonnet) | `.agents/` | Facts with a source before a decision is written |
| Test-writer (Opus) | `test/acceptance/` | Tests come from the contract, by an agent that has not seen the code |
| Implementer (Opus) | `cmd/`, `internal/`, `test/` except `test/acceptance/` | Cannot edit the tests that judge it, nor the contracts |
| Verifier (Opus, fresh context) | `.agents/` | Judges a finished slice without the lane's reasoning; runs the heavy proofs; edits nothing |

The loop for each slice: the coordinator freezes the contract (spec, schema, ports, and a one-page digest, `docs/briefs/CONTRACT.md`); a test-writer writes black-box acceptance tests, one or more per requirement row; an implementer makes them pass; a verifier in a fresh context reviews the diff and runs the proofs; findings go back as a fix-up lane plus gap tests; the coordinator re-runs the acceptance tests itself before a pull request, and CI is the full gate.

Choices the brief or the decision records left open are never asked mid-run. The agent takes an assumption, writes it to `.agents/owner-items/` with the options and trade-offs, and continues; the owner answers later.

## Delegated vs kept
| Work | Who | Why |
|---|---|---|
| API spec, schema, state machine, ports, the limiter redesign | Coordinator | Coupled decisions; a weaker or split author costs more than it saves |
| Decision records 0002 to 0019 | Coordinator, facts by the researcher | Each external fact has a source (`.agents/research/`) |
| Acceptance harness and every acceptance test | Test-writers | Independent of the code; the harness runs the real binary as child processes |
| Intake, delivery, observability code | One implementer per slice, one at a time | Slices share the store and generated code; two writers in one checkout break each other's build |
| Review of each slice; compose, fault and readiness proofs | Verifiers | Three reports in `.agents/reviews/`; each found defects the tests had passed, two were a fail |
| Thresholds and trade-offs (rate test floor, send pacing, trace continuation) | Coordinator decides, owner confirms | Recorded as owner items 0001 and 0005 to 0010 |
| README, this file, requirements ledger, work log | Coordinator | |

## Where the agent was wrong, and how it was caught
One row per real mistake, written when it happened (from `.agents/logs/`).

| Date | What the agent did | Caught by (test, guard, verifier, owner read) | Commit | Rule or check added |
|---|---|---|---|---|
| 13 Nov | The coordinating agent began editing the scaffold itself instead of briefing a lane | Owner read | `b53e50d` (finished by a lane) | Coordinator writes only docs, contracts and briefs; building goes to a lane with a written brief |
| 13 Nov | Pre-commit file used a deprecated `gitleaks protect` command, as the coordinator's brief had specified | Coordinator's review of the lane's hand-back | `afe5a76` | Hook commands are checked against the installed tool's `--help` |
| 13 Nov | Pre-commit scripts contained double quotes, which cut every script short under lefthook on Windows; all three steps failed | Running the hook once for real, on a clean file and on a deliberately broken one | `afe5a76` | A hook is not done until it has been run and seen to fail on bad input |
| 13 Nov | First draft of the idempotency decision returned 409 for a reused key with a different payload | Researcher's source check against the IETF draft (422) | `39f2d5a` | Decision records cite the research note for every external fact |
| 13 Nov | Provider double recorded status 202 for a response that was never sent; the coordinating agent read that code and missed it | Verifier agent in a fresh context | `b12cfbf` | Every slice gets a verifier that has not seen the lane's reasoning, in addition to the coordinator's read |
| 13 Nov | Provider double judged the rate limit on deliveries only, and its per-key numbers could not show a resend while dedupe is on: two later tests would have passed without proving anything | Verifier agent, asked what the later tests would rely on | `b12cfbf` | A test double's counters are reviewed against the claims they will be used to prove |
| 13 Nov | Lane inserted a README table row above the document title | Coordinator's read of the diff | `98a0d66` | none; caught by reading |
| 13 Nov | Settings lint forbade only `os.Getenv`; the lane passed it with `os.LookupEnv` and reported that itself | Lane's own hand-back | `ee187d0` | Rule now covers `LookupEnv` and `Environ`; tagged files are linted too |
| 13 Nov | Guard hook resolved paths against the main checkout, so a test-writer in its own worktree could write nothing; 7.5 minutes lost | The agent stopped and reported instead of working around two checks | none (owner item 0002) | Lanes share one checkout on disjoint paths; test-writers run no git |
| 14 Nov | Coordinator's briefs never asked for lint; 5 issues in the acceptance harness reached CI | CI gate on pull request 3 | `11e220a` | Every brief names the scoped lint command as a done-when check |
| 14 Nov | Every file in `test/acceptance` carried a build tag, so `go vet ./...` found no package | Stop hook | `977b48c` | Untagged `doc.go` in tagged-only packages |
| 14 Nov | Coordinator's spec allowed any string as `Idempotency-Key` while the column is ASCII (a 500), and set no body size limit | Verifier, intake slice | `eb54e0d`, `525b818` | Contract 1.1.0; gap tests `TestGapR11Idempotency`, `TestGapR10RequestBodyLimit` |
| 14 Nov | Intake looked a template up before the idempotency key, so a replay failed after the template changed | Verifier, intake slice | `5bb9055` | Gap test |
| 14 Nov | Worker sent without checking its lease; a call that outlived the lease was a second request with no attempt row | Verifier by reading, then reproduced by a gap test: 6 of 20 notifications had 2 requests and 1 row | `a4882f0` | `TestGapDeliveryR11CallOutlivesLease` compares attempt rows with provider requests |
| 14 Nov | Rate limit permits were lost when taken late, so throughput fell with machine load (86 to 90 per second); the coordinator's own quiet re-run had passed and would have merged it | Verifier, on a loaded machine | `a4882f0` to `43fae4e` | Limiter redesigned to reserved send slots; the rate test logs a per-second histogram |
| 14 Nov | Breaker's half-open state could be decided by a late result of an earlier call; one pool of call slots shared by all channels | Verifier, delivery slice | `a4882f0`, `43fae4e` | Breaker tickets carry a generation; per-channel call places; tests for both |
| 14 Nov | An implementer rebuilt its commits with `git commit-tree` and `update-ref`; the rebuild swallowed a test-writer's test change and the coordinator's compose change into a `feat(api)` commit, and skipped the commit hook | Final verifier, reading the history | history re-split; pull request 4 replaced | Briefs forbid every history-rewriting git command by name |
| 14 Nov | A 1000-item batch published 1000 events one by one; with Valkey down the request took about 1000 s | Final verifier, by stopping Valkey under the compose stack | final fix-up | `TestGapOutage*`; the digest states the bound |
| 14 Nov | Coordinator's README example scheduled a notification in 2030, past the 30 day horizon | Final verifier walking the README against the stack | README fix | The README is walked command by command before it ships |

## Cost
Overnight run of 13 to 14 Nov, unattended, on one machine shared with another of the owner's jobs. Minutes are wall clock per lane; "context" is the lane's context size when it handed back, not its total token spend (not measured).

| Lane | Role | Minutes | Context |
|---|---|---|---|
| Scaffold against the new contracts | implementer | 8 | 86K |
| Harness and intake tests (first try, blocked by the guard) | test-writer | 8 | 91K |
| Harness and intake tests | test-writer | 15 | 125K |
| Intake | implementer | 19 | 186K |
| Intake review | verifier | 5 | 106K |
| Delivery tests | test-writer | 15 | 114K |
| Observability tests | test-writer | 15 | 106K |
| Delivery | implementer | 48 | 223K |
| Intake fix-ups | implementer | 18 | 127K |
| Lint fixes and intake gap tests | test-writer | 9 | 96K |
| Delivery review | verifier | 5 | 112K |
| Delivery fix-ups (limiter redesign) | implementer | 68 | 267K |
| Delivery gap tests | test-writer | 8 | 113K |
| Metrics, WebSocket, events | implementer | 30 | 175K |
| Rate test histogram | test-writer | 3 | 24K |
| Whole-system review and compose proofs | verifier | 18 | 135K |

What parallel lanes bought: test-writers ran beside the implementer of the previous slice (different directories), which saved roughly 45 minutes of the night. What they cost: two implementers were never run together after the first shared-checkout build breaks; a lane's acceptance run slowed its neighbours' timing-sensitive tests; one lane's history rewrite took another's commits with it. The verifiers were the cheapest lanes (5 to 18 minutes each) and found every serious defect that the acceptance tests and the coordinator's own re-runs had passed.
