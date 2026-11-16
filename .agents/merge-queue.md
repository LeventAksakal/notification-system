# Merge queue (coordinator, 16 Nov 2026)

Integration branch: `refs/heads/integration/decision-review`, checked out in `C:\Code\notification-system`. Only the coordinator commits to it. It is local and never pushed until the final gate passes.

## Landed (squashed, one commit per lane)
| Tip | Lane |
|---|---|
| 2376699 | Coordinator's walkthrough: no default role, 12 retry attempts plus `RetrySpan`, records 0002 to 0019 |
| 1b74a74 | wake-hint (0003) |
| d580c4e | huma-code-first (0004 → 0024): Huma owns the HTTP contract, `task gen` writes `api/openapi.yaml` and `test/apiclient` |
| 6873097 | provider-health (0008 → 0026): shared outage state; `APP_MAX_ATTEMPTS` 10 |
| af16e3a | trace-correlation (0019): sampler env vars, stored correlation id on every line |
| 5eb668f | db-rules-retention (0005, 0025): state machine in MySQL, write-ahead attempts, retention |
| 34bc20d | reschedule (0016): PATCH reschedule, horizon on the database clock |
| 341630c | realtime-seq (0013): status_seq ordering, MySQL-only readiness, 503/1012 in the contract |
| c488661 | template-versioning (0015): versions with If-Match, detailed variables error, template origin on notifications |
| aaeb893 | adaptive-align (0009): alignment follows measured claim latency |
| ce1fd26 | limits-standards (0017): every limit cited, blank content refused at every layer |
| efd3d6b | metrics-rollup (0014): per-minute rollups and latency buckets |
| f7bef32 | dials-derived (0018): every dial derived, config cross-checks |
| 3c2d59e | R7 second-row fix (adaptive-align): given-back slots return once later ones are back; R7 p99 18.9 ms (was 387) |
| e7f1053 | public-polish: README without a directory tree, merged contract described; GitHub metadata drafted in `.agents/pr/public-metadata.md` (owner applies) |
| 25176b4 | attempt rows over requests (adaptive-align): a call of which no byte left is released, not recorded (load-driven timeouts; not caused by 3c2d59e) |
| (this commit) | not-sent follow-up (adaptive-align): a failed dial stays a recorded provider fault; TLS counted above the handshake; HTTP/2 off; verifier findings on 25176b4 fixed |

## Protocol for every lane
1. Finish your work and your verifier on your own branch, as briefed. Do not merge other lanes into your branch.
2. When you're ready, end your turn with the line `READY <branch> <tip>`. The coordinator answers with `REBASE ONTO <integration tip>`. Only one lane rebases at a time, so you never rebase twice.
3. Rebase your own commits onto that tip, never merge: `git rebase --onto <integration tip> <the commit your work started from>`. If you started from the code-first branch, your base is `03bbb6b`. Resolve conflicts in your domain. In someone else's domain, keep their side and adapt your code to it.
4. Generated files are never hand-merged. If `task gen` fails on a stale sqlc querier, run it again: it compiles store before regenerating it.
    Delete your branch's migration files, then rerun `task migrate:diff -- <name>` on the rebased schema. Rerun `task gen` for the spec, the client and sqlc.
5. Run your scoped checks (build, vet, your packages' tests, the integration tests you touch, pinned to two cores), commit, and end your turn with `REBASED <branch> <new tip> <checks with result and duration>`. The coordinator squash-merges it and runs the full gate once at the end.

## Shared files and who goes first
- `schema/notifications.sql`, `internal/store`: db-rules-retention, then realtime-seq, metrics-rollup, template-versioning, limits-standards.
- `config/config.go`, `docs/briefs/CONTRACT.md`: provider-health and adaptive-align first, then trace-correlation, and dials-derived last (it documents every dial).
- `internal/delivery`: provider-health, then adaptive-align, then dials-derived.
- The HTTP contract (`internal/httpapi/api.go`, `contract.go`): template-versioning, reschedule, limits-standards. Add operations in a file named for them, and keep edits to the shared registration small.
- `docs/decisions/README.md`, `.agents/logs/*`: append-only. On a conflict keep both sides.
- Don't rename existing files. A renaming pass by responsibility runs after every lane has landed.

## Acceptance tests blocked by the guard (owner item 0002)
Don't route around the guard. Write the tests you would add to `.agents/pr/<lane>-acceptance.md`, as test names, what each asserts, and code if you have it. After your lane lands, the coordinator has the test-writer write them in the main checkout, where the guard allows it.

## Waiting for later lanes
- AsyncAPI contract for the WebSocket, generated from the Go types: realtime-seq has landed. A session can't be started from an automated tick, so a coordinator subagent builds it in the main checkout after the last code lane lands, before the renaming pass.
- Renaming files by responsibility: after every lane has landed.
- public-polish: lands last.

## Coordinator follow-ups (for the morning brief)
- provider-health changed `APP_MAX_ATTEMPTS` from 12 to 10. Outages no longer spend the budget (0026), so 10 is the smallest budget that reaches the 5-minute cap. Tell the owner, since they approved 12 in 0007.
- After realtime-seq lands, update the R15 row in `docs/REQUIREMENTS.md`: `/readyz` checks MySQL only, and while Valkey is down `/v1/ws` answers 503 and open streams close with 1012.
- Flaky on a loaded host, to re-run on a quiet one: `TestWorkersAcrossSmallLimiterClockSteps` (also on main under `-race`), and the delivery rate floor `TestWorkersHoldTheRateAtTheProvider`.
- `test/acceptance/delivery_wake_test.go:42` uses `n.ID` where the generated client field is `Id`; `go vet -tags integration ./test/...` fails on integration. Test-writer fixes it with the held tests.
- adaptive-align reports lint failures in other lanes' code on the integration tip; fix at the gate.
- For the owner: `.claude/agent-policy.json` should list `api/asyncapi.yaml` under `generated`, so the guard refuses hand edits, as it does for `openapi.yaml`. That is a config change, left to the owner. AsyncAPI is split: an implementer writes the builder under `internal/`, and the coordinator does the contract wiring, because `general-purpose` has no guard profile.
- Held acceptance tests landed in f6497d1 (47 of 48 pass). `TestGapDeliveryR7SecondRowAfterALoneSend` fails on three workers (p99 387 ms against 100 ms); fixed by adaptive-align (`claude/fix-r7-second-row`, squash-merged 16 Nov): p50 12.3 ms, p99 18.9 ms; rate caps hold (busiest second at most 97, R8 at 94 to 95/s). `GiveBack`'s doc comment in `ports.go` changed (doc only).
- Before the gate, update these REQUIREMENTS rows: R15 (readiness without Valkey), B3 (template versions), a new O1 row (retention), R11 evidence (CallOutlivesLease replaced by SlowCallEndsInsideLease), R13 (rollups), B1 and R12 (shared health, budget_spent).
- Realtime commit `46aefb6` doesn't build on its own; squashing covers it.

## Owner answers (16 Nov 2026, 09:10)
- Keep `APP_MAX_ATTEMPTS` at 10 (decision 0026).
- REQUIREMENTS row on the VS Code profile reworded: no local path, company line or extension authors (git history keeps the old text).
- No licence: all rights reserved for the submission.
- `integration/decision-review` lands on `main` as one squash commit (f6497d1 failed an acceptance test, so the commits are not all atomic).
- History rewrite (drop Co-Authored-By trailers and private quotes): skipped. Every hash cited in docs and commit messages would dangle and the force push to `main` is denied by the agent policy; the tip is clean.
