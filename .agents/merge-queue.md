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

## Protocol for every lane
1. Finish your work and your verifier on your own branch, as briefed. Do not merge other lanes into your branch.
2. When you're ready, end your turn with the line `READY <branch> <tip>`. The coordinator answers with `REBASE ONTO <integration tip>`. Only one lane rebases at a time, so you never rebase twice.
3. Rebase your own commits onto that tip, never merge: `git rebase --onto <integration tip> <the commit your work started from>`. If you started from the code-first branch, your base is `03bbb6b`. Resolve conflicts in your domain. In someone else's domain, keep their side and adapt your code to it.
4. Generated files are never hand-merged. Delete your branch's migration files, then rerun `task migrate:diff -- <name>` on the rebased schema. Rerun `task gen` for the spec, the client and sqlc.
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
- AsyncAPI contract for the WebSocket, generated from the Go types: starts after realtime-seq lands.
- Renaming files by responsibility: after every lane has landed.
- public-polish: lands last.

## Coordinator follow-ups (for the morning brief)
- provider-health changed `APP_MAX_ATTEMPTS` from 12 to 10. Outages no longer spend the budget (0026), so 10 is the smallest budget that reaches the 5-minute cap. Tell the owner, since they approved 12 in 0007.
- After realtime-seq lands, update the R15 row in `docs/REQUIREMENTS.md`: `/readyz` checks MySQL only, and while Valkey is down `/v1/ws` answers 503 and open streams close with 1012.
- Flaky on a loaded host, to re-run on a quiet one: `TestWorkersAcrossSmallLimiterClockSteps` (also on main under `-race`), and the delivery rate floor `TestWorkersHoldTheRateAtTheProvider`.
- `test/acceptance/delivery_wake_test.go:42` uses `n.ID` where the generated client field is `Id`; `go vet -tags integration ./test/...` fails on integration. Test-writer fixes it with the held tests.
- Realtime commit `46aefb6` doesn't build on its own; squashing covers it.
