# Merge queue (coordinator, 16 Nov 2026)

Integration branch: `refs/heads/integration/decision-review`, checked out in `C:\Code\notification-system`. Only the coordinator commits to it. It is local and never pushed until the final gate passes.

## Landed (squashed, one commit per lane)
| Tip | Lane |
|---|---|
| 2376699 | Coordinator's walkthrough: no default role, 12 retry attempts plus `RetrySpan`, records 0002 to 0019 |
| 1b74a74 | wake-hint (0003) |
| d580c4e | huma-code-first (0004 → 0024): Huma owns the HTTP contract, `task gen` writes `api/openapi.yaml` and `test/apiclient` |

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
