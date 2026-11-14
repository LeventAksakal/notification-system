# Submission check: fresh-context reviewer (14 Nov 2026, on `fix/ci-rate-floors-qa` at `300a4b1`)

Saved by the coordinator from the reviewer's hand-back (the verifier role cannot write files). The role's guard also refused `curl`, so the reviewer could not walk the README against the stack; the coordinator did that afterwards (last row).

Verdict: no blocker; six should-fix items, the first a defect.

| # | Severity | Finding | What was done |
|---|---|---|---|
| 1 | should fix | An idle compose stack logged WARN "limiter clock stepped" every 30 s on every worker (Docker Desktop corrects the VM's wall clock in small steps); since `ea459a8` each such restart voided the worker's pending sends. Likely why the new binary measured 89 to 92 per second where the old one reached 97 | Fixed in `ef6c956`: only a forward step of one interval or more voids. Idle stack: no warning in 100 s; drain numbers in row R8 |
| 2 | should fix | The README's test command can miss the strict throughput floors on a loaded laptop | README names `ACCEPTANCE_RATE_FLOOR` for that case |
| 3 | should fix | README said "never more than 100"; the ledger records 106 once | README reworded |
| 4 | should fix | Row B6 said passing while its text said `main` is red; "Where it stopped" named the PR branch | Rewritten after the branch's CI run |
| 5 | should fix | Ten commit hashes in `docs/AI-WORKFLOW.md` were from before the rebase merges and are not on `main` | Replaced with the hashes on `main` |
| 6 | should fix | `docs/decisions/README.md` lists only record 0001; 0002 to 0019 are `proposed` | The file now says why; accepting them is the owner's step |
| 7 | cosmetic | The ledger's first section reads as a process log (owner quotes, machine leftovers) | Kept: the owner's rules ask for the ledger of what they raised, in their words |
| 8 | cosmetic | `.env.example` is tracked and unchecked (agents may not read it) | Open for the owner |
| 9 | cosmetic | `docs/briefs/CONTRACT.md` said spec 1.2.0 | 1.3.0 |
| 10 | cosmetic | QA evidence could not be reproduced from the repo | Scripts committed under `tools/qa/` |
| 11 | cosmetic | Row B5's evidence was thinner than its claim | Row says what the test checks and what was checked by hand |
| 12 | cosmetic | `.gitignore` missed `*.exe`, `*.test`, `coverage.out` | Added |
| 13 | cosmetic | Commit `300a4b1` carries two types in its subject | Left: the merge is a fast-forward |

Checked and correct: 64 acceptance tests as stated and every test name cited in the ledger exists; all 31 `APP_*` settings in the README match `internal/config` both ways; CI does what the README says; eight decision records carry "As built"; every cited path exists; no TODO, debug print or commented-out code in non-test Go; `go vet ./...` clean; compose: all eight services up.

Not checked by the reviewer: README examples against the stack, `/docs`, WebSocket, Grafana (no allowed tool); `.env.example`; CI status.

README walked by the coordinator against the same stack (ports translated): create returns `pending` with the correlation id; two seconds later `sent` with a provider message id and one delivery attempt; batch of two returns an id and both are `sent`; template stored; the templated, scheduled notification is `scheduled` with the rendered text; cancel answers `cancelled`; the filtered list, the metrics, the failed list answer 200; retry of a cancelled notification answers 409; the provider's fault control answers 204 for PUT and DELETE; `/healthz`, `/readyz`, `/docs`, `/openapi.json` (1.3.0) and Grafana's health answer 200. The WebSocket example was run earlier the same day with `tools/qa/ws.mjs` (scheduled, processing, sent in order), not again here.

Found while checking finding 1 on the stack: the three workers exported metrics without an instance id, so their counters overwrote each other in Prometheus. Fixed in `5ac1f68`.
