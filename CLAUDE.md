# CLAUDE.md

Router, not a manual. Every line is something you could not work out from the code.

## Start here
- The task, its requirements (each mapped to a test) and where work stopped: `docs/REQUIREMENTS.md`.
- Decisions in force: `docs/decisions/README.md` (accepted only; never follow a file under `superseded/`).
- Owner items you raise: `.agents/owner-items/NNNN-<slug>.md` (question, options, the assumption you took). Never stop to ask; take the assumption and continue.
- Work log: `.agents/logs/YYYY-MM/DD.md`, one line per pivot or mistake caught.

## Commands (all via Go; lefthook and gitleaks are separate, optional installs)
- `lefthook install` once per clone: pre-commit runs gofmt, gitleaks and go vet on staged files. commit-msg holds the subject to Conventional Commits (`tools/commitcheck`; CI checks every commit of a pull request).
- `go tool -modfile=tools.mod task --list` lists every command. Alias it: `T="go tool -modfile=tools.mod"`.
- While iterating: `go test ./internal/<pkg> -run <Test>`; the narrowest check that can fail.
- `task gen`: regenerate API (oapi-codegen) and SQL (sqlc) code. Never hand-edit `*.gen.go` or `internal/store/sqlc/`.
- `task migrate:diff -- <name>`: write the next migration from `schema/*.sql`. Never hand-write `migrations/`.
- `task check`: the full gate (build, vet, lint, tests, vuln). Once, before merge, by the coordinator.
- `docker compose up --wait`: the whole stack; `docker compose logs app`.

## Tools agents can use
- Telemetry: Grafana at http://localhost:3000 (otel-lgtm); `mcp-grafana` when configured.
- Docs: context7 MCP for library docs. `go doc <pkg>.<Symbol>` for the standard library.

## Traps agents fall into here
- Go-specific traps live in `.claude/rules/go-code.md` (loaded with `.go` files).
- MySQL readiness must be checked over TCP (`--protocol=tcp`); the first-boot server answers socket pings with networking off.

## Numbers and names (owner, 15 Nov 2026)
- No unexplained number. Every dial, constant, timeout, window, ratio or test floor says where it comes from: derived from a stated target, measured, or a named industry default. If it is arbitrary, say so and say how to tune it (a stress test, a measured percentile, an optimisation metric). Put the reason in the dial's doc line or beside the constant, and in the decision record's Numbers table.
- Files are named for what they do (`claim_loop.go`, `retry_budget.go`), never for the package's topic (`delivery.go`, `service.go`, `handlers.go`).

## Pushing
- CI minutes are billed (private repository). Batch commits locally, run the gate locally (`task check`, then `task test:integration`; pin a test to two cores to imitate the runner), and push once per change set. Never push to see what CI says.

## Merging
- Fast-forward `main` to the PR branch locally (`git merge --ff-only`, then `git push origin main`; GitHub marks the PR merged). Squash-merge a PR too large to read commit by commit. Never a merge commit, never GitHub's "Rebase and merge" (it rewrites the commits CI tested), never rebase a pushed branch.

## Roles
Subagents: `researcher`, `test-writer`, `implementer`, `verifier` (`.claude/agents/`). The main thread coordinates and alone edits contracts (`api/`, `schema/`, `internal/notification/`). Limits are enforced by `tools/agentguard` and at merge by `tools/ownercheck`; a deny message says what to do instead.
