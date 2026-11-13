# CLAUDE.md

Router, not a manual. Every line is something you could not work out from the code.

## Start here
- The task, its requirements (each mapped to a test) and where work stopped: `docs/REQUIREMENTS.md`.
- Decisions in force: `docs/decisions/README.md` (accepted only; never follow a file under `superseded/`).
- Owner items you raise: `.agents/owner-items/NNNN-<slug>.md` (question, options, the assumption you took). Never stop to ask; take the assumption and continue.
- Work log: `.agents/logs/YYYY-MM/DD.md`, one line per pivot or mistake caught.

## Commands (all via Go, no other installs)
- `lefthook install` once per clone: pre-commit runs gofmt, gitleaks and go vet on staged files.
- `go tool -modfile=tools.mod task --list` lists every command. Alias it: `T="go tool -modfile=tools.mod"`.
- While iterating: `go test ./internal/<pkg> -run <Test>`; the narrowest check that can fail.
- `task gen`: regenerate API (oapi-codegen) and SQL (sqlc) code. Never hand-edit `*.gen.go` or `internal/store/sqlc/`.
- `task migrate:diff -- <name>`: write the next migration from `schema/*.sql`. Never hand-write `migrations/`.
- `task check`: the full gate (build, vet, lint, tests, vuln). Once, before merge, by the coordinator.
- `docker compose up --wait`: the whole stack; `docker compose logs app`.

## Tools agents can use
- Telemetry: Grafana at http://localhost:3000 (otel-lgtm); `mcp-grafana` when configured.
- Docs: context7 MCP for library docs. `go doc <pkg>.<Symbol>` for the standard library.

## Go traps agents fall into here
- Pass `ctx` as the first parameter; never store it in a struct. Every goroutine has an owner that waits for it.
- Wrap errors with `fmt.Errorf("doing x: %w", err)`; compare with `errors.Is`. Map `sql.ErrNoRows` to a typed 404 response, not a 500.
- Use the generated strict-server types; if the spec lacks a field, that is a contract change for the coordinator, not a local struct.
- MySQL readiness must be checked over TCP (`--protocol=tcp`); the first-boot server answers socket pings with networking off.

## Roles
Subagents: `researcher`, `test-writer`, `implementer`, `verifier` (`.claude/agents/`). The main thread coordinates and alone edits contracts (`api/`, `schema/`, `internal/core/`). Limits are enforced by `tools/agentguard` and at merge by `tools/ownercheck`; a deny message says what to do instead.
