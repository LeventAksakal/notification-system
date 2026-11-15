---
paths: ["**/*.go"]
---
# Go code

- Packages by domain under `internal/<domain>`; `cmd/app` only wires them. No package named `util` or `common`.
- Errors: sentinel `var ErrX = errors.New(...)` per package for conditions callers branch on; wrap with `fmt.Errorf("doing x: %w", err)` and compare with `errors.Is`; log once, at the boundary that handles it. Map `sql.ErrNoRows` to a typed 404 response, not a 500.
- Concurrency: pass `ctx` as the first parameter; never store it in a struct. Every goroutine is started by something that waits for it (`errgroup` or a `WaitGroup` in the same function) and stops on `ctx.Done()`. Shared state behind one mutex or one owner goroutine, never both.
- HTTP handlers are Huma operations over the types in `internal/httpapi/contract.go`; a new field or parameter changes the contract, so it is the coordinator's change, not a local struct.
- Config comes only from `config`; no `os.Getenv` elsewhere. Every dial has a default, bounds and a doc line (a test enforces it).
- Logging: `slog` with the request's context (`slog.InfoContext(ctx, ...)`) so the trace id is attached. No secrets or message content in logs.
- Rules on every layer: a limit is defined once and checked in the API spec, the service and a DB `CHECK`; a test asserts they agree.
