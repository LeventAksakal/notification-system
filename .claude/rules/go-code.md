---
paths: ["**/*.go"]
---
# Go code

- Packages by domain under `internal/<domain>`; `cmd/app` only wires them. No package named `util` or `common`.
- Errors: sentinel `var ErrX = errors.New(...)` per package for conditions callers branch on; wrap with `%w`; log once, at the boundary that handles it.
- Concurrency: every goroutine is started by something that waits for it (`errgroup` or a `WaitGroup` in the same function) and stops on `ctx.Done()`. Shared state behind one mutex or one owner goroutine, never both.
- Config comes only from `config`; no `os.Getenv` elsewhere. Every dial has a default, bounds and a doc line (a test enforces it).
- Logging: `slog` with the request's context (`slog.InfoContext(ctx, ...)`) so the trace id is attached. No secrets or message content in logs.
- Rules on every layer: a limit is defined once and checked in the API spec, the service and a DB `CHECK`; a test asserts they agree.
