---
paths: ["**/*_test.go", "test/**"]
---
# Tests

- Table-driven with `t.Run(name, ...)`; names describe the behaviour, not the function.
- Tests check outcomes a user or operator can observe (HTTP response, row state, deliveries counted by the fake provider), never private fields or log text.
- `test/acceptance/` is written by `test-writer` before the code and is read-only for everyone else; each test cites its requirement row in `docs/REQUIREMENTS.md`.
- Concurrency code gets a `-race` run and `goleak.VerifyNone(t)`; time-dependent code takes a clock, never `time.Sleep` in assertions.
- Integration tests use real MySQL and Valkey (testcontainers or the compose stack), not mocks of them.
