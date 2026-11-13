---
status: open
created: 2026-11-13
tags: [tests, lint]
---
# The acceptance harness needs os.Environ for its child processes (0003)

## Question
The harness gives `app serve` / `app worker` the test's environment with every `APP_*` removed, then the stack's settings (brief of the intake test-writer). Building that list needs `os.Environ()`, which `.golangci.yaml` forbids everywhere (`forbidigo`, "read settings through internal/config"). How should the exception be recorded?

## Options
1. Inline `//nolint:forbidigo` with a reason on the one line in `test/acceptance/harness_test.go`. Local and visible; the exception lives in a test-writer path.
2. An exclusion rule in `.golangci.yaml` for `^test/acceptance/` and `os\.Environ`, like the existing ones for `cmd/app` and `cmd/fakeprovider`. Central; the coordinator edits config.
3. Give children only an explicit environment (no inheritance). No exception; breaks on Windows, where a child without `SYSTEMROOT`/`TEMP` cannot open sockets or temp files.

## Assumption taken
Option 1. The read is not a setting: it copies PATH/TEMP/Docker variables to the children and strips `APP_*`.

## Answer
