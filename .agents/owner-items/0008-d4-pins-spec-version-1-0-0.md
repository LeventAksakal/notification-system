# 0008 TestD4 pins the spec version to 1.0.0 (raised by the intake fix-up implementer)

Seen: `test/acceptance/stack_test.go:54`

    if info, _ := spec["info"].(map[string]any); info["version"] != "1.0.0" {

fails with `/openapi.json info.version: want 1.0.0, got 1.1.0` since the coordinator's
commit eb54e0d bumped `api/openapi.yaml` to 1.1.0. The intake fix-up brief requires
"Run `go tool -modfile=tools.mod task gen` so `internal/api/api.gen.go` matches spec
1.1.0", so the served spec is 1.1.0 by design.

Options: (a) the test compares against the version in `api/openapi.yaml` (read the file);
(b) pin 1.1.0 and bump it on every contract change.
Assumption taken: the test is stale; the code serves the embedded 1.1.0 spec unchanged.
Owner: test-writer (only they edit `test/acceptance/`).
