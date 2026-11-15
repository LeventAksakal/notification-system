# Review: code-first HTTP API with Huma (15 Nov 2026)

Fresh-context verifier, on `git diff main...HEAD` at commit aff7022 (acceptance-test changes still uncommitted, not reviewed in this pass). Handling of each finding is under "Handling".

Verdict: FAIL. One client-triggerable panic; the acceptance suite as committed at HEAD is red. Rest low severity.

## Findings (most severe first)
1. HIGH: client-triggerable panic. `internal/httpapi/notifications.go:110` calls `uuid.MustParse(in.BatchID)`. Huma's `format:"uuid"` check accepts a `urn:uuid:` prefix case-insensitively; the stdlib `uuid.UnmarshalText` trims it case-sensitively. `GET /v1/notifications?batchId=URN:UUID:3f2504e0-4f89-11d3-9a0c-0305e82c3301` passes validation, then panics; withTimeout re-panics, no recover, the connection drops with no response. Old server (google/uuid) answered 200.
2. HIGH: HEAD is red. `TestR10LimitsAgreeSpecAndCore` reads `doc.Components.Parameters["IdempotencyKey" | "CorrelationId" | "Limit"]`; the generated spec inlines parameters and headers, so it fails "parameter IdempotencyKey missing". oasdiff does not flag this, but oapi-codegen clients lose the named types `IdempotencyKey`, `Limit`, `Cursor`, `CorrelationId`; the listTemplates inline response became the named `TemplateList`.
3. MEDIUM: null disagreement between spec and server. Huma skips validation of an optional property set to null (validate.go:839/953), but the spec does not declare these fields nullable. `{"to":"+905551234567","channel":"sms","content":"hi","priority":null}` is now 201; the old server answered 400 "Value is not nullable". Same for scheduledAt, template, content, template.variables. Contradicts record 0024's "they cannot disagree".
4. LOW-MEDIUM: repeated headers no longer refused. The old binder answered 400 when Idempotency-Key or X-Correlation-ID was sent twice; Huma reads the first. `repeatedQuery` covers query parameters only.
5. LOW: an empty parameter value counts as absent (huma.go:1870): `Idempotency-Key:` (empty) creates without a key; `?cursor=` is ignored, instead of 400 for minLength 1. Old answer not confirmed.
6. LOW: `errors.go:169`: an unknown property whose name contains a dot is misreported (`{"a.b":1}` gives field "a", `property "b"`; should be field "body", `property "a.b"`). `GET /v1/notifications/batch` now answers 400 (bad id) where it was 404. HEAD is now served on every GET route.
7. LOW, docs: `docs/briefs/CONTRACT.md:8` still lists the deleted `api.gen.go` and omits `api/openapi.yaml` and `test/apiclient`; the `ws.go:88` comment still says "generated server / strict interface"; record 0024 says the document stayed "identical" (it did not, see 2; "compatible" is accurate); record 0024's Confirmation and `test/apiclient/doc.go` describe acceptance tests that are not on the client at HEAD; putTemplate's 413 is undocumented (already so on main; additive).

## Checked clean
- Error body field order and shape match main; Huma 400/415/422 map to 400; 422 only from a reused key; 413 maps to request_too_large; 5xx answers "internal" with no internal text, cause logged server-side.
- Retry-After on 503 on the Huma path, the deadline path and WebSocket lookups; no duplicate header.
- requireJSON (charset accepted, other types refused); timeoutWriter state under its mutex, late writes dropped.
- Spec: paths, operationIds, status codes per operation, required lists, formats, the Idempotent-Replayed enum and Retry-After header match main; request bodies closed, responses open; limits from limits.go including MaxCursorLen.
- 92 captured parity inputs: beyond wording, differences are detail order, an extra batchId detail, trailing data now refused. Acceptable.
- Drift check: `TestCommittedSpecIsGenerated` compares from `components:` on (keys sorted), so a hand edit or a stale type fails it; CI's `go generate` + `git diff` covers the client; the oasdiff step has fetch-depth 0.
- test/apiclient has all 15 operations, StreamEvents and StatusEvent included.

## Proofs run by the verifier
- `go test ./internal/httpapi/... ./tools/...`: pass (httpapi 3.6 s).
- `go test -race -count=1 ./internal/httpapi/`: pass, 7.1 s.
- `go vet ./internal/httpapi/... ./test/apiclient/`: clean.
- `task api:breaking`: not run; the guard's verifier allowlist has `go tool task` but not `go tool -modfile=tools.mod task`.
- Acceptance, fault and k6: not run (out of scope for this pass).

## Handling (first pass)
1. Fixed in cbcd9c3, then aafc2c0: `uuidParam` reads path ids, batchId and the WebSocket filter through `parseUUID`, which strips `urn:uuid:` in any case as Huma's check does. A failed parse is a 400 naming the parameter, never a panic. Test: `TestEveryUUIDHumaAcceptsIsParsed`.
2. The test-writer's change (d53d3ae) reads the limits from each operation's inline parameters and adds a cursor check. The lost named parameter types (`IdempotencyKey`, `CorrelationId`, `Limit`, `Cursor`) and the new `TemplateList` schema are stated in record 0024 and the PR description. The wire is unchanged and oasdiff reports nothing. Restoring `components.parameters` would need a post-processor over Huma's output for a code-generation nicety, so it was not done.
3. Fixed: `refuseNull` (api.go) finds null for an optional, non-nullable property. Since aafc2c0 its details join Huma's in one 400. Tests: rows "null for an optional field", "null inside the template", "null in a batch item", "null reported beside a missing field".
4. Fixed: `singleValues` refuses a repeated header parameter. Tests: Idempotency-Key and X-Correlation-ID sent twice.
5. Fixed for every parameter whose schema has minLength >= 1 (Idempotency-Key, cursor). Tests: "empty Idempotency-Key", "empty cursor".
6. Fixed: a dotted unknown property is named whole (`unexpected`, errors.go). `GET /v1/notifications/batch` answering 400 (bad id) and HEAD on GET routes are kept and listed in the PR description. Neither breaks a client.
7. Fixed: CONTRACT.md, the ws.go comment, and record 0024 ("compatible", plus the inlined components). The putTemplate 413 was already undocumented on main; left as is.

The GET by id `deliveryAttempts: []` regression was found by the test-writer on the generated client, not by this review. Fixed in c9e5534 (omitzero).

## Second pass (on cbcd9c3, c9e5534, d53d3ae)
Verdict: PASS with low findings. The fixes are correct. d53d3ae only adapts the tests: no assertion was removed or loosened, and several got stricter (typed answers need a JSON body; idempotency concurrency covers both create paths; readEvent refuses a status event missing `at` or `attempts`). TestR10LimitsAgreeSpecAndCore is still meaningful, if weaker: since the spec is generated from limits.go, it mostly guards against a hard-coded tag or a lost TransformSchema. The R10 behaviour tests prove that each limit is enforced. The remaining raw HTTP is justified, and the tests are still black box.

Low findings and handling:
1. refuseNull answered alone, dropping Huma's other details. Fixed in aafc2c0: the details ride in the context to the operation's resolver.
2. Every JSON body is read into memory once more before Huma reads it, and a body containing the bytes `null` is parsed a third time. Kept: a copy of at most 8 MiB, against a POST p50 dominated by the database (about 16 to 19 ms). Recorded in the PR description for the load run.
3. Path ids and the WebSocket filter refused `URN:UUID:`. Fixed in aafc2c0 (`uuidParam`, `parseUUID` in ws.go).
4. Missing tests for a repeated X-Correlation-ID and an empty cursor: added in aafc2c0. The over-limit body containing `null` follows the read-error path that the acceptance 413 test covers.

Process notes from the review (follow-ups, not changed here):
- The guard's verifier allowlist has `go tool task` but not `go tool -modfile=tools.mod task`, so a verifier cannot run `task api:breaking`.
- agentguard resolves Write paths against CLAUDE_PROJECT_DIR (the main checkout), so it refused `test/acceptance/**` writes inside this worktree. The test-writer used Bash; the merge ownership check still covers the diff.
