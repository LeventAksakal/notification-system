---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when templates need conditionals or loops
---
# Templates are rendered and validated when the notification is accepted (0015)

## Question
How does the system support message templates with variables, and when are they turned into the final message?

## Rule
A notification stores its final content. A template reference is resolved at intake; nothing downstream of the API knows templates exist.

## Context
Bonus item: "Template System: support message templates with variable substitution". Content limits (0017) apply to what is actually sent.

## Options
1. Stored templates (name, channel, body) with `{{name}}` placeholders only. A create request gives either `content` or `template` plus `variables`. The API substitutes, checks the result against the channel's limits, and stores the rendered content. A missing or unknown variable is a 422 naming it.
2. Go `text/template` syntax. Conditionals and loops for free; templates become programs: harder to validate, and a template author can reach anything exposed to the template.
3. Render in the worker at send time. A template edit reaches queued messages; a rendering or length error then surfaces after the request was accepted, as a failed delivery instead of a 4xx.

## Decision
Option 1. Because rendering happens at intake, editing a template never changes a notification that was already accepted, and the worker and its tests are untouched by this feature.

## Confirmation
Acceptance tests: template plus variables yields the expected content at the provider double; a missing variable is refused; a rendering that exceeds the channel limit is refused; editing the template after acceptance does not change the queued content.

## As built (14 Nov 2026)
Templates have a name and a body, no channel: the channel's limits are checked on the rendered content of each notification. A missing variable is a `400` naming the field, like every other validation error (`422` is kept for a reused `Idempotency-Key`); variables the template does not use are ignored.

## Numbers
| Number | Value | Where it comes from |
|---|---|---|
| Template name, body, variables per request, one value | 63-character DNS label; 32,768 bytes; 6,553; 32,768 bytes | Each stands on a standard; record 0017's Numbers table and `internal/notification/limits.go` give the sources (replaced the first, arbitrary values on 16 Nov 2026) |

## Owner review (15 Nov 2026)
- Unknown variables stay accepted. Every placeholder is required, so a typo already fails because the real variable is then missing. Rejecting extras would break every sender the moment a template drops a variable. Instead, the `400` lists `required`, `missing` and `unknown`, so a typo shows up next to the variable it was meant to be.
- Stated assumption: `content` is plain text. Values are inserted as they are, without escaping. Whoever renders text as HTML (the email provider) escapes it. Substitution is a single pass: a value containing `{{x}}` is not substituted again.
- Versions: a template carries a `version`. Updates need `If-Match` (`428` without it, `412` when stale). A notification stores the `template_name` and `template_version` it was rendered from. The rendered content is already stored, so "what was sent" was answerable before; this adds "from which template version". Full history, rollback and pinning a version per request are backlog, not required by the brief. Built in session template-versioning (branch `feat-template-versioning`), merged with the rest.

## As built: versions, traceability, detailed refusals (16 Nov 2026)
Owner, 15 Nov 2026, accepting the design: three improvements, decided in chat.

**Missing variables are one detailed refusal.** A template reference with missing variables is `400 invalid_request` with one detail, `field: "template.variables"` (plus `index` in a batch), carrying `required` (the template's variables in order of first appearance), `missing` and `unknown` (variables sent that the template does not use, sorted, `[]` when none). Unknown variables alone are still accepted, with no error and an unchanged `201`: rejecting them would break every sender the day a template drops a variable, and a typo already fails because the real variable is then missing (where `unknown` shows the typo). Code: `internal/intake/render_template.go` (`renderRef`).

**Optimistic concurrency on templates.** `templates.version` is 1 on create and one more on every update, returned in the body and as a strong `ETag` (`"3"`). `PUT /v1/templates/{name}` without `If-Match` creates (`201`); if the template exists, `428 precondition_required`. With `If-Match` at the current version it replaces (`200`); a stale version, or a template that no longer exists, is `412 precondition_failed` (RFC 9110: `If-Match` is false when there is no current representation). Both refusals carry `currentVersion` on the `If-Match` detail when the template exists. `*`, weak and listed ETags are refused with a `400`: a client sends back the version it read. The rule holds on every layer:

| Layer | Where | What it enforces |
|---|---|---|
| API | `internal/httpapi/template_versioning.go` (`ifMatch`, the putTemplate operation), `contract.go` (`Template.version`); `api/openapi.yaml` is generated from them | one strong ETag of 1 to 10 digits, checked by Huma's request validation; 201, 412, 428 in the contract |
| Service | `internal/intake/manage_templates.go` | create is a plain `INSERT` (a duplicate key is the 428, tried once more if the template was deleted before its version could be read); replace is one `UPDATE templates SET ... WHERE name = ? AND version = ?` (0 rows is the 412), never read-then-write; the read after the write shares its transaction, so the answer is the row this write produced |
| Database | `schema/templates.sql` | `BEFORE INSERT` trigger sets `version = 1`, `BEFORE UPDATE` trigger sets `version = OLD.version + 1`, so no writer can skip, reuse or reset a version; CHECK `template_version_positive` (`version >= 1`) still holds if the triggers are dropped |

sqldef 3.11.30 expresses these triggers (they need no `SIGNAL`, decision 0023); `tools/schemadiff` wraps them for goose and keeps them out of sqlc's schema. Tests: `internal/store/template_versioning_integration_test.go` drives the triggers, the CHECKs and the real `UpdateTemplate` text with plain SQL; `internal/intake/template_versioning_integration_test.go` covers create, replace, stale and missing `If-Match` and eight concurrent replaces from one version (exactly one wins).

**Traceability.** A notification rendered from a template stores `template_name` and `template_version`; both are NULL for content sent as is (CHECK `template_origin_complete`: both or neither; CHECK `template_origin_version_positive`). The API shows them read-only as `template: {name, version}` on the notification. Body and version are read from the same template row before rendering, so the version recorded is the one rendered even if the template changes before the insert commits. That read sits outside the insert's transaction, as it did before: a transaction would not make the pair more consistent, a refused request must be refused before the transaction stores its `Idempotency-Key`, and the key check stays ahead of the template read (`afe82fd`: a replay answers from the stored notification, whatever the template is now). No foreign key: a template may be replaced or deleted later, and the notification keeps what produced it.

**Not built (backlog).** Full version history (old bodies are overwritten), rollback to an earlier version, and pinning a create request to a version. The stored `(template_name, template_version)` pair is the hook for all three.

### Numbers added by these changes
| Number | Where it comes from |
|---|---|
| `version` starts at 1, +1 per update | the owner's rule (15 Nov 2026); 0 is never a version, so `currentVersion` absent means "no template" |
| `version` INT UNSIGNED, at most 4,294,967,295 | MySQL's 4-byte unsigned integer; more updates than one template will see. One more update past it fails with an out-of-range error |
| `If-Match` at most 10 digits (`^"[1-9][0-9]{0,9}"$`) | the digits of 4,294,967,295; a 10-digit value above it is a `412`, never a match |
| 2 insert attempts per create (`createAttempts`) | one retry covers a delete landing between the duplicate key and the version read; a second loss in a row is answered 428 without a version |
| 8 concurrent writers in the race test | arbitrary: enough to collide on one row; any number above 1 proves the rule |
| Migration cost: 1.1 s on an empty schema (scratch MySQL, 16 Nov 2026) | each new CHECK is an `ALTER TABLE` that copies the table; on a populated `notifications` it is proportional to the rows, writes blocked meanwhile (as for decision 0005's CHECKs) |
