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
| Template name | at most 64 characters, `^[a-z0-9][a-z0-9_-]*$` | Arbitrary: a readable slug, safe in URLs. Widen it if real names hit it |
| Template body | at most 100,000 characters | The largest channel limit (email), so any template can render for every channel whose limit it fits |
| Variables per request | at most 50 | Arbitrary: bounds the work done per request. Tune it from the largest variable set real templates use |
| One variable's value | at most 1000 characters | Arbitrary, same purpose. The binding limit is the channel's limit on the rendered content |

## Owner review (15 Nov 2026)
- Unknown variables stay accepted. Every placeholder is required, so a typo already fails because the real variable is then missing. Rejecting extras would break every sender the moment a template drops a variable. Instead, the `400` lists `required`, `missing` and `unknown`, so a typo shows up next to the variable it was meant to be.
- Stated assumption: `content` is plain text. Values are inserted as they are, without escaping. Whoever renders text as HTML (the email provider) escapes it. Substitution is a single pass: a value containing `{{x}}` is not substituted again.
- Versions: a template carries a `version`. Updates need `If-Match` (`428` without it, `412` when stale). A notification stores the `template_name` and `template_version` it was rendered from. The rendered content is already stored, so "what was sent" was answerable before; this adds "from which template version". Full history, rollback and pinning a version per request are backlog, not required by the brief. Built in session template-versioning (branch `feat-template-versioning`), merged with the rest.
