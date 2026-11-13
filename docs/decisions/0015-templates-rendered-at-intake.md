---
status: proposed
date: 2026-11-13
decision-makers: owner (pending); proposed by the coordinator
revisit: when templates need conditionals or loops
---
# Templates are rendered and validated when the notification is accepted (0015)

## Rule
A notification stores its final content. A template reference is resolved at intake; nothing downstream of the API knows templates exist.

## Context
Bonus item: "Template System: support message templates with variable substitution". Content limits (0017) apply to what is actually sent.

## Options
1. Stored templates (name, channel, body) with `{{name}}` placeholders only. A create request gives either `content` or `template` plus `variables`. The API substitutes, checks the result against the channel's limits, and stores the rendered content. A missing or unknown variable is a 422 naming it.
2. Go `text/template` syntax. Conditionals and loops for free; templates become programs: harder to validate, and a template author can reach anything exposed to the template.
3. Render in the worker at send time. A template edit reaches queued messages; a rendering or length error then surfaces after the request was accepted, as a failed delivery instead of a 4xx.

## Decision
Pending. Recommended: option 1. Because rendering happens at intake, editing a template never changes a notification that was already accepted, and the worker and its tests are untouched by this feature.

## Confirmation
Acceptance tests: template plus variables yields the expected content at the provider double; a missing variable is refused; a rendering that exceeds the channel limit is refused; editing the template after acceptance does not change the queued content.
