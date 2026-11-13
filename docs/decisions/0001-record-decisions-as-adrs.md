---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat)
revisit: when the index passes 20 records
---
# Record decisions as short, checkable ADRs (0001)

## Rule
Every decision that shapes code gets an ADR titled by its rule, with a Confirmation check; replaced ADRs are superseded, never edited.

## Context
Decisions need an audit trail a newcomer can follow, and coding agents over-follow stale decisions unless superseded ones are clearly marked (https://oreilly.com/radar/coding-agents-love-decision-records/).

## Options
1. Short ADRs with a rule line and a check; superseded ones moved out of the index (chosen).
2. Full MADR with every optional section: more reading per decision, no measured gain.
3. Decisions only in commit messages: invisible to agents, lost on squash.

## Decision
Option 1.

## Confirmation
`go test ./tools/adrcheck` fails if the index lists a non-accepted ADR, misses an accepted one, or a superseded ADR lacks its link and banner.
