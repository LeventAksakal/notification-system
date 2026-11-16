---
status: closed
created: 2026-11-13
tags: [tooling, agentguard]
---
# The guard hook denies every write inside a sub-agent worktree (0002)

## Question
Should `tools/agentguard` resolve paths against the worktree a sub-agent runs in?

## Context
A test-writer started with worktree isolation could not write `test/acceptance/main_test.go`: the hook computes the path relative to `CLAUDE_PROJECT_DIR` (the main checkout), so the file arrives at the policy as `.claude/worktrees/agent-<id>/test/acceptance/main_test.go` and matches no allow rule (`tools/agentguard/main.go:36`). Lane H did not hit this because it ran as its own session in its own worktree.

## Options
1. Resolve against the git top level of the target file (or the hook input's `cwd`) when the target is inside a worktree. Trade-off: a small change in the guard plus a test; restores isolated lanes.
2. Keep the guard; run lanes in the main checkout on disjoint paths. Trade-off: no isolation, two writers share one index, test-writers cannot commit.
3. Start each lane as a separate session with its own project directory. Trade-off: works today, but the coordinator cannot start those from the Agent tool.

## Assumption taken
Option 2 for this run; nobody edited `tools/`. Also add `.claude/worktrees/` to `.gitignore` when the guard is fixed.

## Answer
Owner, 16 Nov 2026: covered in the 15 to 16 Nov walkthrough. The assumption taken above stands as the answer; closed by the coordinator.
