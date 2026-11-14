---
status: accepted
date: 2026-11-14
decision-makers: owner (in chat)
revisit: when the service gets a deploy target
---
# main moves by fast-forward; releases are cut by hand from a green commit (0021)

## Rule
`main` receives a pull request's commits unchanged, by fast-forward (a squash for a pull request too large to read commit by commit, or when `main` moved). Never a merge commit, never a rebase of pushed commits. Commit subjects follow Conventional Commits, checked by a commit-msg hook and in CI. A release candidate (`v1.2.3-rc.1`) or a release (`v1.2.3`) is cut on demand by the `release` workflow from a commit on `main` whose `ci` run is green: image built, scanned, pushed to the repository's registry, tag and release notes created. Nothing deploys on a push.

## Context
Pull requests 1 to 6 were merged with GitHub's "Rebase and merge", which rewrites every commit, so `main` did not hold the hashes CI had tested. Pushing to a pull request to see what CI says spent billed minutes. The owner set both rules on 14 Nov 2026 and asked for on-demand release jobs rather than a deploy on every push.

## Options
1. Fast-forward, checked subjects, manual release workflow (chosen).
2. GitHub's rebase or merge buttons: rewritten hashes or merge commits.
3. Publish an image on every push to `main`: minutes and packages for commits nobody will run.

## Decision
Option 1. The gate runs locally before a push (`task check`, `task test:integration`); CI confirms once per change set.

## Confirmation
`git log --merges main` is empty. `go run ./tools/commitcheck -range <base>..HEAD` passes in CI for every pull request. `.github/workflows/release.yml` has only the `workflow_dispatch` trigger and refuses a commit that is not on `main` or has no green `ci` run. The release workflow itself has not been run yet (status: shipped, not run).
