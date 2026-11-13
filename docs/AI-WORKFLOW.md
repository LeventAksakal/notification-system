# How AI was used

## Setup
Roles (researcher, test-writer, implementer, verifier) with limits enforced by a hook and a merge check; what each was allowed and why.

## Delegated vs kept
| Work | Who | Why |
|---|---|---|

## Where the agent was wrong, and how it was caught
One row per real mistake, written when it happened (from `.agents/logs/`).

| Date | What the agent did | Caught by (test, guard, verifier, owner read) | Commit | Rule or check added |
|---|---|---|---|---|
| 13 Nov | The coordinating agent began editing the scaffold itself instead of briefing a lane | Owner read | `b53e50d` (finished by a lane) | Coordinator writes only docs, contracts and briefs; building goes to a lane with a written brief |
| 13 Nov | Pre-commit file used a deprecated `gitleaks protect` command, as the coordinator's brief had specified | Coordinator's review of the lane's hand-back | `afe5a76` | Hook commands are checked against the installed tool's `--help` |
| 13 Nov | Pre-commit scripts contained double quotes, which cut every script short under lefthook on Windows; all three steps failed | Running the hook once for real, on a clean file and on a deliberately broken one | `afe5a76` | A hook is not done until it has been run and seen to fail on bad input |
| 13 Nov | First draft of the idempotency decision returned 409 for a reused key with a different payload | Researcher's source check against the IETF draft (422) | `39f2d5a` | Decision records cite the research note for every external fact |
| 13 Nov | Provider double recorded status 202 for a response that was never sent; the coordinating agent read that code and missed it | Verifier agent in a fresh context | `b12cfbf` | Every slice gets a verifier that has not seen the lane's reasoning, in addition to the coordinator's read |
| 13 Nov | Provider double judged the rate limit on deliveries only, and its per-key numbers could not show a resend while dedupe is on: two later tests would have passed without proving anything | Verifier agent, asked what the later tests would rely on | `b12cfbf` | A test double's counters are reviewed against the claims they will be used to prove |
| 13 Nov | Lane inserted a README table row above the document title | Coordinator's read of the diff | `98a0d66` | none; caught by reading |
| 13 Nov | Settings lint forbade only `os.Getenv`; the lane passed it with `os.LookupEnv` and reported that itself | Lane's own hand-back | `ee187d0` | Rule now covers `LookupEnv` and `Environ`; tagged files are linted too |

## Cost
Tokens and wall-clock per phase; what parallel lanes cost vs saved.
