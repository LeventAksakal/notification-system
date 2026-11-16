# fix(provider): a failed dial is a recorded provider fault, not a call that never left

> No push and no PR (the coordinator merges through `.agents/merge-queue.md`). Base: `25176b4` (integration/decision-review).

The verifier failed 25176b4 on six findings. This branch fixes each.

| Finding | Fix | Test |
|---|---|---|
| A refusing provider loops forever and never trips the breaker (1, blocker) | `ErrNotSent` only when the call obtained a connection and started no write on any of them. A refused dial, a DNS failure or a dial timeout stays a `transient` provider fault: recorded, counted, fed to health (0026). | `TestARefusingProviderExhaustsBudgetsAndOpensTheOutage`: one job ends `failed`/`exhausted` after 3 transient attempts and 3 claims; 30 rows over a 1 s outage open it, make 21 calls and 25 claims (bound 29 and 59). On the old rule: no outage, 197 calls, 204 claims, no dead letter in 5 s. |
| Under TLS the fix never fires (2) | Writes are counted above TLS: the client dials TLS itself (`DialTLSContext`) and wraps the `*tls.Conn`, so the close_notify written on Close is not a request write. | TLS cases in `TestSendTellsACallThatNeverLeftFromOneThatMayHave` and `TestConnUseCountsRequestWritesSinceTheCallGotItsConnections`. |
| Under HTTP/2, closing the raw conn kills every stream (3) | HTTP/2 off for the provider client (`ForceAttemptHTTP2=false`, empty `TLSNextProto`, ALPN `http/1.1`); the comment says why. | |
| Stress test only logged its stages (4) | Rounds halve or double the timeout until both a not-sent call and a call failed after its write have occurred; fails if not within 20 rounds. | 12 runs at GOMAXPROCS 1, 2, 8: both stages seen in 1-7 rounds. |
| A second GotConn overwrote the first (5) | `connUse` keeps every connection the call obtained with its mark. | "a write on a second connection" case. |
| goleak and idle connections (6) | `goleak.VerifyTestMain` in `internal/provider`; every test client closes its idle connections at cleanup, the stress test after each round. | |

Residual, conservative by design: a dial that times out on a starved worker is recorded although nothing left. Under the earlier loads the extra rows came from warm connections, which this fix still releases.
