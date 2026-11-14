---
status: open
created: 2026-11-14
tags: [rate-limit, headroom]
---
# How far under 100 the limiter paces (0013)

## Question
The limiter paces at 97 per second (`APP_RATE_LIMIT_HEADROOM_PERCENT=3`). Is three percent enough room for requests that leave on time but arrive at the provider in a clump?

## Context
The provider counts by arrival. The worker guarantees that a request leaves within one interval (10.3 ms) of its slot; what happens between that check and the provider's clock (a stalled process, the network, the provider's own accept loop) it cannot see. The arithmetic: a one-second window at the provider stays at or under 100 while lateness plus arrival jitter is under 20.7 ms; lateness may use 10.3 ms of that, leaving about 10 ms for the rest.

Measured 14 Nov on the compose stack, busiest second per channel at the provider:

| Run | Busiest second |
|---|---|
| 3 workers, 9,000 messages, quiet (after the clock fix) | 98, 98, 98 |
| the same with MySQL killed, Valkey killed, Valkey paused 14 times | 98, 98, 98 each |
| 6 workers, 30,000 messages | 100, 100, 99 |

In the six-worker run exactly one window of the 10,000 email arrivals reached 100; it starts with four arrivals inside 5 ms, a clump after a stall. No compose run went over 100.

One acceptance run did: 106 in a second on one channel, right after a stall of several seconds (MySQL claims over 2 s). Headroom does not cover this case. The likely cause is a step of the limiter's clock (the Docker VM's time catching up): the limiter then restarts its schedule at its new "now" while slots already handed out are still pending. A unit test with a stepping clock gives the same number (106 to 107, two workers, three channels). Since `ea459a8` a worker that sees the step releases every send placed before it (reason `clock_stepped` in `notifier.slots.given_up`); the same test then stays at the paced rate plus one. Since `ef6c956` this happens only for a forward step of one interval (10.3 ms) or more, counting the smaller forward steps of the last 1.4 s with it: Docker Desktop corrects the VM clock by a fraction of a millisecond about every 30 s, and voiding on each of those cost 5 to 8 percent of throughput. A forward step of s lets old and new slots overlap for s, about s divided by the interval extra requests in one second: under one interval that is at most one, and the three percent headroom is what pays for it. That is one more use of the headroom this item asks about.

Still open after that fix, by reasoning, not seen:
- A worker notices a step only on its next call to the limiter. If all its channels are stuck in a slow claim when the step happens, its old slots that come due before that call still leave, beside new slots of a worker that noticed first.
- A step smaller than the offset uncertainty is not noticed; the overlap is then under one interval (one extra request at most).
- Not reproduced on a real stack: only the unit test with a fake clock exists. A proof would step the clock of the Valkey container under load.

## Options
1. Keep 3%. Trade-off: one more clumped arrival in such a window would show the provider 101.
2. Pace at 95 (5%): the room for clumps grows from about 10 ms to about 31 ms. Trade-off: 2% less throughput; the rate tests' floors (93 steady state, 95 for low priority alone) and every "97" in the documents move with it.
3. Keep 97 and halve the lateness a send may have (5 ms). Trade-off: about 5 ms more room, paid for with more slots given up whenever a timer wakes late.

## Assumption taken
Option 1: the dial exists, no run exceeded the limit, and the choice trades the brief's hard number against throughput, which is the owner's call.

## Answer
