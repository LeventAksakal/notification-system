---
status: open
created: 2026-11-14
tags: [tests, rate-limit]
---
# The rate test's throughput floor (0010)

## Question
Decision 0009's confirmation says the drain must average "at least 95 per channel per second". Measured over a 700-message drain that floor fails about one run in three on this machine. Which number does the test assert?

## Context
The limiter paces at 97 per second. Three runs with a per-second histogram (14 Nov, `TestDeliveryR8RateLimit`): steady state 96.9 to 97.0 per second on every channel in every run; busiest second 98 always. Whole-drain averages 94.7 to 97.0. All the lost time sits in one gap of 83 to 186 ms among the last 20 requests, where three workers contend for the last rows. On a 7.2 s drain, 150 ms is 2%.
Histogram of one run: `sms [98 97 97 97 97 97 96 21]`, `email [98 97 97 97 96 98 97 20]`, `push [98 97 96 98 96 98 97 20]`.

## Options
1. Assert the whole drain at 90 or more and the steady state (first and last second dropped) at 93 or more; log both. Trade-off: the test's number is below the record's 95, though the measured steady state is 97.
2. Keep 95 on the whole drain. Trade-off: a correct build fails when the tail gap lands badly; a retry culture follows.
3. Drain a much longer backlog so the tail is under 1%. Trade-off: the test takes a minute instead of nine seconds.

## Assumption taken
Option 1, set by the coordinator, implemented by a test-writer (not by the lane that wrote the worker). The ceiling assertion (at most 100 in any sliding second) is unchanged.

## Answer
