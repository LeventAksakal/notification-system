---
status: open
created: 2026-11-14
tags: [delivery, rate-limit, tracing]
---
# Choices the delivery slice took that the records left open (0007)

## Question
Four choices were made while building the worker. Does the owner keep them?

## Context and options
1. **Send pacing.** On this Docker setup a claim takes 16 ms at the median, about 50 ms at p99 and about 170 ms at start-up. Sending straight after the claim squeezed up to 110 requests into one second at the provider although permits left Valkey 10.3 ms apart. Options: (a) send each job 200 ms after its permit was granted, or at once if the claim took longer: a jitter buffer; a claimed job waits at most 200 ms of a 30 s lease; (b) claim before taking the permit: breaks "permits are taken before claiming" (0009), a claimed row would wait on the limiter; (c) more headroom: at 110 seen, about 12% of the rate would be given up.
2. **Go's automatic replay of a keyed POST (0006).** Options: (a) disable it (`GetBody = nil`), so one recorded attempt is exactly one request at the provider; (b) accept it and say so.
3. **Trace across the queue (0019).** The record recommends a new trace linked to the request's. Requirement B5 says "one trace links the API request, the queue wait and the provider call". Options: (a) the delivery span is a child of the stored trace, so the provider sees the request's trace id; (b) new trace plus link.
4. **What trips the breaker (0008).** Options: (a) only transient outcomes (5xx, 408, 425, 429, timeouts, network errors); a 4xx refusal counts as a healthy provider; (b) every non-2xx.

## Assumption taken
1a, 2a, 3a, 4a. Measured with 1a: 700 requests per channel in 7.2 s, 97.1 per second on average, busiest second 98, on all three channels with three workers.
Also: the MySQL driver now interpolates parameters client-side (`InterpolateParams`), because a prepared-statement round trip made an empty claim take 48 ms against 1.7 ms.

## Answer
