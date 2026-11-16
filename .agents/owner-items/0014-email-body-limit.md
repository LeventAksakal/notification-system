---
status: closed
created: 2026-11-16
tags: [limits, email, decision-0017]
---
# Which source should the email body limit stand on? (0014)

## Question
Should the email body limit come from the SMTP guarantee (RFC 5321) or from a named provider's documented message size?

## Context
The owner ruled on 15 Nov 2026 that every limit must come from a standard. The old value was 100,000 characters, with no source. RFC 5321 §4.5.3.1.7 says every SMTP server "MUST" accept at least 64K octets of message content, headers included. A body base64-encoded in 76-character lines (RFC 2045 §6.8) must therefore stay at or under 32 KiB, which leaves about 20 KB for headers. Providers accept far more: Amazon SES allows 10 MB per message on API v1 and 40 MB on v2 or SMTP, after base64 (https://docs.aws.amazon.com/ses/latest/dg/quotas.html). A 32 KiB body holds any plain-text message, but it refuses large HTML newsletters, which commonly run 50 to 100 KB.

## Options
1. **RFC 5321 guarantee: 32,768 bytes (built).** Any conforming server delivers it, and no provider is assumed. Trade-off: HTML-heavy mail is refused with a 400 that names `content`.
2. **A provider's limit, e.g. SES v1: 10 MB after base64, so about 7.3 MB of body.** It matches what a real provider accepts. Trade-off: the service is tied to one provider's number. One such body almost fills the 8 MiB request limit, so `MaxRequestBodyBytes` and `MaxPageSize` would have to be derived again: a page of 200 rows could reach 1.4 GB.

## Assumption taken
Option 1, since the provider is a stand-in and the brief names none. Revisit when a real email provider is chosen.

## Answer
Settled by decision 0017 (limits-standards, ce1fd26): the email body limit is 32,768 bytes (`notification.MaxEmailContentBytes`), with its source in 0017's Numbers table. Closed 16 Nov.
