---
status: accepted
date: 2026-11-13
decision-makers: owner (in chat, 15 Nov 2026); proposed by the coordinator
revisit: when a real provider's limits are known; the email body limit with owner item 0014
---
# Each content limit is one constant, enforced in the spec, the service and the schema (0017)

## Question
The brief asks for content validation (character limits, required fields) without numbers: what are the limits, and how does every layer enforce the same ones?

## Rule
Every limit is declared once in `internal/notification/limits.go`. A test fails if the OpenAPI spec, the validator or a database `CHECK` disagrees with it.

## Context
The brief: "content validation (character limits, required fields)". The provider request has three fields: `to`, `channel`, `content`. Limits are per channel and the brief gives no numbers, so they are ours to choose and state.

## Options
1. Constants plus an agreement test. The API refuses bad input with the field named; the database refuses it even if a future code path skips the validator.
2. Validate in the handler only. Least work; the second writer of the table (a batch path, a replay) can store what the first would refuse.
3. Limits as runtime configuration. Changeable without a release; a database `CHECK` cannot follow a dial, so the last line of defence is lost.

## Decision
Option 1. The numbers are the owner's call; proposed (replaced by the Numbers table under As built):

| Channel | `to` | `content` |
|---|---|---|
| sms | E.164: `+` and 7 to 15 digits | 1 to 1600 characters (10 segments); alternative: 160, a single segment |
| email | an address, at most 254 characters | 1 to 100,000 characters |
| push | a device token, 1 to 255 characters, opaque | 1 to 1024 characters |

Required on every notification: `to`, `channel`, `content` (or a template, 0015). `priority` defaults to `normal`. Length is counted in characters, not bytes.

## Confirmation
The agreement test above, and one acceptance test per channel at the limit (accepted) and one past it (refused, field named).

## Owner review (15 Nov 2026)
- Every limit stands on a standard or a provider's documented limit, or is derived from one; no arbitrary numbers. The sources go into this record's Numbers table and beside each constant.
- Correction: 1600 is Twilio's maximum message body, not "10 segments". A segment holds 160 GSM-7 characters (153 when concatenated) or 70 UCS-2 characters (67), so 1600 characters is 11 to 24 billed segments.
- Content and recipients that are blank after trimming are refused at every layer (spec, validator, CHECK).
- Push providers limit payload bytes (APNs and FCM: 4096); the session decides between counting bytes and the worst-case character derivation, and states why.
- Built in session limits-standards (branch `feat-limits-from-standards`, on top of the code-first API branch), merged with the rest.

## As built (16 Nov 2026)
Option 1. The owner ruled on 15 Nov 2026: "every limit should stand off a standard, no arbitrary magic numbers". So every constant in `internal/notification/limits.go` now cites the standard or provider document it comes from, or states how it is derived. Where neither exists, its doc comment says so in one line and says how to tune it. The table below repeats each source.

- **A limit counts in its source's unit.** Push and email content, the email address and the template body count UTF-8 bytes (`len` in Go, `length()` in MySQL), because their sources count octets. SMS content counts characters (code points; `utf8.RuneCountInString`, `char_length()`), because Twilio counts characters. The other limits are ASCII-only, so the two units agree. This replaces "characters in every layer". The spec cannot say bytes: JSON Schema's `maxLength` counts characters. A byte limit of n implies at most n characters, so the spec carries n, and its description lists each channel's limit and unit. The validator and the CHECK enforce the bytes.
- **Blank is refused.** `to`, `content` (given, or rendered from a template) and a template body must hold a character outside Unicode's White_Space property. That property has 25 code points; Go's `unicode.IsSpace` uses the same set. The validator calls `notification.IsBlank` (`strings.TrimSpace`). The spec has `pattern: notification.NotBlankPattern`, a character class written with the literal characters, because ECMA-262 and Go's RE2 share no escape above U+00FF. The CHECKs `recipient_not_whitespace`, `content_not_blank` and `template_body_not_blank` spell the same class in ICU escapes; MySQL's `TRIM` strips spaces only. A unit test checks the pattern against `IsBlank` on every code point. `internal/store/limit_checks_integration_test.go` checks the database on every White_Space code point and on three lookalikes that are not whitespace (U+200B, U+FEFF, U+180E). The acceptance rows are in `test/acceptance/validation_test.go` (`TestR10LimitsAgreeDatabaseAndCore` among them).
- `template_name_shape` now matches with REGEXP_LIKE's `'c'` flag. Before, under the column's `ascii_general_ci` collation, it let capital letters through, which the API refused.
- Migration `20261116015435_limits_from_standards` (sqldef) drops and re-adds the changed CHECKs and narrows `templates.name` and `notifications.template_name` to 63. MySQL checks existing rows when a CHECK is added, so the migration fails on a database holding a row the new rules refuse:
  - an email body over 32,768 bytes, a push body over 4,067 bytes, blank content or a blank recipient;
  - an E.164 number whose country code starts with 0;
  - an email address over 254 bytes;
  - a template named with `_` or capitals, or longer than 63;
  - a template body over 32,768 bytes.

  No production data exists, so the migration does not delete or rewrite rows. MySQL DDL is not transactional: a failed run leaves the constraints it dropped before the failure dropped, goose records nothing, and a second run fails on the first `DROP CONSTRAINT`. Recovery on a development stack is to recreate the database.
- Breaking for clients of `main` (`task api:breaking` reports them): the smaller maxLengths (content, template body, cursor, correlation id, template name) and the new not-blank patterns. `maxProperties` of template variables and the variable value's maxLength grow. The SMS `to` loses its 7-digit minimum.
- Not changed: the `correlation_id` columns stay VARCHAR(128), wider than the 55-character limit. The two create operations declare `X-Correlation-ID` in the spec, so Huma refuses a longer or malformed one with 400 (before, the bound was 128). Every other route replaces it with a generated UUID. Either way no stored id is longer than 55. Narrowing the columns is left for after the schema lanes have landed, so it does not collide with their edits to `notifications.sql`.
- The email body limit trades HTML-heavy mail for delivery through any SMTP server; owner item 0014 holds the alternative (a provider's documented size, e.g. Amazon SES).
- Rendering a template stops writing once the content passes the channel's byte limit (4 bytes per character for SMS). Without that stop, a 33 KB request (6,553 placeholders and one 32 KiB value) would build about 214 MB before the length check. A refused batch keeps no rendered content.

### Numbers
| Constant | Value | Unit | Source or derivation |
|---|---|---|---|
| `MaxBatchSize` | 1000 | items | Assignment brief, requirement R2: "up to 1000 notifications per request" |
| `SMSRecipientPattern` | `^\+[1-9][0-9]{1,14}$` | digits | E.164 as Twilio documents it (https://www.twilio.com/docs/glossary/what-e164): "a total of 15 digits"; "no E.164 country code starts with 0". E.164 sets no minimum, so the pattern has none (the old 7 had no source). The ITU text itself could not be fetched. |
| `MaxEmailRecipientBytes` | 254 | bytes | RFC 5321 §4.5.3.1.3: a path is at most 256 octets with its angle brackets; RFC 3696 erratum 1690 (https://errata.rfc-editor.org/eid1690): "254" |
| `MaxPushRecipientLen` = `MaxRecipientLen` | 255 | characters | No standard: neither Apple nor Firebase (https://firebase.google.com/docs/cloud-messaging/manage-tokens) states a token length. Observed: APNs 64 hex characters, FCM about 163. Headroom; raise it with the column if a provider issues longer tokens. |
| `MaxSMSContentLen` | 1600 | characters | Twilio Message resource, Body: "Can be up to 1,600 characters in length" (https://www.twilio.com/docs/messaging/api/message-resource). Segments (https://www.twilio.com/docs/glossary/what-sms-character-limit): GSM-7 160 single, 153 concatenated; UCS-2 70 and 67. So 1600 is 11 GSM-7 or 24 UCS-2 segments, not "10 segments". |
| `MaxEmailContentBytes` | 32,768 | bytes | RFC 5321 §4.5.3.1.7: message content (headers and body) "MUST BE at least 64K octets". A body is base64-encoded when needed (RFC 5322 §2.1.1: 998-character lines; RFC 2045 §6.8: 76-character encoded lines). 32 KiB encodes to 44,842 octets, which leaves 20,694 for headers; 64 KiB would encode to 89,684. Unit test `TestEmailContentLimitFitsTheSMTPGuarantee`. |
| `MaxPushContentBytes` | 4,067 | bytes | APNs "maximum payload size is 4 KB (4096 bytes)" (Apple, Generating a remote notification); FCM "Maximum payload for both message types is 4096 bytes" (https://firebase.google.com/docs/cloud-messaging/customize-messages/set-message-type). Minus the 29-byte envelope `{"aps":{"alert":{"body":""}}}`. JSON escaping of quotes, backslashes and control characters is not counted. |
| `MaxContentLen` | 32,768 | characters | The largest channel limit, as the spec's character bound |
| `MaxTemplateNameLen`, `TemplateNamePattern` | 63, `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$` | characters | Kubernetes "DNS Label Names", the RFC 1123 form (https://kubernetes.io/docs/concepts/overview/working-with-objects/names/); RFC 1035 §2.3.4: "labels 63 octets or less"; RFC 1123 §2.1 allows a leading digit |
| `MaxTemplateBodyBytes` | 32,768 | bytes | The largest channel content: a longer body cannot render into any notification |
| `MaxTemplateVariables` | 6,553 | variables | The most placeholders the largest body holds: 32,768 / 5 bytes (`{{x}}`) |
| `MaxTemplateVariableBytes` | 32,768 | bytes | The largest channel content: a longer value fits no rendering. The request body limit bounds the sum of all values. |
| `MaxIdempotencyKeyLen` | 255 | characters | Stripe: "Idempotency keys are up to 255 characters long" (https://docs.stripe.com/api/idempotent_requests). draft-ietf-httpapi-idempotency-key-header-08 §2 sets no length. |
| `MaxCorrelationIDLen` | 55 | characters | W3C Trace Context §3.2: a version-00 traceparent is 2 + 32 + 16 + 2 hex digits and 3 dashes (https://www.w3.org/TR/trace-context/). A UUID (RFC 9562 §4, 36) fits. |
| `MaxRequestBodyBytes` | 8 MiB | bytes | The largest bulk batch: 1000 SMS at 1600 four-byte characters is about 6.5 MB of JSON. 8 MiB is the smallest power of two above it (unit test `TestRequestBodyHoldsAFullBatchOfLongestSMS`). JSON escaping is not counted: 1000 SMS of 1600 escaped characters (6 bytes each), or 1000 emails at their limit (32 MiB), are refused with 413. |
| `MaxErrorLen` | 1024 | characters | No standard: the width of `last_error` and `delivery_attempts.error`. It holds a status line and a provider's error sentence. Change it with both columns. |
| `MaxCursorLen` | 52 | characters | The longest cursor issued: unpadded base64url (RFC 4648 §5) of `n1:` and a 36-character UUID (unit test in internal/intake) |
| `MaxPageSize` | 200 | rows | No standard. Ceiling derived from a response budget equal to the request one: 8 MiB / 32 KiB = 256 rows of email content alone, so 255 at most. 200 is a round value under that ceiling. |
| `DefaultPageSize` | 50 | rows | No standard: one screen of rows; tune it freely up to `MaxPageSize` |
| `MaxMetricsWindow`, `DefaultMetricsWindow` | 1 h, 60 s | time | No standard. The endpoint reads the window's per-minute rollups plus at most 30 s of raw rows (0014), so the maximum bounds that read; tune it against the endpoint's latency. The default is the last minute. |
| `ScheduleHorizon` | 30 days | time | Decision 0016 |

### Values that changed (old → new)
- SMS `to`: `^\+[0-9]{7,15}$` → `^\+[1-9][0-9]{1,14}$`
- Email content: 100,000 characters → 32,768 bytes
- Push content: 1,024 characters → 4,067 bytes
- Email `to`: 254 characters → 254 bytes
- Template name: 64, `_` allowed → 63, DNS label
- Template body: 100,000 characters → 32,768 bytes
- Template variables: 50 → 6,553
- Variable value: 1,000 characters → 32,768 bytes
- Correlation id: 128 → 55
- Cursor: 255 → 52

Unchanged, now sourced: SMS content 1600, push `to` 255, idempotency key 255, batch 1000, request body 8 MiB, error 1024, pages 50 and 200, metrics windows 60 s and 1 h, schedule 30 days.
