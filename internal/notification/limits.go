package notification

import (
	"strings"
	"time"
	"unicode/utf8"
)

// Every limit is declared once, here (decision 0017). The HTTP contract reads
// them (internal/httpapi, so the generated spec carries them, decision 0024);
// the database CHECKs repeat them, and a test fails if one disagrees.
//
// Each number cites the standard or provider document it comes from, or
// states how it is derived; record 0017 has the same list as a table. A limit
// counts in the unit its source counts in: Bytes (UTF-8 octets; len in Go,
// OCTET_LENGTH in MySQL) where the source counts octets, Characters (Unicode
// code points; utf8.RuneCountInString, CHAR_LENGTH) where it counts
// characters. The spec can only say characters (JSON Schema maxLength); a
// byte limit of n implies at most n characters, so the spec carries n as the
// character bound and the validator and the CHECK enforce the bytes.
const (
	// MaxBatchSize is the assignment brief, requirement R2: "Support batch
	// creation (up to 1000 notifications per request)".
	MaxBatchSize = 1000

	// MaxEmailRecipientBytes is RFC 5321 §4.5.3.1.3 (a path is at most 256
	// octets including the angle brackets) as RFC 3696 erratum 1690 reads it:
	// "the upper limit on address lengths should normally be considered to be
	// 254". In bytes, as the RFC counts.
	MaxEmailRecipientBytes = 254
	// SMSRecipientPattern is an E.164 number in international form: "+", a
	// country code, which never starts with 0, and at most 15 digits in all
	// (Twilio, https://www.twilio.com/docs/glossary/what-e164: "Be limited to
	// a total of 15 digits in length"; "no E.164 country code starts with 0").
	// E.164 sets no minimum length, so neither does this pattern.
	SMSRecipientPattern = `^\+[1-9][0-9]{1,14}$`
	// MaxPushRecipientLen has no standard: neither Apple's APNs documentation
	// nor Firebase's (https://firebase.google.com/docs/cloud-messaging/manage-tokens)
	// states a token length. Observed tokens are 64 hex characters (APNs, 32
	// bytes) and about 163 characters (FCM); 255 is headroom over both. Raise
	// it, with the column, if a provider issues longer tokens.
	MaxPushRecipientLen = 255
	// MaxRecipientLen is the longest `to` any channel accepts, in characters:
	// the spec's maxLength. Push's 255 is the largest (email's 254 bytes are at
	// most 254 characters; E.164 is at most 16).
	MaxRecipientLen = MaxPushRecipientLen

	// MaxSMSContentLen is Twilio's maximum message body
	// (https://www.twilio.com/docs/messaging/api/message-resource, Body: "Can
	// be up to 1,600 characters in length"), in characters. Twilio splits it
	// into segments (https://www.twilio.com/docs/glossary/what-sms-character-limit):
	// a GSM-7 message is one segment up to 160 characters and 153 per segment
	// when concatenated; a UCS-2 message (any character outside GSM-7) 70 and
	// 67. 1600 characters is 11 GSM-7 or 24 UCS-2 segments.
	MaxSMSContentLen = 1600
	// MaxEmailContentBytes is derived from RFC 5321 §4.5.3.1.7: "The maximum
	// total length of a message content (including any message header section
	// as well as the message body) MUST BE at least 64K octets", so a message
	// within 65,536 octets is accepted by every conforming SMTP server. A body
	// that is not 7-bit ASCII in lines of at most 998 characters (RFC 5322
	// §2.1.1) is sent base64-encoded, which grows it by 4/3 plus a CRLF per 76
	// characters (RFC 2045 §6.8). 32 KiB encodes to 44,842 octets and leaves
	// 20,694 for the headers; 64 KiB would encode to 89,684, past the guarantee
	// (a test in this package recomputes both).
	MaxEmailContentBytes = 32 << 10
	// MaxPushContentBytes is derived from the 4096-byte payload limit of both
	// push services (APNs: "For all other remote notifications, the maximum
	// payload size is 4 KB (4096 bytes)",
	// https://developer.apple.com/documentation/usernotifications/generating-a-remote-notification;
	// FCM: "Maximum payload for both message types is 4096 bytes",
	// https://firebase.google.com/docs/cloud-messaging/customize-messages/set-message-type)
	// less the smallest APNs envelope that carries the content as an alert
	// body (FCM's {"notification":{"body":""}} is one byte shorter). JSON
	// escaping of quotes, backslashes and control characters is not counted.
	MaxPushContentBytes = 4096 - len(apnsAlertEnvelope)
	// MaxContentLen is the largest content any channel accepts, in characters:
	// the spec's maxLength. Email's 32,768 bytes is the largest and holds at
	// most 32,768 characters.
	MaxContentLen = MaxEmailContentBytes

	// MaxTemplateNameLen and TemplateNamePattern make a template name a DNS
	// label in the RFC 1123 form Kubernetes uses for object names
	// (https://kubernetes.io/docs/concepts/overview/working-with-objects/names/,
	// "DNS Label Names": at most 63 characters, lowercase alphanumerics or
	// '-', starting and ending with an alphanumeric). The 63 is RFC 1035
	// §2.3.4, "labels 63 octets or less"; RFC 1123 §2.1 allows a leading digit.
	MaxTemplateNameLen  = 63
	TemplateNamePattern = `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// MaxTemplateBodyBytes is the largest channel content (email): a longer
	// body could not render into any notification.
	MaxTemplateBodyBytes = MaxEmailContentBytes
	// MaxTemplateVariables is the most placeholders the largest template body
	// can hold, each at least 5 bytes ({{x}}); a variable the body does not
	// name is never used.
	MaxTemplateVariables = MaxTemplateBodyBytes / len("{{x}}")
	// MaxTemplateVariableBytes is the largest channel content: a longer value
	// could not fit any rendered notification. The request body limit bounds
	// the sum of all values.
	MaxTemplateVariableBytes = MaxContentLen

	// MaxIdempotencyKeyLen follows Stripe ("Idempotency keys are up to 255
	// characters long", https://docs.stripe.com/api/idempotent_requests); the
	// IETF draft (draft-ietf-httpapi-idempotency-key-header-08 §2) sets no
	// length. The key is visible ASCII, so characters are bytes.
	MaxIdempotencyKeyLen = 255
	// MaxCorrelationIDLen is the longest identifier a client is expected to
	// carry: a W3C Trace Context traceparent, version 00
	// (https://www.w3.org/TR/trace-context/ §3.2: version "-" 32 hex trace-id
	// "-" 16 hex parent-id "-" 2 hex flags), 55 characters. A UUID (RFC 9562
	// §4, 36 characters), which the server generates, fits as well. The two
	// create operations declare the header in the spec, so a longer or
	// malformed one is refused there with 400; on every other route the
	// middleware replaces it with a generated UUID.
	MaxCorrelationIDLen = 2 + 1 + 32 + 1 + 16 + 1 + 2

	// MaxRequestBodyBytes is derived from the largest batch clients send in
	// bulk: 1000 SMS at the content limit in worst-case UTF-8 (4 bytes per
	// character) is about 6.5 MB of JSON; 8 MiB is the smallest power of two
	// above it (a test in this package recomputes it). JSON escaping is not
	// counted: a character a client escapes as \uXXXX takes 6 bytes, so 1000
	// SMS of 1600 such characters (about 9.6 MB) are refused with 413, like a
	// batch of 1000 emails at their limit (32 MiB); send those in smaller
	// batches.
	MaxRequestBodyBytes = 8 << 20

	// MaxErrorLen has no standard: it bounds a stored provider error message
	// (last_error, delivery_attempts.error, both VARCHAR(1024)) in characters;
	// longer ones are cut. 1 KiB holds an HTTP status line and a provider's
	// error sentence. Change it with both columns.
	MaxErrorLen = 1024

	// MaxCursorLen is the longest cursor the server issues: unpadded base64url
	// (RFC 4648 §5) of "n1:" and a 36-character UUID, 39 bytes, is 52
	// characters; a batch cursor ("b1:" and an index below 1000) is shorter.
	// A test in internal/intake checks it against the encoder.
	MaxCursorLen = ((len("n1:")+36)*8 + 5) / 6

	// MaxPageSize has no standard; it is derived from a response budget equal
	// to the request one: a page of rows at the email content limit must fit
	// MaxRequestBodyBytes, and 8 MiB / 32 KiB = 256 rows of content alone, so
	// the ceiling is 255 with the other fields. 200 is a round value under it.
	// DefaultPageSize has no standard either: a product choice, one screen of
	// rows; tune it freely up to MaxPageSize.
	DefaultPageSize = 50
	MaxPageSize     = 200

	// DefaultMetricsWindow and MaxMetricsWindow have no standard. The metrics
	// endpoint reads the window's per-minute rollups plus at most 30 s of raw
	// rows (decision 0014), so the maximum bounds that read; tune it against the
	// endpoint's latency. The default is
	// a product choice: the last minute.
	DefaultMetricsWindow = 60 * time.Second
	MaxMetricsWindow     = time.Hour

	// ScheduleHorizon is how far ahead `scheduledAt` may lie (decision 0016).
	ScheduleHorizon = 30 * 24 * time.Hour
)

// apnsAlertEnvelope is the smallest APNs payload that carries a notification's
// content as its alert body (29 bytes).
const apnsAlertEnvelope = `{"aps":{"alert":{"body":""}}}`

// NotBlankPattern matches a string that holds at least one character that is
// not whitespace. Whitespace is Unicode's White_Space property
// (https://www.unicode.org/Public/UCD/latest/ucd/PropList.txt): the 25 code
// points Go's unicode.IsSpace reports and strings.TrimSpace removes. It is the
// spec's pattern for `to`, `content` and a template body. The characters are
// written literally (Go resolves the escapes), not as regex escapes, because
// the spec's ECMA-262 dialect and Go's RE2 share no escape for code points
// above U+00FF. The database CHECKs spell the same set in ICU escapes.
const NotBlankPattern = "[^\t\n\v\f\r \u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]"

// IsBlank reports whether s is empty or whitespace only (NotBlankPattern's set).
func IsBlank(s string) bool { return strings.TrimSpace(s) == "" }

// Unit is what a limit counts.
type Unit string

const (
	// Characters are Unicode code points (utf8.RuneCountInString, CHAR_LENGTH).
	Characters Unit = "characters"
	// Bytes are UTF-8 octets (len, OCTET_LENGTH).
	Bytes Unit = "bytes"
)

// Len measures s in u.
func (u Unit) Len(s string) int {
	if u == Bytes {
		return len(s)
	}
	return utf8.RuneCountInString(s)
}

// Limit is a maximum length and the unit it counts.
type Limit struct {
	Max  int
	Unit Unit
}

// ContentLimit returns the content limit of a channel; the zero Limit for an unknown channel.
func ContentLimit(c Channel) Limit {
	switch c {
	case ChannelSMS:
		return Limit{MaxSMSContentLen, Characters}
	case ChannelEmail:
		return Limit{MaxEmailContentBytes, Bytes}
	case ChannelPush:
		return Limit{MaxPushContentBytes, Bytes}
	}
	return Limit{}
}

// RecipientLimit returns the recipient limit of a channel; the zero Limit for an unknown channel.
func RecipientLimit(c Channel) Limit {
	switch c {
	case ChannelEmail:
		return Limit{MaxEmailRecipientBytes, Bytes}
	case ChannelSMS, ChannelPush:
		return Limit{MaxRecipientLen, Characters}
	}
	return Limit{}
}
