package core

import "time"

// Every limit is declared once, here (decision 0017). The OpenAPI spec and the
// database CHECKs repeat these numbers; a test fails if any of them disagrees.
const (
	// MaxBatchSize is the most notifications one batch request may carry.
	MaxBatchSize = 1000

	// MaxRecipientLen bounds `to` on every channel, in characters.
	MaxRecipientLen = 255
	// MaxEmailRecipientLen is the longest email address accepted.
	MaxEmailRecipientLen = 254
	// SMSRecipientPattern is E.164: a plus sign and 7 to 15 digits.
	SMSRecipientPattern = `^\+[0-9]{7,15}$`

	// MaxSMSContentLen is 10 SMS segments.
	MaxSMSContentLen = 1600
	// MaxEmailContentLen bounds an email body.
	MaxEmailContentLen = 100_000
	// MaxPushContentLen bounds a push payload.
	MaxPushContentLen = 1024
	// MaxContentLen is the largest content any channel accepts (the spec's maxLength).
	MaxContentLen = MaxEmailContentLen

	// MaxTemplateNameLen bounds a template name; TemplateNamePattern is its shape.
	MaxTemplateNameLen  = 64
	TemplateNamePattern = `^[a-z0-9][a-z0-9_-]*$`
	// MaxTemplateBodyLen bounds a template body, in characters.
	MaxTemplateBodyLen = 100_000
	// MaxTemplateVariables and MaxTemplateVariableLen bound the variables of one request.
	MaxTemplateVariables   = 50
	MaxTemplateVariableLen = 1000

	// MaxIdempotencyKeyLen and MaxCorrelationIDLen bound the two request headers.
	MaxIdempotencyKeyLen = 255
	MaxCorrelationIDLen  = 128

	// MaxRequestBodyBytes bounds a request body; a larger one is refused with 413.
	MaxRequestBodyBytes = 8 << 20

	// MaxErrorLen bounds a stored error message; longer ones are cut.
	MaxErrorLen = 1024

	// DefaultPageSize and MaxPageSize bound `limit` on list endpoints.
	DefaultPageSize = 50
	MaxPageSize     = 200

	// DefaultMetricsWindow and MaxMetricsWindow bound `windowSeconds`.
	DefaultMetricsWindow = 60 * time.Second
	MaxMetricsWindow     = time.Hour

	// ScheduleHorizon is how far ahead `scheduledAt` may lie (decision 0016).
	ScheduleHorizon = 30 * 24 * time.Hour
)

// MaxContentLenFor returns the content limit of a channel, in characters; 0 for an unknown channel.
func MaxContentLenFor(c Channel) int {
	switch c {
	case ChannelSMS:
		return MaxSMSContentLen
	case ChannelEmail:
		return MaxEmailContentLen
	case ChannelPush:
		return MaxPushContentLen
	}
	return 0
}

// MaxRecipientLenFor returns the recipient limit of a channel, in characters; 0 for an unknown channel.
func MaxRecipientLenFor(c Channel) int {
	switch c {
	case ChannelEmail:
		return MaxEmailRecipientLen
	case ChannelSMS, ChannelPush:
		return MaxRecipientLen
	}
	return 0
}
