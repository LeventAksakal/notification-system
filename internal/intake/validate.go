// Package intake accepts notifications: it validates and renders create
// requests, stores them (one transaction per request, batches included),
// deduplicates client retries by Idempotency-Key, and serves the reads,
// cancels and replays of the API. Every limit comes from internal/notification.
package intake

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// Conditions the API maps to its error codes.
var (
	ErrNotFound             = errors.New("not found")
	ErrNotCancellable       = errors.New("only a pending notification can be cancelled")
	ErrNotRetryable         = errors.New("only a failed notification can be replayed")
	ErrIdempotencyKeyReused = errors.New("idempotency key was used with a different payload")
)

// Request is one notification as a client asked for it.
type Request struct {
	To          string                `json:"to"`
	Channel     notification.Channel  `json:"channel"`
	Content     *string               `json:"content,omitempty"`
	Template    *TemplateRef          `json:"template,omitempty"`
	Priority    notification.Priority `json:"priority,omitempty"` // "" means normal
	ScheduledAt *time.Time            `json:"scheduledAt,omitempty"`
}

// TemplateRef names a stored template and the values of its variables.
type TemplateRef struct {
	Name      string            `json:"name"`
	Variables map[string]string `json:"variables,omitempty"`
}

// FieldError names one broken rule: the JSON field and, in a batch, the item.
type FieldError struct {
	Index  *int
	Field  string
	Reason string
}

// ValidationError lists every broken rule of a request.
type ValidationError struct{ Details []FieldError }

func (e *ValidationError) Error() string {
	if len(e.Details) == 0 {
		return "invalid request"
	}
	d := e.Details[0]
	msg := d.Field + ": " + d.Reason
	if d.Index != nil {
		msg = "notifications[" + strconv.Itoa(*d.Index) + "]." + msg
	}
	if len(e.Details) > 1 {
		msg += fmt.Sprintf(" (and %d more)", len(e.Details)-1)
	}
	return msg
}

func invalid(field, reason string) *ValidationError {
	return &ValidationError{Details: []FieldError{{Field: field, Reason: reason}}}
}

// Accepted is a request that passed validation: the content is final
// (templates rendered) and the priority defaulted.
type Accepted struct {
	To          string
	Channel     notification.Channel
	Priority    notification.Priority
	Content     string
	ScheduledAt *time.Time // nil: due now; a past time is stored and due now
}

var smsRecipient = regexp.MustCompile(notification.SMSRecipientPattern)

// Validate checks one request against every intake rule and renders its
// template. templates maps the names of the stored templates the request may
// reference to their bodies; a name missing from it is unknown. now is the
// database clock (Service.scheduleClock), which the schedule horizon is
// measured from; it is only read when r carries a schedule.
func Validate(r Request, templates map[string]string, now time.Time) (Accepted, []FieldError) {
	var errs []FieldError
	add := func(field, reason string) { errs = append(errs, FieldError{Field: field, Reason: reason}) }

	a := Accepted{To: r.To, Channel: r.Channel, Priority: r.Priority, ScheduledAt: r.ScheduledAt}
	if a.Priority == "" {
		a.Priority = notification.PriorityNormal
	}
	if !a.Priority.Valid() {
		add("priority", "must be one of high, normal, low")
	}

	if !r.Channel.Valid() {
		add("channel", "must be one of sms, email, push")
	} else if reason := checkRecipient(r.Channel, r.To); reason != "" {
		add("to", reason)
	}

	haveContent := false // the final content is known and can be measured
	switch {
	case r.Content == nil && r.Template == nil:
		add("content", "one of content and template is required")
	case r.Content != nil && r.Template != nil:
		add("template", "give either content or template, not both")
	case r.Content != nil:
		a.Content, haveContent = *r.Content, true
	default:
		content, terrs := renderRef(*r.Template, templates)
		errs = append(errs, terrs...)
		a.Content, haveContent = content, len(terrs) == 0
	}
	if haveContent && r.Channel.Valid() {
		if reason := checkContent(r.Channel, a.Content); reason != "" {
			add("content", reason)
		}
	}

	if r.ScheduledAt != nil {
		a.ScheduledAt = truncate(r.ScheduledAt)
		if a.ScheduledAt.After(now.Add(notification.ScheduleHorizon)) {
			add("scheduledAt", horizonReason())
		}
	}
	return a, errs
}

func checkRecipient(c notification.Channel, to string) string {
	n := utf8.RuneCountInString(to)
	if n == 0 {
		return "must not be empty"
	}
	if limit := notification.MaxRecipientLenFor(c); n > limit {
		return fmt.Sprintf("must be at most %d characters", limit)
	}
	switch c {
	case notification.ChannelSMS:
		if !smsRecipient.MatchString(to) {
			return "must be an E.164 phone number (+ and 7 to 15 digits)"
		}
	case notification.ChannelEmail:
		if strings.Count(to, "@") != 1 {
			return "must be an email address with exactly one @"
		}
	}
	return ""
}

func checkContent(c notification.Channel, content string) string {
	n := utf8.RuneCountInString(content)
	if n == 0 {
		return "must not be empty"
	}
	if limit := notification.MaxContentLenFor(c); n > limit {
		return fmt.Sprintf("is %d characters; %s allows at most %d", n, c, limit)
	}
	return ""
}

func renderRef(ref TemplateRef, templates map[string]string) (string, []FieldError) {
	var errs []FieldError
	if len(ref.Variables) > notification.MaxTemplateVariables {
		errs = append(errs, FieldError{Field: "template.variables", Reason: fmt.Sprintf("at most %d variables", notification.MaxTemplateVariables)})
	}
	for name, v := range ref.Variables {
		if utf8.RuneCountInString(v) > notification.MaxTemplateVariableLen {
			errs = append(errs, FieldError{Field: "template.variables." + name,
				Reason: fmt.Sprintf("must be at most %d characters", notification.MaxTemplateVariableLen)})
		}
	}
	body, ok := templates[ref.Name]
	if !ok {
		return "", append(errs, FieldError{Field: "template.name", Reason: "no template with this name"})
	}
	content, missing := Render(body, ref.Variables)
	for _, name := range missing {
		errs = append(errs, FieldError{Field: "template.variables." + name, Reason: "the template needs this variable"})
	}
	return content, errs
}

// CheckTemplate validates a template before it is stored.
func CheckTemplate(name, body string) []FieldError {
	var errs []FieldError
	if n := len(name); n == 0 || n > notification.MaxTemplateNameLen || !templateName.MatchString(name) {
		errs = append(errs, FieldError{Field: "name", Reason: fmt.Sprintf("must match %s, at most %d characters", notification.TemplateNamePattern, notification.MaxTemplateNameLen)})
	}
	if n := utf8.RuneCountInString(body); n == 0 || n > notification.MaxTemplateBodyLen {
		errs = append(errs, FieldError{Field: "body", Reason: fmt.Sprintf("must be 1 to %d characters", notification.MaxTemplateBodyLen)})
	}
	return errs
}

var templateName = regexp.MustCompile(notification.TemplateNamePattern)

// horizonReason is the refusal of a schedule beyond notification.ScheduleHorizon.
func horizonReason() string {
	return fmt.Sprintf("must be at most %s ahead", humanDuration(notification.ScheduleHorizon))
}

func humanDuration(d time.Duration) string {
	if d%(24*time.Hour) == 0 {
		return strconv.Itoa(int(d/(24*time.Hour))) + " days"
	}
	return d.String()
}
