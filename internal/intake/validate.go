// Package intake accepts notifications: it validates and renders create
// requests, stores them (one transaction per request, batches included),
// deduplicates client retries by Idempotency-Key, and serves the reads,
// cancels and replays of the API. Every limit comes from internal/core.
package intake

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/LeventAksakal/notification-system/internal/core"
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
	To          string        `json:"to"`
	Channel     core.Channel  `json:"channel"`
	Content     *string       `json:"content,omitempty"`
	Template    *TemplateRef  `json:"template,omitempty"`
	Priority    core.Priority `json:"priority,omitempty"` // "" means normal
	ScheduledAt *time.Time    `json:"scheduledAt,omitempty"`
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
	Channel     core.Channel
	Priority    core.Priority
	Content     string
	ScheduledAt *time.Time // nil: due now; a past time is stored and due now
}

var smsRecipient = regexp.MustCompile(core.SMSRecipientPattern)

// Validate checks one request against every intake rule and renders its
// template. templates maps the names of the stored templates the request may
// reference to their bodies; a name missing from it is unknown. now is the
// clock the schedule horizon is measured from.
func Validate(r Request, templates map[string]string, now time.Time) (Accepted, []FieldError) {
	var errs []FieldError
	add := func(field, reason string) { errs = append(errs, FieldError{Field: field, Reason: reason}) }

	a := Accepted{To: r.To, Channel: r.Channel, Priority: r.Priority, ScheduledAt: r.ScheduledAt}
	if a.Priority == "" {
		a.Priority = core.PriorityNormal
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
		if r.ScheduledAt.After(now.Add(core.ScheduleHorizon)) {
			add("scheduledAt", fmt.Sprintf("must be at most %s ahead", humanDuration(core.ScheduleHorizon)))
		}
		t := r.ScheduledAt.UTC().Truncate(time.Millisecond)
		a.ScheduledAt = &t
	}
	return a, errs
}

func checkRecipient(c core.Channel, to string) string {
	n := utf8.RuneCountInString(to)
	if n == 0 {
		return "must not be empty"
	}
	if limit := core.MaxRecipientLenFor(c); n > limit {
		return fmt.Sprintf("must be at most %d characters", limit)
	}
	switch c {
	case core.ChannelSMS:
		if !smsRecipient.MatchString(to) {
			return "must be an E.164 phone number (+ and 7 to 15 digits)"
		}
	case core.ChannelEmail:
		if strings.Count(to, "@") != 1 {
			return "must be an email address with exactly one @"
		}
	}
	return ""
}

func checkContent(c core.Channel, content string) string {
	n := utf8.RuneCountInString(content)
	if n == 0 {
		return "must not be empty"
	}
	if limit := core.MaxContentLenFor(c); n > limit {
		return fmt.Sprintf("is %d characters; %s allows at most %d", n, c, limit)
	}
	return ""
}

func renderRef(ref TemplateRef, templates map[string]string) (string, []FieldError) {
	var errs []FieldError
	if len(ref.Variables) > core.MaxTemplateVariables {
		errs = append(errs, FieldError{Field: "template.variables", Reason: fmt.Sprintf("at most %d variables", core.MaxTemplateVariables)})
	}
	for name, v := range ref.Variables {
		if utf8.RuneCountInString(v) > core.MaxTemplateVariableLen {
			errs = append(errs, FieldError{Field: "template.variables." + name,
				Reason: fmt.Sprintf("must be at most %d characters", core.MaxTemplateVariableLen)})
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
	if n := len(name); n == 0 || n > core.MaxTemplateNameLen || !templateName.MatchString(name) {
		errs = append(errs, FieldError{Field: "name", Reason: fmt.Sprintf("must match %s, at most %d characters", core.TemplateNamePattern, core.MaxTemplateNameLen)})
	}
	if n := utf8.RuneCountInString(body); n == 0 || n > core.MaxTemplateBodyLen {
		errs = append(errs, FieldError{Field: "body", Reason: fmt.Sprintf("must be 1 to %d characters", core.MaxTemplateBodyLen)})
	}
	return errs
}

var templateName = regexp.MustCompile(core.TemplateNamePattern)

func humanDuration(d time.Duration) string {
	if d%(24*time.Hour) == 0 {
		return strconv.Itoa(int(d/(24*time.Hour))) + " days"
	}
	return d.String()
}
