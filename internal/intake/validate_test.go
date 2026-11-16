package intake

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func ptr[T any](v T) *T { return &v }

func TestValidateNamesEveryBrokenField(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	templates := storedTemplates(map[string]string{"welcome": "Hello {{name}}", "long": strings.Repeat("x", 1000) + "{{tail}}", "bare": "{{x}}", "amplifier": strings.Repeat("{{x}}", notification.MaxTemplateVariables)})
	ok := func(r Request) Request { return r }
	sms := func(content string) Request {
		return Request{To: "+905551234567", Channel: notification.ChannelSMS, Content: &content}
	}

	for _, tc := range []struct {
		name   string
		req    Request
		fields []string // nil: accepted
	}{
		{"plain sms accepted", sms("hi"), nil},
		{"sms content at its limit, multi-byte", sms(strings.Repeat("é", notification.MaxSMSContentLen)), nil},
		{"sms content past its limit", sms(strings.Repeat("é", notification.MaxSMSContentLen+1)), []string{"content"}},
		{"push content at its limit in bytes", Request{To: "tok", Channel: notification.ChannelPush, Content: ptr(strings.Repeat("é", notification.MaxPushContentBytes/2) + "a")}, nil},
		{"push content one byte past its limit", Request{To: "tok", Channel: notification.ChannelPush, Content: ptr(strings.Repeat("é", notification.MaxPushContentBytes/2) + "ab")}, []string{"content"}},
		{"email content at its limit in bytes", Request{To: "a@b.c", Channel: notification.ChannelEmail, Content: ptr(strings.Repeat("é", notification.MaxEmailContentBytes/2))}, nil},
		{"email content one byte past its limit", Request{To: "a@b.c", Channel: notification.ChannelEmail, Content: ptr(strings.Repeat("é", notification.MaxEmailContentBytes/2) + "a")}, []string{"content"}},
		{"empty content", sms(""), []string{"content"}},
		{"whitespace-only content", sms(" \t\n\u00a0\u3000\u2028"), []string{"content"}},
		{"content of one visible character among whitespace", sms(" \u3000x\n"), nil},
		{"content of a zero-width space is not whitespace", sms("\u200b"), nil},
		{"neither content nor template", Request{To: "+905551234567", Channel: notification.ChannelSMS}, []string{"content"}},
		{"both content and template", ok(Request{To: "+905551234567", Channel: notification.ChannelSMS, Content: ptr("x"),
			Template: &TemplateRef{Name: "welcome"}}), []string{"template"}},
		{"sms recipient not E.164", Request{To: "05551234567", Channel: notification.ChannelSMS, Content: ptr("x")}, []string{"to"}},
		{"sms recipient of one digit after the country code", Request{To: "+12", Channel: notification.ChannelSMS, Content: ptr("x")}, nil},
		{"sms recipient of 15 digits", Request{To: "+123456789012345", Channel: notification.ChannelSMS, Content: ptr("x")}, nil},
		{"sms recipient of 16 digits", Request{To: "+1234567890123456", Channel: notification.ChannelSMS, Content: ptr("x")}, []string{"to"}},
		{"sms recipient with a country code starting 0", Request{To: "+0123456", Channel: notification.ChannelSMS, Content: ptr("x")}, []string{"to"}},
		{"sms recipient of a plus sign alone", Request{To: "+", Channel: notification.ChannelSMS, Content: ptr("x")}, []string{"to"}},
		{"email recipient without @", Request{To: "ada.example.com", Channel: notification.ChannelEmail, Content: ptr("x")}, []string{"to"}},
		{"email recipient with two @", Request{To: "a@b@c", Channel: notification.ChannelEmail, Content: ptr("x")}, []string{"to"}},
		{"email recipient past 254", Request{To: strings.Repeat("a", 250) + "@b.cd", Channel: notification.ChannelEmail, Content: ptr("x")}, []string{"to"}},
		{"email recipient at 254 bytes", Request{To: strings.Repeat("a", 249) + "@b.cd", Channel: notification.ChannelEmail, Content: ptr("x")}, nil},
		{"email recipient of 254 characters but more bytes", Request{To: strings.Repeat("é", 249) + "@b.cd", Channel: notification.ChannelEmail, Content: ptr("x")}, []string{"to"}},
		{"push recipient at 255", Request{To: strings.Repeat("t", 255), Channel: notification.ChannelPush, Content: ptr("x")}, nil},
		{"push recipient past 255", Request{To: strings.Repeat("t", 256), Channel: notification.ChannelPush, Content: ptr("x")}, []string{"to"}},
		{"push recipient empty", Request{To: "", Channel: notification.ChannelPush, Content: ptr("x")}, []string{"to"}},
		{"push recipient whitespace only", Request{To: " \t\u00a0", Channel: notification.ChannelPush, Content: ptr("x")}, []string{"to"}},
		{"unknown channel", Request{To: "x", Channel: "fax", Content: ptr("x")}, []string{"channel"}},
		{"unknown priority", Request{To: "+905551234567", Channel: notification.ChannelSMS, Content: ptr("x"), Priority: "urgent"}, []string{"priority"}},
		{"schedule at the horizon", Request{To: "t", Channel: notification.ChannelPush, Content: ptr("x"), ScheduledAt: ptr(now.Add(notification.ScheduleHorizon))}, nil},
		{"schedule past the horizon", Request{To: "t", Channel: notification.ChannelPush, Content: ptr("x"), ScheduledAt: ptr(now.Add(notification.ScheduleHorizon + time.Second))}, []string{"scheduledAt"}},
		{"schedule in the past", Request{To: "t", Channel: notification.ChannelPush, Content: ptr("x"), ScheduledAt: ptr(now.Add(-time.Hour))}, nil},
		{"template rendered", Request{To: "t", Channel: notification.ChannelPush, Template: &TemplateRef{Name: "welcome", Variables: map[string]string{"name": "Ada"}}}, nil},
		{"template variable missing", Request{To: "t", Channel: notification.ChannelPush, Template: &TemplateRef{Name: "welcome"}}, []string{"template.variables"}},
		{"template unknown", Request{To: "t", Channel: notification.ChannelPush, Template: &TemplateRef{Name: "nope"}}, []string{"template.name"}},
		{"template renders blank", Request{To: "t", Channel: notification.ChannelPush, Template: &TemplateRef{Name: "bare", Variables: map[string]string{"x": " \u3000"}}}, []string{"content"}},
		{"template variables past the count", Request{To: "t", Channel: notification.ChannelPush, Template: &TemplateRef{Name: "bare", Variables: manyVars(notification.MaxTemplateVariables + 1)}}, []string{"template.variables"}},
		{"template variable past its bytes", Request{To: "t", Channel: notification.ChannelPush, Template: &TemplateRef{Name: "bare", Variables: map[string]string{"x": strings.Repeat("é", notification.MaxTemplateVariableBytes/2) + "a"}}}, []string{"template.variables.x", "content"}},
		{"template amplified past the cap stops rendering", Request{To: "t", Channel: notification.ChannelPush,
			Template: &TemplateRef{Name: "amplifier", Variables: map[string]string{"x": strings.Repeat("y", notification.MaxTemplateVariableBytes)}}}, []string{"content"}},
		{"template renders past the channel limit", Request{To: "+905551234567", Channel: notification.ChannelSMS,
			Template: &TemplateRef{Name: "long", Variables: map[string]string{"tail": strings.Repeat("y", 601)}}}, []string{"content"}},
		{"several broken fields at once", Request{To: "nope", Channel: notification.ChannelSMS, Content: ptr(""), Priority: "urgent"}, []string{"priority", "to", "content"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, errs := Validate(tc.req, templates, now)
			var got []string
			for _, e := range errs {
				got = append(got, e.Field)
				if e.Reason == "" {
					t.Errorf("field %s has no reason", e.Field)
				}
			}
			if !slices.Equal(got, tc.fields) {
				t.Fatalf("fields = %v, want %v", got, tc.fields)
			}
		})
	}
}

// manyVars returns n variables x, v1 ... v(n-1).
func manyVars(n int) map[string]string {
	m := map[string]string{"x": "y"}
	for i := 1; len(m) < n; i++ {
		m["v"+strconv.Itoa(i)] = "y"
	}
	return m
}

func TestValidateRendersAndDefaults(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	a, errs := Validate(Request{To: "t", Channel: notification.ChannelPush,
		Template: &TemplateRef{Name: "w", Variables: map[string]string{"name": "Ada", "extra": "ignored"}}},
		storedTemplates(map[string]string{"w": "Hi {{ name }}!"}), now)
	if len(errs) > 0 {
		t.Fatalf("refused: %v", errs)
	}
	if a.Content != "Hi Ada!" || a.Priority != notification.PriorityNormal {
		t.Fatalf("got content %q priority %q", a.Content, a.Priority)
	}
}

func TestCheckTemplate(t *testing.T) {
	for _, tc := range []struct {
		name, tname, body string
		fields            []string
	}{
		{"valid", "welcome-1", "Hi", nil},
		{"name starting with a digit", "1st", "Hi", nil},
		{"name with capitals", "Welcome", "Hi", []string{"name"}},
		{"name with an underscore", "welcome_1", "Hi", []string{"name"}},
		{"name ending with a hyphen", "welcome-", "Hi", []string{"name"}},
		{"name at its limit", strings.Repeat("n", notification.MaxTemplateNameLen), "Hi", nil},
		{"name too long", strings.Repeat("n", notification.MaxTemplateNameLen+1), "Hi", []string{"name"}},
		{"empty body", "ok", "", []string{"body"}},
		{"whitespace-only body", "ok", " \n\u3000", []string{"body"}},
		{"body at its limit in bytes", "ok", strings.Repeat("é", notification.MaxTemplateBodyBytes/2), nil},
		{"body one byte past its limit", "ok", strings.Repeat("é", notification.MaxTemplateBodyBytes/2) + "a", []string{"body"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, e := range CheckTemplate(tc.tname, tc.body) {
				got = append(got, e.Field)
			}
			if !slices.Equal(got, tc.fields) {
				t.Fatalf("fields = %v, want %v", got, tc.fields)
			}
		})
	}
}
