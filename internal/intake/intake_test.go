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

func TestTemplateVariablesAndRendering(t *testing.T) {
	for _, tc := range []struct {
		name, body  string
		vars        map[string]string
		wantVars    []string
		wantContent string
		wantMissing []string
	}{
		{"no placeholders", "plain", nil, []string{}, "plain", nil},
		{"one placeholder", "Hello {{name}}", map[string]string{"name": "Ada"}, []string{"name"}, "Hello Ada", nil},
		{"spaces inside braces", "Hello {{ name }}", map[string]string{"name": "Ada"}, []string{"name"}, "Hello Ada", nil},
		{"repeated variable listed once", "{{a}}{{b}}{{a}}", map[string]string{"a": "1", "b": "2"}, []string{"a", "b"}, "121", nil},
		{"missing variables reported once each", "{{a}} {{b}} {{a}}", map[string]string{}, []string{"a", "b"}, "  ", []string{"a", "b"}},
		{"values are not rendered again", "{{a}}", map[string]string{"a": "{{b}}", "b": "x"}, []string{"a"}, "{{b}}", nil},
		{"single braces are text", "{a} {{ }}", nil, []string{}, "{a} {{ }}", nil},
		{"multi-byte text kept", "Grüße {{n}} ☃", map[string]string{"n": "Ada"}, []string{"n"}, "Grüße Ada ☃", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Variables(tc.body); !slices.Equal(got, tc.wantVars) || got == nil {
				t.Errorf("Variables = %#v, want %#v", got, tc.wantVars)
			}
			content, missing, over := Render(tc.body, tc.vars, notification.MaxContentLen)
			if over {
				t.Errorf("over = true for %q", tc.body)
			}
			if !slices.Equal(missing, tc.wantMissing) {
				t.Errorf("missing = %v, want %v", missing, tc.wantMissing)
			}
			if len(missing) == 0 && content != tc.wantContent {
				t.Errorf("content = %q, want %q", content, tc.wantContent)
			}
		})
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

func TestCursorsRoundTripAndRefuseForeignInput(t *testing.T) {
	id := "0193a5b2-7c1e-7d4f-8a2b-1c3d4e5f6a7b"
	if got, err := DecodeListCursor(EncodeListCursor(id)); err != nil || got != id {
		t.Fatalf("list cursor round trip: %q, %v", got, err)
	}
	if got, err := DecodeBatchCursor(EncodeBatchCursor(42)); err != nil || got != 42 {
		t.Fatalf("batch cursor round trip: %d, %v", got, err)
	}
	// MaxCursorLen is the longest cursor issued, so the spec's maxLength refuses no real cursor.
	if n := len(EncodeListCursor(id)); n != notification.MaxCursorLen {
		t.Errorf("list cursor is %d characters, MaxCursorLen %d", n, notification.MaxCursorLen)
	}
	if n := len(EncodeBatchCursor(notification.MaxBatchSize - 1)); n > notification.MaxCursorLen {
		t.Errorf("batch cursor is %d characters, past MaxCursorLen %d", n, notification.MaxCursorLen)
	}
	for _, tc := range []struct {
		name, cursor string
		decode       func(string) error
	}{
		{"garbage list cursor", strings.Repeat("z", 40), func(c string) error { _, err := DecodeListCursor(c); return err }},
		{"not base64", "!!!", func(c string) error { _, err := DecodeListCursor(c); return err }},
		{"batch cursor on the list", EncodeBatchCursor(3), func(c string) error { _, err := DecodeListCursor(c); return err }},
		{"list cursor on a batch", EncodeListCursor(id), func(c string) error { _, err := DecodeBatchCursor(c); return err }},
		{"list cursor with a non-uuid", EncodeListCursor("x' OR 1=1"), func(c string) error { _, err := DecodeListCursor(c); return err }},
		{"negative batch index", EncodeBatchCursor(-1), func(c string) error { _, err := DecodeBatchCursor(c); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.decode(tc.cursor)
			ve, ok := err.(*ValidationError) //nolint:errorlint // the concrete type is the contract here
			if !ok || len(ve.Details) != 1 || ve.Details[0].Field != "cursor" {
				t.Fatalf("got %v, want a validation error naming cursor", err)
			}
		})
	}
}

func TestRequestHashIgnoresDefaultsButNotContent(t *testing.T) {
	at := time.Date(2026, 12, 1, 9, 0, 0, 0, time.FixedZone("x", 3600))
	a := Request{To: "t", Channel: notification.ChannelPush, Content: ptr("x"), ScheduledAt: &at}
	b := a
	b.Priority = notification.PriorityNormal
	utc := at.UTC()
	b.ScheduledAt = &utc
	c := a
	c.Content = ptr("y")
	if string(requestHash("notification", a)) != string(requestHash("notification", b)) {
		t.Error("explicit default priority or another zone changed the hash")
	}
	if string(requestHash("notification", a)) == string(requestHash("notification", c)) {
		t.Error("different content, same hash")
	}
	if string(requestHash("notification", []Request{a})) == string(requestHash("batch", []Request{a})) {
		t.Error("kind is not part of the hash")
	}
}

func TestIdempotencyKeyOutsideVisibleASCIIIsRefusedNamingTheHeader(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		ok        bool
	}{
		{"absent", "", true},
		{"visible ASCII", "order-42:retry_1!~", true},
		{"at its limit", strings.Repeat("k", notification.MaxIdempotencyKeyLen), true},
		{"past its limit", strings.Repeat("k", notification.MaxIdempotencyKeyLen+1), false},
		{"non-ASCII", "clé", false},
		{"space", "a key", false},
		{"control character", "a\tkey", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkIdempotencyKey(tc.key)
			if tc.ok != (err == nil) {
				t.Fatalf("key %q: got %v", tc.key, err)
			}
			if err != nil && err.(*ValidationError).Details[0].Field != "Idempotency-Key" { //nolint:errorlint // concrete type is the contract
				t.Fatalf("key %q: %v does not name the header", tc.key, err)
			}
		})
	}
}

func TestTheRequestHashCoversTheRequestNotTheRenderedContent(t *testing.T) {
	tpl := func(v string) Request {
		return Request{To: "a@example.com", Channel: notification.ChannelEmail,
			Template: &TemplateRef{Name: "welcome", Variables: map[string]string{"name": v}}}
	}
	if !slices.Equal(requestHash("notification", tpl("Ada")), requestHash("notification", tpl("Ada"))) {
		t.Fatal("the same request hashes differently")
	}
	if slices.Equal(requestHash("notification", tpl("Ada")), requestHash("notification", tpl("Bob"))) {
		t.Fatal("different variables hash the same")
	}
	if slices.Equal(requestHash("notification", tpl("Ada")), requestHash("batch", []Request{tpl("Ada")})) {
		t.Fatal("a single create and a batch hash the same")
	}
}
