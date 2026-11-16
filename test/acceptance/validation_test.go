//go:build integration

package acceptance

import (
	"context"
	"fmt"
	"math"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// emailOfLen returns a syntactically plausible address of exactly n characters (n >= 70).
func emailOfLen(n int) string {
	local := strings.Repeat("a", 64)
	// local@ + d1.d2.<rest>.com
	rest := n - len(local) - 1 - (63 + 1 + 63 + 1) - 4
	return local + "@" + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", rest) + ".com"
}

// atAndPast returns a string exactly at l, in l's unit, and one unit past it.
// Multi-byte characters make bytes and characters disagree, so a check that
// counts the wrong unit fails one of the two.
func atAndPast(l notification.Limit) (at, past string) {
	if l.Unit == notification.Bytes {
		at = strings.Repeat("é", l.Max/2) + strings.Repeat("a", l.Max%2)
		return at, at + "a"
	}
	at = strings.Repeat("é", l.Max)
	return at, at + "é"
}

// whiteSpaceRunes lists Unicode's White_Space property, the set
// notification.IsBlank treats as blank.
func whiteSpaceRunes() []rune {
	var rs []rune
	for _, r16 := range unicode.White_Space.R16 {
		for r := rune(r16.Lo); r <= rune(r16.Hi); r += rune(r16.Stride) {
			rs = append(rs, r)
		}
	}
	for _, r32 := range unicode.White_Space.R32 {
		for r := rune(r32.Lo); r <= rune(r32.Hi); r += rune(r32.Stride) { //nolint:gosec // G115: Unicode table values are code points, below U+10FFFF
			rs = append(rs, r)
		}
	}
	return rs
}

// notWhiteSpace look like whitespace but are not White_Space: content made of
// one of them alone is not blank.
var notWhiteSpace = []rune{'\u200b', '\ufeff', '\u180e'}

// R10: Content validation (character limits, required fields)
// (over-limit or missing fields are refused with the field named; each limit
// counts in its standard's unit: sms characters, email and push bytes).
func TestR10ContentLimitsPerChannel(t *testing.T) {
	s := startStack(t, stackOptions{})

	for _, ch := range notification.Channels {
		limit := notification.ContentLimit(ch)
		t.Run(string(ch), func(t *testing.T) {
			at, past := atAndPast(limit)
			n := s.create(t, msg(string(ch), at))
			if got := s.get(t, n.Id); got.Content != at {
				t.Errorf("content at the limit (%d %s) not stored intact: got %d %s",
					limit.Max, limit.Unit, limit.Unit.Len(got.Content), limit.Unit)
			}
			r := s.postCreate(t, msg(string(ch), past))
			wantError(t, r, http.StatusBadRequest, "invalid_request", "content")
		})
	}
}

// R10: Content validation (character limits, required fields)
// (missing or malformed fields are refused with the field named).
func TestR10RequiredAndMalformedFields(t *testing.T) {
	s := startStack(t, stackOptions{})
	requireIntake(t, s)

	t.Run("email to at 254 bytes accepted", func(t *testing.T) {
		body := msg("email", "hi")
		body.To = emailOfLen(notification.MaxEmailRecipientBytes)
		s.create(t, body)
	})

	t.Run("sms to of a country code and one digit accepted (E.164 sets no minimum)", func(t *testing.T) {
		body := msg("sms", "hi")
		body.To = "+12"
		s.create(t, body)
	})

	// A body the typed client can express goes through it; a body without a
	// required field (the generated type always sends `to` and `channel`) is
	// sent raw.
	type create = apiclient.NotificationCreate
	cases := []struct {
		name  string
		body  any    // create through the typed client; map[string]any raw
		field string // "" when no single field is required by the contract
	}{
		{"missing to", map[string]any{"channel": "sms", "content": "x"}, "to"},
		{"missing channel", map[string]any{"to": smsTo, "content": "x"}, "channel"},
		{"neither content nor template", create{To: smsTo, Channel: apiclient.Sms}, "content"},
		{"both content and template", create{To: smsTo, Channel: apiclient.Sms, Content: ptr("x"),
			Template: &apiclient.TemplateRef{Name: "welcome", Variables: &map[string]string{"name": "Ada"}}}, ""},
		{"empty content", create{To: smsTo, Channel: apiclient.Sms, Content: ptr("")}, "content"},
		{"sms to not E.164", create{To: "05551234567", Channel: apiclient.Sms, Content: ptr("x")}, "to"},
		{"whitespace-only content", create{To: smsTo, Channel: apiclient.Sms, Content: ptr(" \t \u3000")}, "content"},
		{"whitespace-only push to", create{To: " \t ", Channel: apiclient.Push, Content: ptr("x")}, "to"},
		{"sms to with a country code starting 0", create{To: "+0123456", Channel: apiclient.Sms, Content: ptr("x")}, "to"},
		{"sms to of 16 digits", create{To: "+1234567890123456", Channel: apiclient.Sms, Content: ptr("x")}, "to"},
		{"sms to of a plus alone", create{To: "+", Channel: apiclient.Sms, Content: ptr("x")}, "to"},
		{"email to over 254 bytes", create{To: emailOfLen(notification.MaxEmailRecipientBytes + 1), Channel: apiclient.Email, Content: ptr("x")}, "to"},
		{"email to of 254 characters in 503 bytes", create{To: strings.Repeat("é", 249) + "@b.cd", Channel: apiclient.Email, Content: ptr("x")}, "to"},
		{"push to over 255 characters", create{To: strings.Repeat("t", notification.MaxPushRecipientLen+1), Channel: apiclient.Push, Content: ptr("x")}, "to"},
		{"unknown channel", create{To: smsTo, Channel: "fax", Content: ptr("x")}, "channel"},
		{"unknown priority", create{To: smsTo, Channel: apiclient.Sms, Content: ptr("x"), Priority: ptr(apiclient.Priority("urgent"))}, "priority"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r typedAnswer
			if b, typed := tc.body.(create); typed {
				r = s.postCreate(t, b)
			} else {
				r = s.postJSON(t, "/v1/notifications", tc.body)
			}
			if tc.field == "" {
				wantError(t, r, http.StatusBadRequest, "invalid_request")
				return
			}
			wantError(t, r, http.StatusBadRequest, "invalid_request", tc.field)
		})
	}

	t.Run("template rendering to blank content: 400 naming content", func(t *testing.T) {
		s.putTemplate(t, "blank-render", "{{x}}")
		r := s.postCreate(t, withTemplate("sms", "blank-render", map[string]string{"x": " \u3000"}))
		wantError(t, r, http.StatusBadRequest, "invalid_request", "content")
	})

	t.Run("whitespace-only template body: 400 naming body", func(t *testing.T) {
		r := answered(t)(s.API.PutTemplateWithResponse(t.Context(), "blank-body", nil, apiclient.TemplateWrite{Body: " \t\n\u3000"}))
		wantError(t, r, http.StatusBadRequest, "invalid_request", "body")
	})

	t.Run("template name outside the DNS label pattern: 400 naming name", func(t *testing.T) {
		r := answered(t)(s.API.PutTemplateWithResponse(t.Context(), "welcome_1", nil, apiclient.TemplateWrite{Body: "Hi {{name}}"}))
		wantError(t, r, http.StatusBadRequest, "invalid_request", "name")
	})
}

// R10: Content validation (character limits, required fields) - a malformed
// parameter is refused like a malformed field. CONTRACT (api/openapi.yaml,
// Conventions): "Every refused request has the `Error` body. `details` names
// each offending field." Finding 2 of the 14 Nov fault run: a malformed id and
// a repeated single-value query parameter were refused with a plain-text body.
// The reason's wording is not asserted. Sent raw on purpose: the typed client
// cannot send a non-UUID id or a repeated single-value parameter.
func TestR10MalformedParametersRefusedWithErrorBody(t *testing.T) {
	s := startStack(t, stackOptions{})
	requireIntake(t, s)

	cases := []struct {
		name, method, path, field string
	}{
		{"malformed id on get", http.MethodGet, "/v1/notifications/not-a-uuid", "id"},
		{"malformed id on cancel", http.MethodPost, "/v1/notifications/not-a-uuid/cancel", "id"},
		{"malformed id on retry", http.MethodPost, "/v1/notifications/not-a-uuid/retry", "id"},
		{"malformed id on batch read", http.MethodGet, "/v1/batches/not-a-uuid", "id"},
		{"status repeated on list", http.MethodGet, "/v1/notifications?status=sent&status=failed", "status"},
		{"channel repeated on list", http.MethodGet, "/v1/notifications?channel=sms&channel=push", "channel"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := s.do(t, tc.method, tc.path, nil)
			if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
				t.Errorf("%s %s: Content-Type want application/json, got %q (%s)", tc.method, tc.path, r.Header.Get("Content-Type"), r)
			}
			wantError(t, r, http.StatusBadRequest, "invalid_request", tc.field)
		})
	}
}

// R10 / decision 0017: the same limit holds in the API spec and in internal/notification.
func TestR10LimitsAgreeSpecAndCore(t *testing.T) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(filepath.Join(repoRoot, "api", "openapi.yaml"))
	if err != nil {
		t.Fatalf("load api/openapi.yaml: %v", err)
	}
	schema := func(name string) *openapi3.Schema {
		ref := doc.Components.Schemas[name]
		if ref == nil || ref.Value == nil {
			t.Fatalf("schema %s missing from the spec", name)
		}
		return ref.Value
	}
	prop := func(s *openapi3.Schema, name string) *openapi3.Schema {
		p := s.Properties[name]
		if p == nil || p.Value == nil {
			t.Fatalf("property %s missing", name)
		}
		return p.Value
	}
	// The spec is generated from the Huma operations, which inline every
	// parameter into its operation: there is no components.parameters.
	param := func(path, method, in, name string) *openapi3.Schema {
		item := doc.Paths.Value(path)
		if item == nil {
			t.Fatalf("path %s missing from the spec", path)
		}
		op := item.GetOperation(method)
		if op == nil {
			t.Fatalf("%s %s missing from the spec", method, path)
		}
		p := op.Parameters.GetByInAndName(in, name)
		if p == nil || p.Schema == nil || p.Schema.Value == nil {
			t.Fatalf("%s parameter %s missing from %s (%s %s)", in, name, op.OperationID, method, path)
		}
		return p.Schema.Value
	}
	u := func(p *uint64) int {
		if p == nil {
			return -1
		}
		if *p <= math.MaxInt32 {
			return int(*p)
		}
		t.Fatalf("spec limit %d does not fit an int", *p)
		return -1
	}
	create := schema("NotificationCreate")
	tref := schema("TemplateRef")
	type check = struct {
		what      string
		spec, cor int
	}
	checks := []check{
		{"BatchCreate.notifications maxItems", u(prop(schema("BatchCreate"), "notifications").MaxItems), notification.MaxBatchSize},
		{"NotificationCreate.content maxLength", u(prop(create, "content").MaxLength), notification.MaxContentLen},
		{"NotificationCreate.to maxLength", u(prop(create, "to").MaxLength), notification.MaxRecipientLen},
		{"TemplateWrite.body maxLength", u(prop(schema("TemplateWrite"), "body").MaxLength), notification.MaxTemplateBodyBytes},
		{"TemplateName maxLength", u(schema("TemplateName").MaxLength), notification.MaxTemplateNameLen},
		{"TemplateRef.variables maxProperties", u(prop(tref, "variables").MaxProps), notification.MaxTemplateVariables},
	}
	// The two request headers, on both create operations.
	for _, path := range []string{"/v1/notifications", "/v1/notifications/batch"} {
		checks = append(checks,
			check{"POST " + path + " Idempotency-Key maxLength", u(param(path, http.MethodPost, openapi3.ParameterInHeader, "Idempotency-Key").MaxLength), notification.MaxIdempotencyKeyLen},
			check{"POST " + path + " X-Correlation-ID maxLength", u(param(path, http.MethodPost, openapi3.ParameterInHeader, "X-Correlation-ID").MaxLength), notification.MaxCorrelationIDLen},
		)
	}
	if ap := prop(tref, "variables").AdditionalProperties.Schema; ap == nil || ap.Value == nil {
		t.Error("TemplateRef.variables has no value schema")
	} else {
		checks = append(checks, check{"TemplateRef.variables value maxLength", u(ap.Value.MaxLength), notification.MaxTemplateVariableBytes})
	}
	// limit and cursor, on both list endpoints.
	for _, path := range []string{"/v1/notifications", "/v1/batches/{id}"} {
		limit := param(path, http.MethodGet, openapi3.ParameterInQuery, "limit")
		if mx := limit.Max; mx == nil || int(*mx) != notification.MaxPageSize {
			t.Errorf("GET %s limit maximum: spec %v, core %d", path, mx, notification.MaxPageSize)
		}
		if df, ok := limit.Default.(float64); !ok || int(df) != notification.DefaultPageSize {
			t.Errorf("GET %s limit default: spec %v, core %d", path, limit.Default, notification.DefaultPageSize)
		}
		checks = append(checks, check{"GET " + path + " cursor maxLength",
			u(param(path, http.MethodGet, openapi3.ParameterInQuery, "cursor").MaxLength), notification.MaxCursorLen})
	}
	if p := schema("TemplateName").Pattern; p != notification.TemplateNamePattern {
		t.Errorf("TemplateName pattern: spec %q, core %q", p, notification.TemplateNamePattern)
	}
	// Blank values are refused by one pattern in the spec, the service and the database.
	for _, pc := range []struct{ what, got string }{
		{"NotificationCreate.to pattern", prop(create, "to").Pattern},
		{"NotificationCreate.content pattern", prop(create, "content").Pattern},
		{"TemplateWrite.body pattern", prop(schema("TemplateWrite"), "body").Pattern},
	} {
		if pc.got != notification.NotBlankPattern {
			t.Errorf("%s: spec %q, core %q", pc.what, pc.got, notification.NotBlankPattern)
		}
	}
	for _, c := range checks {
		if c.spec != c.cor {
			t.Errorf("%s: spec %d, core %d", c.what, c.spec, c.cor)
		}
	}
}

// R10 / decision 0017: the same limit holds in the database; a CHECK refuses
// what the API refuses, even when a code path skips the validator.
func TestR10LimitsAgreeDatabaseAndCore(t *testing.T) {
	s := startStack(t, stackOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	insertNotification := func(channel, recipient, content string) error {
		_, err := s.DB.ExecContext(ctx,
			`INSERT INTO notifications (id, recipient, channel, content, due_at, correlation_id)
			 VALUES (?, ?, ?, ?, NOW(3), 'limits-test')`, newUUID(), recipient, channel, content)
		return err
	}
	type row struct {
		name              string
		channel, to, body string
		accept            bool
	}
	var rows []row
	for _, ch := range notification.Channels {
		limit := notification.ContentLimit(ch)
		at, past := atAndPast(limit)
		c, to := string(ch), recipientFor(string(ch))
		rows = append(rows,
			row{fmt.Sprintf("%s content at %d %s", c, limit.Max, limit.Unit), c, to, at, true},
			row{fmt.Sprintf("%s content one %s past", c, strings.TrimSuffix(string(limit.Unit), "s")), c, to, past, false},
			row{c + " empty content", c, to, "", false},
			row{c + " whitespace-only content", c, to, " \t\n\u00a0\u3000", false},
		)
	}
	for _, r := range whiteSpaceRunes() {
		if !notification.IsBlank(string(r)) {
			t.Errorf("IsBlank(%U) = false for a White_Space code point", r)
		}
		rows = append(rows, row{fmt.Sprintf("content of %U alone", r), "push", pushTo, string(r), false})
	}
	for _, r := range notWhiteSpace {
		if notification.IsBlank(string(r)) {
			t.Errorf("IsBlank(%U) = true outside White_Space", r)
		}
		rows = append(rows, row{fmt.Sprintf("content of %U alone", r), "push", pushTo, string(r), true})
	}
	rows = append(rows,
		row{"email to at 254 bytes", "email", emailOfLen(notification.MaxEmailRecipientBytes), "x", true},
		row{"email to at 255 bytes", "email", emailOfLen(notification.MaxEmailRecipientBytes + 1), "x", false},
		row{"email to of 254 characters in 503 bytes", "email", strings.Repeat("é", 249) + "@b.cd", "x", false},
		row{"email to whitespace only", "email", " \t ", "x", false},
		row{"push to at 255", "push", strings.Repeat("t", notification.MaxPushRecipientLen), "x", true},
		row{"push to at 256", "push", strings.Repeat("t", notification.MaxPushRecipientLen+1), "x", false},
		row{"push to empty", "push", "", "x", false},
		row{"push to whitespace only", "push", " \t ", "x", false},
		row{"sms to E.164", "sms", "+1234567", "x", true},
		row{"sms to of a country code and one digit", "sms", "+12", "x", true},
		row{"sms to not E.164", "sms", "1234567", "x", false},
		row{"sms to with a country code starting 0", "sms", "+0123456", "x", false},
		row{"sms to of 16 digits", "sms", "+1234567890123456", "x", false},
	)
	for _, r := range rows {
		err := insertNotification(r.channel, r.to, r.body)
		if r.accept && err != nil {
			t.Errorf("%s: the database refused a row the contract allows: %v", r.name, err)
		}
		if !r.accept && err == nil {
			t.Errorf("%s: the database accepted a row past the limit", r.name)
		}
	}

	batch := func(total int) error {
		_, err := s.DB.ExecContext(ctx, `INSERT INTO batches (id, total, correlation_id) VALUES (?, ?, 'limits-test')`, newUUID(), total)
		return err
	}
	if err := batch(notification.MaxBatchSize); err != nil {
		t.Errorf("batch of %d refused by the database: %v", notification.MaxBatchSize, err)
	}
	if err := batch(notification.MaxBatchSize + 1); err == nil {
		t.Errorf("batch of %d accepted by the database", notification.MaxBatchSize+1)
	}

	tmpl := func(name, body string) error {
		_, err := s.DB.ExecContext(ctx, `INSERT INTO templates (name, body) VALUES (?, ?)`, name, body)
		return err
	}
	bodyAt, bodyPast := atAndPast(notification.Limit{Max: notification.MaxTemplateBodyBytes, Unit: notification.Bytes})
	if err := tmpl(strings.Repeat("n", notification.MaxTemplateNameLen), bodyAt); err != nil {
		t.Errorf("template at both limits (name %d characters, body %d bytes) refused by the database: %v",
			notification.MaxTemplateNameLen, notification.MaxTemplateBodyBytes, err)
	}
	for _, tc := range []struct{ what, name, body string }{
		{"a name of 64 characters", strings.Repeat("m", notification.MaxTemplateNameLen+1), "x"},
		{"a body one byte past its limit", "too-long-body", bodyPast},
		{"a whitespace-only body", "blank-body", " \n\u3000"},
		{"a name with an underscore", "welcome_1", "x"},
		{"a name with a capital", "Welcome", "x"},
		{"a name ending with a hyphen", "welcome-", "x"},
		{"a name with a space", "Bad Name", "x"},
	} {
		if err := tmpl(tc.name, tc.body); err == nil {
			t.Errorf("template with %s accepted by the database", tc.what)
		}
	}
}
