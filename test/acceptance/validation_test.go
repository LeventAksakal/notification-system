//go:build integration

package acceptance

import (
	"context"
	"math"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

// R10: Content validation (character limits, required fields)
// (over-limit or missing fields are refused with the field named).
func TestR10ContentLimitsPerChannel(t *testing.T) {
	s := startStack(t, stackOptions{})

	for _, ch := range notification.Channels {
		limit := notification.MaxContentLenFor(ch)
		t.Run(string(ch), func(t *testing.T) {
			// Multi-byte characters: the limit counts characters, not bytes.
			at := strings.Repeat("é", limit)
			n := s.create(t, msg(string(ch), at))
			if got := s.get(t, n.Id); got.Content != at {
				t.Errorf("content at the limit (%d characters) not stored intact: got %d characters",
					limit, len([]rune(got.Content)))
			}
			r := s.postCreate(t, msg(string(ch), at+"é"))
			wantError(t, r, http.StatusBadRequest, "invalid_request", "content")
		})
	}
}

// R10: Content validation (character limits, required fields)
// (missing or malformed fields are refused with the field named).
func TestR10RequiredAndMalformedFields(t *testing.T) {
	s := startStack(t, stackOptions{})
	requireIntake(t, s)

	t.Run("email to at 254 characters accepted", func(t *testing.T) {
		body := msg("email", "hi")
		body.To = emailOfLen(notification.MaxEmailRecipientLen)
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
		{"sms to too short", create{To: "+123456", Channel: apiclient.Sms, Content: ptr("x")}, "to"},
		{"email to over 254 characters", create{To: emailOfLen(notification.MaxEmailRecipientLen + 1), Channel: apiclient.Email, Content: ptr("x")}, "to"},
		{"push to over 255 characters", create{To: strings.Repeat("t", notification.MaxRecipientLen+1), Channel: apiclient.Push, Content: ptr("x")}, "to"},
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
		{"TemplateWrite.body maxLength", u(prop(schema("TemplateWrite"), "body").MaxLength), notification.MaxTemplateBodyLen},
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
		checks = append(checks, check{"TemplateRef.variables value maxLength", u(ap.Value.MaxLength), notification.MaxTemplateVariableLen})
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
		limit := notification.MaxContentLenFor(ch)
		rows = append(rows,
			row{string(ch) + " content at limit", string(ch), recipientFor(string(ch)), strings.Repeat("é", limit), true},
			row{string(ch) + " content past limit", string(ch), recipientFor(string(ch)), strings.Repeat("é", limit+1), false},
			row{string(ch) + " empty content", string(ch), recipientFor(string(ch)), "", false},
		)
	}
	rows = append(rows,
		row{"email to at 254", "email", emailOfLen(notification.MaxEmailRecipientLen), "x", true},
		row{"email to at 255", "email", emailOfLen(notification.MaxEmailRecipientLen + 1), "x", false},
		row{"push to at 255", "push", strings.Repeat("t", notification.MaxRecipientLen), "x", true},
		row{"push to at 256", "push", strings.Repeat("t", notification.MaxRecipientLen+1), "x", false},
		row{"push to empty", "push", "", "x", false},
		row{"sms to E.164", "sms", "+1234567", "x", true},
		row{"sms to not E.164", "sms", "1234567", "x", false},
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
	if err := tmpl(strings.Repeat("n", notification.MaxTemplateNameLen), strings.Repeat("é", notification.MaxTemplateBodyLen)); err != nil {
		t.Errorf("template at both limits refused by the database: %v", err)
	}
	if err := tmpl(strings.Repeat("m", notification.MaxTemplateNameLen+1), "x"); err == nil {
		t.Errorf("template name of %d characters accepted by the database", notification.MaxTemplateNameLen+1)
	}
	if err := tmpl("too-long-body", strings.Repeat("é", notification.MaxTemplateBodyLen+1)); err == nil {
		t.Errorf("template body of %d characters accepted by the database", notification.MaxTemplateBodyLen+1)
	}
	if err := tmpl("Bad Name", "x"); err == nil {
		t.Error("template name outside the pattern accepted by the database")
	}
}
