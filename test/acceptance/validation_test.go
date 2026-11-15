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
			if got := s.get(t, n.ID); got.Content != at {
				t.Errorf("content at the limit (%d characters) not stored intact: got %d characters",
					limit, len([]rune(got.Content)))
			}
			r := s.postJSON(t, "/v1/notifications", msg(string(ch), at+"é"))
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
		body["to"] = emailOfLen(notification.MaxEmailRecipientLen)
		s.create(t, body)
	})

	cases := []struct {
		name  string
		body  map[string]any
		field string // "" when no single field is required by the contract
	}{
		{"missing to", map[string]any{"channel": "sms", "content": "x"}, "to"},
		{"missing channel", map[string]any{"to": smsTo, "content": "x"}, "channel"},
		{"neither content nor template", map[string]any{"to": smsTo, "channel": "sms"}, "content"},
		{"both content and template", map[string]any{"to": smsTo, "channel": "sms", "content": "x",
			"template": map[string]any{"name": "welcome", "variables": map[string]any{"name": "Ada"}}}, ""},
		{"empty content", map[string]any{"to": smsTo, "channel": "sms", "content": ""}, "content"},
		{"sms to not E.164", map[string]any{"to": "05551234567", "channel": "sms", "content": "x"}, "to"},
		{"sms to too short", map[string]any{"to": "+123456", "channel": "sms", "content": "x"}, "to"},
		{"email to over 254 characters", map[string]any{"to": emailOfLen(notification.MaxEmailRecipientLen + 1), "channel": "email", "content": "x"}, "to"},
		{"push to over 255 characters", map[string]any{"to": strings.Repeat("t", notification.MaxRecipientLen+1), "channel": "push", "content": "x"}, "to"},
		{"unknown channel", map[string]any{"to": smsTo, "channel": "fax", "content": "x"}, "channel"},
		{"unknown priority", map[string]any{"to": smsTo, "channel": "sms", "content": "x", "priority": "urgent"}, "priority"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := s.postJSON(t, "/v1/notifications", tc.body)
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
// The reason's wording is not asserted.
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
	param := func(name string) *openapi3.Schema {
		p := doc.Components.Parameters[name]
		if p == nil || p.Value == nil || p.Value.Schema == nil {
			t.Fatalf("parameter %s missing", name)
		}
		return p.Value.Schema.Value
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
	checks := []struct {
		what      string
		spec, cor int
	}{
		{"BatchCreate.notifications maxItems", u(prop(schema("BatchCreate"), "notifications").MaxItems), notification.MaxBatchSize},
		{"NotificationCreate.content maxLength", u(prop(create, "content").MaxLength), notification.MaxContentLen},
		{"NotificationCreate.to maxLength", u(prop(create, "to").MaxLength), notification.MaxRecipientLen},
		{"TemplateWrite.body maxLength", u(prop(schema("TemplateWrite"), "body").MaxLength), notification.MaxTemplateBodyLen},
		{"TemplateName maxLength", u(schema("TemplateName").MaxLength), notification.MaxTemplateNameLen},
		{"TemplateRef.variables maxProperties", u(prop(tref, "variables").MaxProps), notification.MaxTemplateVariables},
		{"IdempotencyKey maxLength", u(param("IdempotencyKey").MaxLength), notification.MaxIdempotencyKeyLen},
		{"CorrelationId maxLength", u(param("CorrelationId").MaxLength), notification.MaxCorrelationIDLen},
	}
	if ap := prop(tref, "variables").AdditionalProperties.Schema; ap == nil || ap.Value == nil {
		t.Error("TemplateRef.variables has no value schema")
	} else {
		checks = append(checks, struct {
			what      string
			spec, cor int
		}{"TemplateRef.variables value maxLength", u(ap.Value.MaxLength), notification.MaxTemplateVariableLen})
	}
	if mx := param("Limit").Max; mx == nil || int(*mx) != notification.MaxPageSize {
		t.Errorf("Limit maximum: spec %v, core %d", mx, notification.MaxPageSize)
	}
	if df, ok := param("Limit").Default.(float64); !ok || int(df) != notification.DefaultPageSize {
		t.Errorf("Limit default: spec %v, core %d", param("Limit").Default, notification.DefaultPageSize)
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
