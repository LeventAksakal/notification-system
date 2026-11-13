//go:build integration

package acceptance

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/LeventAksakal/notification-system/internal/core"
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

	for _, ch := range core.Channels {
		limit := core.MaxContentLenFor(ch)
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
		body["to"] = emailOfLen(core.MaxEmailRecipientLen)
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
		{"email to over 254 characters", map[string]any{"to": emailOfLen(core.MaxEmailRecipientLen + 1), "channel": "email", "content": "x"}, "to"},
		{"push to over 255 characters", map[string]any{"to": strings.Repeat("t", core.MaxRecipientLen+1), "channel": "push", "content": "x"}, "to"},
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

// R10 / decision 0017: the same limit holds in the API spec and in core.
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
		return int(*p)
	}
	create := schema("NotificationCreate")
	tref := schema("TemplateRef")
	checks := []struct {
		what      string
		spec, cor int
	}{
		{"BatchCreate.notifications maxItems", u(prop(schema("BatchCreate"), "notifications").MaxItems), core.MaxBatchSize},
		{"NotificationCreate.content maxLength", u(prop(create, "content").MaxLength), core.MaxContentLen},
		{"NotificationCreate.to maxLength", u(prop(create, "to").MaxLength), core.MaxRecipientLen},
		{"TemplateWrite.body maxLength", u(prop(schema("TemplateWrite"), "body").MaxLength), core.MaxTemplateBodyLen},
		{"TemplateName maxLength", u(schema("TemplateName").MaxLength), core.MaxTemplateNameLen},
		{"TemplateRef.variables maxProperties", u(prop(tref, "variables").MaxProps), core.MaxTemplateVariables},
		{"IdempotencyKey maxLength", u(param("IdempotencyKey").MaxLength), core.MaxIdempotencyKeyLen},
		{"CorrelationId maxLength", u(param("CorrelationId").MaxLength), core.MaxCorrelationIDLen},
	}
	if ap := prop(tref, "variables").AdditionalProperties.Schema; ap == nil || ap.Value == nil {
		t.Error("TemplateRef.variables has no value schema")
	} else {
		checks = append(checks, struct {
			what      string
			spec, cor int
		}{"TemplateRef.variables value maxLength", u(ap.Value.MaxLength), core.MaxTemplateVariableLen})
	}
	if mx := param("Limit").Max; mx == nil || int(*mx) != core.MaxPageSize {
		t.Errorf("Limit maximum: spec %v, core %d", mx, core.MaxPageSize)
	}
	if df, ok := param("Limit").Default.(float64); !ok || int(df) != core.DefaultPageSize {
		t.Errorf("Limit default: spec %v, core %d", param("Limit").Default, core.DefaultPageSize)
	}
	if p := schema("TemplateName").Pattern; p != core.TemplateNamePattern {
		t.Errorf("TemplateName pattern: spec %q, core %q", p, core.TemplateNamePattern)
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
	for _, ch := range core.Channels {
		limit := core.MaxContentLenFor(ch)
		rows = append(rows,
			row{string(ch) + " content at limit", string(ch), recipientFor(string(ch)), strings.Repeat("é", limit), true},
			row{string(ch) + " content past limit", string(ch), recipientFor(string(ch)), strings.Repeat("é", limit+1), false},
			row{string(ch) + " empty content", string(ch), recipientFor(string(ch)), "", false},
		)
	}
	rows = append(rows,
		row{"email to at 254", "email", emailOfLen(core.MaxEmailRecipientLen), "x", true},
		row{"email to at 255", "email", emailOfLen(core.MaxEmailRecipientLen + 1), "x", false},
		row{"push to at 255", "push", strings.Repeat("t", core.MaxRecipientLen), "x", true},
		row{"push to at 256", "push", strings.Repeat("t", core.MaxRecipientLen+1), "x", false},
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
	if err := batch(core.MaxBatchSize); err != nil {
		t.Errorf("batch of %d refused by the database: %v", core.MaxBatchSize, err)
	}
	if err := batch(core.MaxBatchSize + 1); err == nil {
		t.Errorf("batch of %d accepted by the database", core.MaxBatchSize+1)
	}

	tmpl := func(name, body string) error {
		_, err := s.DB.ExecContext(ctx, `INSERT INTO templates (name, body) VALUES (?, ?)`, name, body)
		return err
	}
	if err := tmpl(strings.Repeat("n", core.MaxTemplateNameLen), strings.Repeat("é", core.MaxTemplateBodyLen)); err != nil {
		t.Errorf("template at both limits refused by the database: %v", err)
	}
	if err := tmpl(strings.Repeat("m", core.MaxTemplateNameLen+1), "x"); err == nil {
		t.Errorf("template name of %d characters accepted by the database", core.MaxTemplateNameLen+1)
	}
	if err := tmpl("too-long-body", strings.Repeat("é", core.MaxTemplateBodyLen+1)); err == nil {
		t.Errorf("template body of %d characters accepted by the database", core.MaxTemplateBodyLen+1)
	}
	if err := tmpl("Bad Name", "x"); err == nil {
		t.Error("template name outside the pattern accepted by the database")
	}
}
