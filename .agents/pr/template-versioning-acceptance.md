# Template versioning: held acceptance tests and the Huma port (16 Nov 2026)

## After the rebase onto c294d99 (read first)
The acceptance suite now speaks through the generated client (`test/apiclient`), so the code below targets the old `s.do` helpers; its assertions still hold, its calls do not. On this branch the regenerated client changes `PutTemplateWithResponse` to `(ctx, name, params *PutTemplateParams, body)`, so **`test/acceptance` does not compile until these edits land** (`go vet -tags integration ./test/acceptance` stops first on the pre-existing `delivery_wake_test.go:42` `n.ID`):
- `schedule_template_test.go:60-70` `putTemplate`: `s.API.PutTemplateWithResponse(t.Context(), name, nil, apiclient.TemplateWrite{Body: body})`, require `201` and return `*r.JSON201`.
- New `replaceTemplate`: `GetTemplateWithResponse`, take `r.HTTPResponse.Header.Get("ETag")`, then `PutTemplateWithResponse(ctx, name, &apiclient.PutTemplateParams{IfMatch: &etag}, body)`, require `200` and `JSON200.Version == old+1`; use it where `TestB3TemplateCRUD` and `TestB3CreateFromTemplate` replace a template.
- `schedule_template_test.go:160`: the field is `template.variables` and its detail has `missing == ["code"]`.
- The three new tests below (`TestB3TemplateMissingVariables`, `TestB3TemplateVersioning`, `TestB3TemplateTraceability`): same assertions through the client (`JSON400`/`JSON412`/`JSON428` details carry `Required`, `Missing`, `Unknown`, `CurrentVersion`; `Notification.Template` is `*TemplateOrigin`; `Template.Version`). Raw-JSON checks (`unknown` is `[]`, `template` absent on plain content) read `r.Body`.
- oasdiff (`task api:breaking BASE=c294d99`) reports no breaking change, but a PUT without `If-Match` on an existing template is now `428` where it was `200`: a behaviour change clients see.

From the template-versioning session (branch `claude/feat-template-versioning-17a276`).
test-writer was denied `test/acceptance/**` from the worktree (owner item 0002, same as db-rules-retention):
`agentguard: role "test-writer" may not write .claude/worktrees/feat-template-versioning-17a276/test/acceptance/intake_helpers_test.go`.
Not bypassed. Apply from the main checkout (or relaunch test-writer there), then:
`go vet -tags integration ./test/acceptance` and `go test -tags integration ./test/acceptance -run "TestB3|TestGapR11Idempotency"`.
Not compiled or run by anyone yet.

**Without this patch the existing acceptance suite fails on this branch**: `putTemplate` expects 200 but a create is now 201, and `TestB3TemplateCRUD` / `TestB3CreateFromTemplate` re-PUT without `If-Match` (now 428). `intake_gaps_test.go` needs no edit (its second PUT follows a DELETE, so it is a create).

Rows: B3 only. Proposed B3 row text (the coordinator owns `docs/REQUIREMENTS.md`):
`| B3 | Bonus: Template System ... | A template plus variables produces the rendered content; missing variables are one 400 detail listing required, missing and unknown (unknown alone accepted); template updates need If-Match at the current version (428 without, 412 stale); a notification records the template name and version it was rendered from | 0015 | ... | TestB3TemplateCRUD, TestB3CreateFromTemplate, TestB3TemplateMissingVariables, TestB3TemplateVersioning, TestB3TemplateTraceability; internal/intake/template_versioning_integration_test.go; internal/store/template_versioning_integration_test.go |`

## 1. test/acceptance/intake_helpers_test.go
```diff
@@ -65,13 +65,18 @@
 type template struct {
 	Name      string    `json:"name"`
 	Body      string    `json:"body"`
+	Version   int       `json:"version"`
 	Variables []string  `json:"variables"`
 	CreatedAt time.Time `json:"createdAt"`
 	UpdatedAt time.Time `json:"updatedAt"`
 }
 
 type errorDetail struct {
-	Index  *int   `json:"index"`
-	Field  string `json:"field"`
-	Reason string `json:"reason"`
+	Index          *int     `json:"index"`
+	Field          string   `json:"field"`
+	Reason         string   `json:"reason"`
+	Required       []string `json:"required"`
+	Missing        []string `json:"missing"`
+	Unknown        []string `json:"unknown"`
+	CurrentVersion *int     `json:"currentVersion"`
 }
```

## 2. test/acceptance/schedule_template_test.go
```diff
@@ -58,11 +58,36 @@
-// putTemplate stores a template, requires 200.
+// putTemplate creates a template (PUT without If-Match), requires 201.
 func (s *stack) putTemplate(t *testing.T, name, body string) template {
 	t.Helper()
 	r := s.do(t, http.MethodPut, "/v1/templates/"+name, map[string]any{"body": body})
-	if r.Status != http.StatusOK {
-		t.Fatalf("PUT /v1/templates/%s: want 200, got %s", name, r)
+	if r.Status != http.StatusCreated {
+		t.Fatalf("PUT /v1/templates/%s (create): want 201, got %s", name, r)
 	}
 	var tp template
 	r.decode(t, &tp)
 	return tp
 }
+
+// replaceTemplate replaces an existing template: it reads the current ETag,
+// sends it as If-Match, and requires 200 and a version one higher.
+func (s *stack) replaceTemplate(t *testing.T, name, body string) template {
+	t.Helper()
+	r := s.getJSON(t, "/v1/templates/"+name)
+	if r.Status != http.StatusOK {
+		t.Fatalf("GET /v1/templates/%s before replacing: want 200, got %s", name, r)
+	}
+	var cur template
+	r.decode(t, &cur)
+	etag := r.Header.Get("ETag")
+	if etag == "" {
+		t.Fatalf("GET /v1/templates/%s: no ETag header", name)
+	}
+	r = s.do(t, http.MethodPut, "/v1/templates/"+name, map[string]any{"body": body}, "If-Match", etag)
+	if r.Status != http.StatusOK {
+		t.Fatalf("PUT /v1/templates/%s with If-Match %s: want 200, got %s", name, etag, r)
+	}
+	var tp template
+	r.decode(t, &tp)
+	if tp.Version != cur.Version+1 {
+		t.Errorf("replace %s: version want %d, got %d", name, cur.Version+1, tp.Version)
+	}
+	return tp
+}
@@ -120,3 +145,3 @@
 
-	replaced := s.putTemplate(t, "welcome", "Hi {{name}}")
+	replaced := s.replaceTemplate(t, "welcome", "Hi {{name}}")
 	if replaced.Body != "Hi {{name}}" {
@@ -148,3 +173,3 @@
 	t.Run("replacing the template does not change an accepted notification", func(t *testing.T) {
-		s.putTemplate(t, "welcome", "Changed {{name}} {{code}}")
+		s.replaceTemplate(t, "welcome", "Changed {{name}} {{code}}")
 		if got := s.get(t, n.ID); got.Content != want {
@@ -155,4 +180,9 @@
 	t.Run("missing variable: 400 naming it", func(t *testing.T) {
 		r := s.postJSON(t, "/v1/notifications", withTemplate("email", "welcome", map[string]any{"name": "Ada"}))
-		wantError(t, r, http.StatusBadRequest, "invalid_request", "template.variables.code")
+		e := wantError(t, r, http.StatusBadRequest, "invalid_request", "template.variables")
+		if !slices.ContainsFunc(e.Details, func(d errorDetail) bool {
+			return d.Field == "template.variables" && slices.Equal(d.Missing, []string{"code"})
+		}) {
+			t.Errorf("details: want template.variables with missing [code], got %+v", e.Details)
+		}
 	})
```
(Add `"slices"` to that file's imports if it is not there.)

## 3. NEW test/acceptance/template_versioning_test.go
```go
//go:build integration

package acceptance

import (
	"maps"
	"net/http"
	"slices"
	"strconv"
	"testing"
)

// letterBody uses {{last}} twice and its variables out of alphabetical order,
// so "order of first appearance, each once" is observable.
const letterBody = "Dear {{last}}, {{first}}: your code is {{code}}. Regards to {{last}}."

var letterRequired = []string{"last", "first", "code"}

// B3: Template System, message templates with variable substitution
// ("a missing variable is refused": one template.variables detail carries
// required, missing and unknown; unknown variables alone are accepted).
func TestB3TemplateMissingVariables(t *testing.T) {
	s := startStack(t, stackOptions{})
	s.putTemplate(t, "letter", letterBody)
	full := map[string]any{"last": "Lovelace", "first": "Ada", "code": "4821"}
	const rendered = "Dear Lovelace, Ada: your code is 4821. Regards to Lovelace."

	t.Run("single: 400 with one detail listing required, missing and unknown", func(t *testing.T) {
		vars := map[string]any{"first": "Ada", "zeta": "z", "alpha": "a", "mid": "m"}
		r := s.postJSON(t, "/v1/notifications", withTemplate("email", "letter", vars))
		e := wantError(t, r, http.StatusBadRequest, "invalid_request", "template.variables")
		if len(e.Details) != 1 {
			t.Errorf("details: want exactly one, got %d: %s", len(e.Details), r)
		}
		wantVariablesDetail(t, e, nil, letterRequired, []string{"last", "code"}, []string{"alpha", "mid", "zeta"})
	})

	t.Run("single, no variables sent: all missing, unknown is []", func(t *testing.T) {
		r := s.postJSON(t, "/v1/notifications", withTemplate("sms", "letter", nil))
		e := wantError(t, r, http.StatusBadRequest, "invalid_request", "template.variables")
		if len(e.Details) != 1 {
			t.Errorf("details: want exactly one, got %d: %s", len(e.Details), r)
		}
		wantVariablesDetail(t, e, nil, letterRequired, letterRequired, []string{})
	})

	t.Run("batch: one detail per refused item, with its index", func(t *testing.T) {
		items := []map[string]any{
			msg("sms", "b3 batch plain"),
			withTemplate("email", "letter", map[string]any{"first": "Ada"}),
			withTemplate("email", "letter", full),
			withTemplate("push", "letter", map[string]any{"last": "Lovelace", "code": "1", "frist": "typo"}),
		}
		r := s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": items})
		e := wantError(t, r, http.StatusBadRequest, "invalid_request", "template.variables")
		perIndex := map[int]int{}
		for _, d := range e.Details {
			if d.Index == nil {
				t.Errorf("batch detail without index: %+v", d)
				continue
			}
			perIndex[*d.Index]++
		}
		if want := map[int]int{1: 1, 3: 1}; !maps.Equal(perIndex, want) {
			t.Errorf("details per item index: want %v, got %v (%s)", want, perIndex, r)
		}
		one, three := 1, 3
		wantVariablesDetail(t, e, &one, letterRequired, []string{"last", "code"}, []string{})
		wantVariablesDetail(t, e, &three, letterRequired, []string{"first"}, []string{"frist"})
	})

	t.Run("unknown variables alone: 201, same response fields as without them", func(t *testing.T) {
		exact := s.postJSON(t, "/v1/notifications", withTemplate("email", "letter", full))
		if exact.Status != http.StatusCreated {
			t.Fatalf("exact variables: want 201, got %s", exact)
		}
		extra := map[string]any{"zeta": "z", "alpha": "a"}
		maps.Copy(extra, full)
		r := s.postJSON(t, "/v1/notifications", withTemplate("email", "letter", extra))
		if r.Status != http.StatusCreated {
			t.Fatalf("unknown variables only: want 201, got %s", r)
		}
		var n apiNotification
		r.decode(t, &n)
		if n.Content != rendered {
			t.Errorf("content: want %q, got %q", rendered, n.Content)
		}
		if got := s.get(t, n.ID); got.Content != rendered {
			t.Errorf("stored content: want %q, got %q", rendered, got.Content)
		}
		a := slices.Sorted(maps.Keys(exact.object(t)))
		b := slices.Sorted(maps.Keys(r.object(t)))
		if !slices.Equal(a, b) {
			t.Errorf("response fields: with unknown variables %v, without %v", b, a)
		}
	})
}

// wantVariablesDetail requires exactly one template.variables detail at index
// (nil: a detail without index) and checks required, missing and unknown;
// unknown must be present ([]) even when empty.
func wantVariablesDetail(t *testing.T, e apiError, index *int, required, missing, unknown []string) {
	t.Helper()
	where := "single request"
	if index != nil {
		where = "item " + strconv.Itoa(*index)
	}
	var found []errorDetail
	for _, d := range e.Details {
		if d.Field != "template.variables" || (index == nil) != (d.Index == nil) {
			continue
		}
		if index != nil && *d.Index != *index {
			continue
		}
		found = append(found, d)
	}
	if len(found) != 1 {
		t.Errorf("%s: want one template.variables detail, got %d: %+v", where, len(found), e.Details)
		return
	}
	d := found[0]
	if !slices.Equal(d.Required, required) {
		t.Errorf("%s: required: want %q, got %q", where, required, d.Required)
	}
	if !slices.Equal(d.Missing, missing) {
		t.Errorf("%s: missing: want %q, got %q", where, missing, d.Missing)
	}
	if d.Unknown == nil {
		t.Errorf("%s: unknown: want %q, got absent or null", where, unknown)
	} else if !slices.Equal(d.Unknown, unknown) {
		t.Errorf("%s: unknown: want %q, got %q", where, unknown, d.Unknown)
	}
}

// wantETag requires the strong ETag "N" for version N.
func wantETag(t *testing.T, r *response, version int) {
	t.Helper()
	want := `"` + strconv.Itoa(version) + `"`
	if got := r.Header.Get("ETag"); got != want {
		t.Errorf("ETag: want %s, got %q", want, got)
	}
}

// wantIfMatchDetail requires details[0] to name If-Match with currentVersion.
func wantIfMatchDetail(t *testing.T, e apiError, current int) {
	t.Helper()
	if len(e.Details) == 0 {
		t.Errorf("want details[0] naming If-Match, got no details")
		return
	}
	d := e.Details[0]
	if d.Field != "If-Match" {
		t.Errorf("details[0].field: want If-Match, got %q", d.Field)
	}
	switch {
	case d.CurrentVersion == nil:
		t.Errorf("details[0].currentVersion: want %d, absent", current)
	case *d.CurrentVersion != current:
		t.Errorf("details[0].currentVersion: want %d, got %d", current, *d.CurrentVersion)
	}
}

// wantStoredTemplate reads name: 200, this version and body, ETag "version".
func (s *stack) wantStoredTemplate(t *testing.T, name string, version int, body string) {
	t.Helper()
	r := s.getJSON(t, "/v1/templates/"+name)
	if r.Status != http.StatusOK {
		t.Fatalf("GET /v1/templates/%s: want 200, got %s", name, r)
	}
	var tp template
	r.decode(t, &tp)
	if tp.Version != version || tp.Body != body {
		t.Errorf("GET %s: want version %d body %q, got version %d body %q", name, version, body, tp.Version, tp.Body)
	}
	wantETag(t, r, version)
}

// B3: Template System, message templates with variable substitution
// (every template has a version, 1 on create and +1 per update, sent as a
// strong ETag; replacing needs If-Match at the current version).
func TestB3TemplateVersioning(t *testing.T) {
	s := startStack(t, stackOptions{})
	put := func(t *testing.T, name, body string, kv ...string) *response {
		t.Helper()
		return s.do(t, http.MethodPut, "/v1/templates/"+name, map[string]any{"body": body}, kv...)
	}
	const v1, v2 = "Hello {{name}}", "Hi {{name}}, v2"

	r := put(t, "greet", v1)
	if r.Status != http.StatusCreated {
		t.Fatalf("PUT without If-Match on a new template: want 201, got %s", r)
	}
	var created template
	r.decode(t, &created)
	if created.Version != 1 {
		t.Errorf("created: version want 1, got %d", created.Version)
	}
	wantETag(t, r, 1)
	s.wantStoredTemplate(t, "greet", 1, v1)

	t.Run("PUT without If-Match on an existing template: 428 with currentVersion, unchanged", func(t *testing.T) {
		r := put(t, "greet", "overwrite {{name}}")
		e := wantError(t, r, http.StatusPreconditionRequired, "precondition_required")
		wantIfMatchDetail(t, e, 1)
		s.wantStoredTemplate(t, "greet", 1, v1)
	})

	t.Run("If-Match at the current version: 200, version 2", func(t *testing.T) {
		r := put(t, "greet", v2, "If-Match", `"1"`)
		if r.Status != http.StatusOK {
			t.Fatalf(`PUT with If-Match "1": want 200, got %s`, r)
		}
		var tp template
		r.decode(t, &tp)
		if tp.Version != 2 || tp.Body != v2 {
			t.Errorf("PUT response: want version 2 body %q, got version %d body %q", v2, tp.Version, tp.Body)
		}
		wantETag(t, r, 2)
		s.wantStoredTemplate(t, "greet", 2, v2)
	})

	t.Run("stale If-Match: 412 with currentVersion, unchanged", func(t *testing.T) {
		r := put(t, "greet", "stale {{name}}", "If-Match", `"1"`)
		e := wantError(t, r, http.StatusPreconditionFailed, "precondition_failed")
		wantIfMatchDetail(t, e, 2)
		s.wantStoredTemplate(t, "greet", 2, v2)
	})

	t.Run("If-Match on a template that does not exist: 412 without currentVersion, nothing created", func(t *testing.T) {
		r := put(t, "ghost", "boo {{name}}", "If-Match", `"1"`)
		e := wantError(t, r, http.StatusPreconditionFailed, "precondition_failed")
		for _, d := range e.Details {
			if d.CurrentVersion != nil {
				t.Errorf("detail %q: want no currentVersion, got %d", d.Field, *d.CurrentVersion)
			}
		}
		wantError(t, s.getJSON(t, "/v1/templates/ghost"), http.StatusNotFound, "not_found")
	})

	t.Run("malformed If-Match: 400 naming If-Match, unchanged", func(t *testing.T) {
		for _, tc := range []struct{ name, value string }{
			{"wildcard", `*`},
			{"weak", `W/"2"`},
			{"unquoted", `2`},
			{"zero", `"0"`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := put(t, "greet", "malformed {{name}}", "If-Match", tc.value)
				wantError(t, r, http.StatusBadRequest, "invalid_request", "If-Match")
				s.wantStoredTemplate(t, "greet", 2, v2)
			})
		}
	})

	t.Run("every update adds one", func(t *testing.T) {
		if tp := s.replaceTemplate(t, "greet", "Third {{name}}"); tp.Version != 3 {
			t.Errorf("third version: want 3, got %d", tp.Version)
		}
		s.wantStoredTemplate(t, "greet", 3, "Third {{name}}")
	})

	t.Run("list carries each template's version", func(t *testing.T) {
		s.putTemplate(t, "aaa-other", "x {{y}}")
		r := s.getJSON(t, "/v1/templates")
		if r.Status != http.StatusOK {
			t.Fatalf("list templates: want 200, got %s", r)
		}
		var list struct {
			Items []template `json:"items"`
		}
		r.decode(t, &list)
		got := map[string]int{}
		for _, x := range list.Items {
			got[x.Name] = x.Version
		}
		if want := map[string]int{"aaa-other": 1, "greet": 3}; !maps.Equal(got, want) {
			t.Errorf("versions in the list: want %v, got %v", want, got)
		}
	})
}

// getObject GETs path, requires 200, returns the body as a JSON object.
func (s *stack) getObject(t *testing.T, path string) map[string]any {
	t.Helper()
	r := s.getJSON(t, path)
	if r.Status != http.StatusOK {
		t.Fatalf("GET %s: want 200, got %s", path, r)
	}
	return r.object(t)
}

// itemsByID indexes a page's raw items by id.
func itemsByID(t *testing.T, r *response) map[string]map[string]any {
	t.Helper()
	var page struct {
		Items []map[string]any `json:"items"`
	}
	r.decode(t, &page)
	out := make(map[string]map[string]any, len(page.Items))
	for _, it := range page.Items {
		if id, ok := it["id"].(string); ok {
			out[id] = it
		}
	}
	return out
}

func wantOrigin(t *testing.T, where string, m map[string]any, name string, version int) {
	t.Helper()
	raw, ok := m["template"]
	if !ok {
		t.Errorf("%s: no template field, want {%s %d}", where, name, version)
		return
	}
	o, ok := raw.(map[string]any)
	if !ok || o["name"] != name || o["version"] != float64(version) {
		t.Errorf("%s: template want {name:%s version:%d}, got %v", where, name, version, raw)
	}
}

func wantNoOrigin(t *testing.T, where string, m map[string]any) {
	t.Helper()
	if v, ok := m["template"]; ok {
		t.Errorf("%s: plain-content notification has template %v, want no field", where, v)
	}
}

// B3: Template System, message templates with variable substitution
// (a notification records the template name and version that rendered it;
// plain content has none; an update does not touch earlier notifications).
func TestB3TemplateTraceability(t *testing.T) {
	s := startStack(t, stackOptions{})
	s.putTemplate(t, "greet", "Hello {{name}}")
	vars := map[string]any{"name": "Ada"}

	r := s.postJSON(t, "/v1/notifications", withTemplate("email", "greet", vars))
	if r.Status != http.StatusCreated {
		t.Fatalf("create from template v1: want 201, got %s", r)
	}
	wantOrigin(t, "create response (v1)", r.object(t), "greet", 1)
	var older apiNotification
	r.decode(t, &older)
	wantOrigin(t, "GET (v1)", s.getObject(t, "/v1/notifications/"+older.ID), "greet", 1)

	r = s.postJSON(t, "/v1/notifications", msg("sms", "b3 trace plain"))
	if r.Status != http.StatusCreated {
		t.Fatalf("create plain: want 201, got %s", r)
	}
	wantNoOrigin(t, "create response (plain)", r.object(t))
	var plain apiNotification
	r.decode(t, &plain)
	wantNoOrigin(t, "GET (plain)", s.getObject(t, "/v1/notifications/"+plain.ID))

	s.replaceTemplate(t, "greet", "Hi {{name}}!")
	r = s.postJSON(t, "/v1/notifications", withTemplate("email", "greet", vars))
	if r.Status != http.StatusCreated {
		t.Fatalf("create from template v2: want 201, got %s", r)
	}
	wantOrigin(t, "create response (v2)", r.object(t), "greet", 2)
	var newer apiNotification
	r.decode(t, &newer)
	if newer.Content != "Hi Ada!" {
		t.Errorf("v2 content: want %q, got %q", "Hi Ada!", newer.Content)
	}
	wantOrigin(t, "GET (v2)", s.getObject(t, "/v1/notifications/"+newer.ID), "greet", 2)

	t.Run("the earlier notification keeps version 1 and its content", func(t *testing.T) {
		m := s.getObject(t, "/v1/notifications/"+older.ID)
		wantOrigin(t, "GET earlier", m, "greet", 1)
		if m["content"] != "Hello Ada" {
			t.Errorf("earlier content: want %q, got %v", "Hello Ada", m["content"])
		}
	})

	t.Run("list items carry template; plain ones have none", func(t *testing.T) {
		r := s.getJSON(t, "/v1/notifications?limit=200")
		if r.Status != http.StatusOK {
			t.Fatalf("list: want 200, got %s", r)
		}
		byID := itemsByID(t, r)
		for _, c := range []struct {
			id      string
			version int
		}{{older.ID, 1}, {newer.ID, 2}} {
			m, ok := byID[c.id]
			if !ok {
				t.Errorf("list: %s missing", c.id)
				continue
			}
			wantOrigin(t, "list item "+c.id, m, "greet", c.version)
		}
		if m, ok := byID[plain.ID]; !ok {
			t.Errorf("list: plain %s missing", plain.ID)
		} else {
			wantNoOrigin(t, "list item (plain)", m)
		}
	})

	t.Run("batch items carry template; plain ones have none", func(t *testing.T) {
		b := s.createBatch(t, []map[string]any{withTemplate("push", "greet", vars), msg("email", "b3 trace batch plain")})
		if len(b.NotificationIDs) != 2 {
			t.Fatalf("batch: want 2 notification ids, got %v", b.NotificationIDs)
		}
		r := s.getJSON(t, "/v1/batches/"+b.ID)
		if r.Status != http.StatusOK {
			t.Fatalf("GET batch: want 200, got %s", r)
		}
		byID := itemsByID(t, r)
		if m, ok := byID[b.NotificationIDs[0]]; !ok {
			t.Errorf("batch items: item 0 missing")
		} else {
			wantOrigin(t, "batch item 0", m, "greet", 2)
		}
		if m, ok := byID[b.NotificationIDs[1]]; !ok {
			t.Errorf("batch items: item 1 missing")
		} else {
			wantNoOrigin(t, "batch item 1", m)
		}
		wantOrigin(t, "GET batch item 0", s.getObject(t, "/v1/notifications/"+b.NotificationIDs[0]), "greet", 2)
		wantNoOrigin(t, "GET batch item 1", s.getObject(t, "/v1/notifications/"+b.NotificationIDs[1]))
	})
}
```

## Other findings for the coordinator
- Files were not renamed (merge-queue rule): `internal/intake/template.go` and `templates.go` keep their names; new code went to new files named for their job.
- Branch base: this branch was fast-forwarded onto `claude/feat-db-state-machine-retention-e9eec0` (a912b4c) before its first commit, because only that branch's `tools/schemadiff` can carry triggers. Merge db-rules first; this branch then fast-forwards on top (if db-rules is rebased or squashed, rebase this one: it was never pushed).
- golangci-lint on `./internal/store/...` reports 7 findings in db-rules-retention's own test files (gosec G202 in `rules_integration_test.go:56,113,128`; rowserrcheck/sqlclosecheck in `sql_integration_test.go:106,118,180,193`). Not touched here; `task check` will fail on them.

## Huma port at rebase (what moves where)
Transport-neutral and moving as is: everything in `internal/intake` (`PutTemplate(ctx, name, body, ifMatch *uint64) (Template, created bool, error)`, `VersionError` with `ErrVersionRequired` / `ErrVersionStale`, `FieldError.Variables` / `VariableReport`, `Accepted.Template`) and `internal/httpapi/template_versioning.go` (`etagOf`, `parseETag`, `versionDetail`, `ifMatchField`). Done in the rebase onto c294d99 (kept for the record): only the oapi-codegen glue was rewritten:
- `contract.go`: `ErrorDetail` gains `Required, Missing, Unknown []string` (`json:",omitempty"` except that `unknown` must serialize as `[]` when the detail is a variables one, so use `*[]string` or a non-omitempty field set only on that detail) and `CurrentVersion *int`; `Template` gains `Version int` (`minimum:"1"`); `Notification` gains `Template *TemplateOrigin` (`readOnly:"true"`); new `TemplateOrigin{Name TemplateName; Version int}`; the `Error.Code` doc lists `precondition_failed, precondition_required`.
- `templates.go`: `putTemplateInput` gains `IfMatch string `header:"If-Match" pattern:"^\"[1-9][0-9]{0,9}\"$"``; the output becomes `struct{ Status int; ETag string `header:"ETag"`; Body Template }` (Status 201 when `created`, else 200); `templateOutput` for GET gains the ETag header. A `*intake.VersionError` becomes `newError(428 or 412, code, ve.Error())` with `Details = []ErrorDetail{versionDetail(ve)}`.
- `notifications.go` `refusal()`: copy `d.Variables` into the detail (required, missing, unknown); `toNotification` sets `Template` from `TemplateName/TemplateVersion`.
- `api.go`: the putTemplate registration adds 201, 412, 428 to its responses and the description from the old spec (see `api/openapi.yaml` on this branch, `putTemplate`).
- Unit tests `internal/httpapi/template_versioning_test.go` move unchanged except the server constructor of the integration branch.

## Verifier (fresh context, 16 Nov 2026): ship with fixes
1. HIGH, fixed: `GET /v1/notifications` dropped `template` (the hand-built list query in `internal/store/list.go` lacked the columns). The intake integration test now lists a templated and a plain notification.
2. MEDIUM, answered: no HTTP-level test of 428/412/ETag/`unknown: []`/list `template` until the held acceptance patch lands; the httpapi unit tests cannot reach the handlers without a database. The patch above covers each of them; apply it before the full gate.
3. LOW, deliberate: `If-Match: *`, weak and listed ETags are 400, not RFC 9110's evaluation (decision 0015 says why).
4. LOW, fixed: a delete between the duplicate key and the version read gave a 428 without `currentVersion`; the create now retries the insert once (`createAttempts`).
5. LOW, documented: the two new CHECKs on `notifications` each rebuild the table (decision 0015 Numbers table); plan the rollout on a populated table.
The verifier confirmed the held acceptance patch's assertions against the code and the harness helpers.
