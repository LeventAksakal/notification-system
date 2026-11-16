//go:build integration

package acceptance

import (
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"testing"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/test/apiclient"
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
	full := map[string]string{"last": "Lovelace", "first": "Ada", "code": "4821"}
	const rendered = "Dear Lovelace, Ada: your code is 4821. Regards to Lovelace."

	t.Run("single: 400 with one detail listing required, missing and unknown", func(t *testing.T) {
		vars := map[string]string{"first": "Ada", "zeta": "z", "alpha": "a", "mid": "m"}
		r := s.postCreate(t, withTemplate("email", "letter", vars))
		e := wantError(t, r, http.StatusBadRequest, "invalid_request", "template.variables")
		if n := len(deref(e.Details)); n != 1 {
			t.Errorf("details: want exactly one, got %d: %s", n, show(r))
		}
		wantVariablesDetail(t, e, nil, letterRequired, []string{"last", "code"}, []string{"alpha", "mid", "zeta"})
	})

	t.Run("single, no variables sent: all missing, unknown is []", func(t *testing.T) {
		r := s.postCreate(t, withTemplate("sms", "letter", nil))
		e := wantError(t, r, http.StatusBadRequest, "invalid_request", "template.variables")
		if n := len(deref(e.Details)); n != 1 {
			t.Errorf("details: want exactly one, got %d: %s", n, show(r))
		}
		wantVariablesDetail(t, e, nil, letterRequired, letterRequired, []string{})
	})

	t.Run("batch: one detail per refused item, with its index", func(t *testing.T) {
		items := []apiclient.NotificationCreate{
			msg("sms", "b3 batch plain"),
			withTemplate("email", "letter", map[string]string{"first": "Ada"}),
			withTemplate("email", "letter", full),
			withTemplate("push", "letter", map[string]string{"last": "Lovelace", "code": "1", "frist": "typo"}),
		}
		r := s.createBatchResp(t, items)
		e := wantError(t, r, http.StatusBadRequest, "invalid_request", "template.variables")
		perIndex := map[int]int{}
		for _, d := range deref(e.Details) {
			if d.Index == nil {
				t.Errorf("batch detail without index: %s", jsonOf(d))
				continue
			}
			perIndex[*d.Index]++
		}
		if want := map[int]int{1: 1, 3: 1}; !maps.Equal(perIndex, want) {
			t.Errorf("details per item index: want %v, got %v (%s)", want, perIndex, show(r))
		}
		one, three := 1, 3
		wantVariablesDetail(t, e, &one, letterRequired, []string{"last", "code"}, []string{})
		wantVariablesDetail(t, e, &three, letterRequired, []string{"first"}, []string{"frist"})
	})

	t.Run("unknown variables alone: 201, same response fields as without them", func(t *testing.T) {
		exact := s.createResp(t, withTemplate("email", "letter", full))
		if exact.StatusCode() != http.StatusCreated {
			t.Fatalf("exact variables: want 201, got %s", show(exact))
		}
		extra := map[string]string{"zeta": "z", "alpha": "a"}
		maps.Copy(extra, full)
		r := s.createResp(t, withTemplate("email", "letter", extra))
		if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
			t.Fatalf("unknown variables only: want 201, got %s", show(r))
		}
		if r.JSON201.Content != rendered {
			t.Errorf("content: want %q, got %q", rendered, r.JSON201.Content)
		}
		if got := s.get(t, r.JSON201.Id); got.Content != rendered {
			t.Errorf("stored content: want %q, got %q", rendered, got.Content)
		}
		a := slices.Sorted(maps.Keys(rawObject(t, exact.Body)))
		b := slices.Sorted(maps.Keys(rawObject(t, r.Body)))
		if !slices.Equal(a, b) {
			t.Errorf("response fields: with unknown variables %v, without %v", b, a)
		}
	})
}

// rawObject decodes a JSON object body, for checks the generated types cannot
// make (a field absent rather than empty).
func rawObject(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("not a JSON object: %v: %s", err, truncate(body))
	}
	return m
}

// wantVariablesDetail requires exactly one template.variables detail at index
// (nil: a detail without index) and checks required, missing and unknown;
// unknown must be present ([]) even when empty.
func wantVariablesDetail(t *testing.T, e apiclient.Error, index *int, required, missing, unknown []string) {
	t.Helper()
	where := "single request"
	if index != nil {
		where = "item " + strconv.Itoa(*index)
	}
	var found []apiclient.ErrorDetail
	for _, d := range deref(e.Details) {
		if d.Field != "template.variables" || (index == nil) != (d.Index == nil) {
			continue
		}
		if index != nil && *d.Index != *index {
			continue
		}
		found = append(found, d)
	}
	if len(found) != 1 {
		t.Errorf("%s: want one template.variables detail, got %d: %s", where, len(found), jsonOf(e.Details))
		return
	}
	d := found[0]
	if got := deref(d.Required); !slices.Equal(got, required) {
		t.Errorf("%s: required: want %q, got %q", where, required, got)
	}
	if got := deref(d.Missing); !slices.Equal(got, missing) {
		t.Errorf("%s: missing: want %q, got %q", where, missing, got)
	}
	if d.Unknown == nil {
		t.Errorf("%s: unknown: want %q, got absent or null", where, unknown)
	} else if !slices.Equal(*d.Unknown, unknown) {
		t.Errorf("%s: unknown: want %q, got %q", where, unknown, *d.Unknown)
	}
}

// wantETag requires the strong ETag "N" for version N.
func wantETag(t *testing.T, h http.Header, version int) {
	t.Helper()
	want := `"` + strconv.Itoa(version) + `"`
	if got := h.Get("ETag"); got != want {
		t.Errorf("ETag: want %s, got %q", want, got)
	}
}

// wantIfMatchDetail requires details[0] to name If-Match with currentVersion.
func wantIfMatchDetail(t *testing.T, e apiclient.Error, current int) {
	t.Helper()
	ds := deref(e.Details)
	if len(ds) == 0 {
		t.Errorf("want details[0] naming If-Match, got no details")
		return
	}
	d := ds[0]
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
	r := answered(t)(s.API.GetTemplateWithResponse(t.Context(), name)).(*apiclient.GetTemplateResponse)
	if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
		t.Fatalf("GET /v1/templates/%s: want 200, got %s", name, show(r))
	}
	if tp := r.JSON200; tp.Version != version || tp.Body != body {
		t.Errorf("GET %s: want version %d body %q, got version %d body %q", name, version, body, tp.Version, tp.Body)
	}
	wantETag(t, r.HTTPResponse.Header, version)
}

// B3: Template System, message templates with variable substitution
// (every template has a version, 1 on create and +1 per update, sent as a
// strong ETag; replacing needs If-Match at the current version: 428 without
// it, 412 when stale; decision 0015).
func TestB3TemplateVersioning(t *testing.T) {
	s := startStack(t, stackOptions{})
	const v1, v2 = "Hello {{name}}", "Hi {{name}}, v2"

	r := s.putTemplateResp(t, "greet", v1, "")
	if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
		t.Fatalf("PUT without If-Match on a new template: want 201, got %s", show(r))
	}
	if r.JSON201.Version != 1 {
		t.Errorf("created: version want 1, got %d", r.JSON201.Version)
	}
	wantETag(t, r.HTTPResponse.Header, 1)
	s.wantStoredTemplate(t, "greet", 1, v1)

	t.Run("PUT without If-Match on an existing template: 428 with currentVersion, unchanged", func(t *testing.T) {
		r := s.putTemplateResp(t, "greet", "overwrite {{name}}", "")
		e := wantError(t, r, http.StatusPreconditionRequired, "precondition_required")
		wantIfMatchDetail(t, e, 1)
		s.wantStoredTemplate(t, "greet", 1, v1)
	})

	t.Run("If-Match at the current version: 200, version 2", func(t *testing.T) {
		r := s.putTemplateResp(t, "greet", v2, `"1"`)
		if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
			t.Fatalf(`PUT with If-Match "1": want 200, got %s`, show(r))
		}
		if tp := r.JSON200; tp.Version != 2 || tp.Body != v2 {
			t.Errorf("PUT response: want version 2 body %q, got version %d body %q", v2, tp.Version, tp.Body)
		}
		wantETag(t, r.HTTPResponse.Header, 2)
		s.wantStoredTemplate(t, "greet", 2, v2)
	})

	t.Run("stale If-Match: 412 with currentVersion, unchanged", func(t *testing.T) {
		r := s.putTemplateResp(t, "greet", "stale {{name}}", `"1"`)
		e := wantError(t, r, http.StatusPreconditionFailed, "precondition_failed")
		wantIfMatchDetail(t, e, 2)
		s.wantStoredTemplate(t, "greet", 2, v2)
	})

	t.Run("If-Match on a template that does not exist: 412 without currentVersion, nothing created", func(t *testing.T) {
		r := s.putTemplateResp(t, "ghost", "boo {{name}}", `"1"`)
		e := wantError(t, r, http.StatusPreconditionFailed, "precondition_failed")
		for _, d := range deref(e.Details) {
			if d.CurrentVersion != nil {
				t.Errorf("detail %q: want no currentVersion, got %d", d.Field, *d.CurrentVersion)
			}
		}
		wantError(t, answered(t)(s.API.GetTemplateWithResponse(t.Context(), "ghost")), http.StatusNotFound, "not_found")
	})

	t.Run("malformed If-Match: 400 naming If-Match, unchanged", func(t *testing.T) {
		for _, tc := range []struct{ name, value string }{
			{"wildcard", `*`},
			{"weak", `W/"2"`},
			{"unquoted", `2`},
			{"zero", `"0"`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := s.putTemplateResp(t, "greet", "malformed {{name}}", tc.value)
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
		lr := answered(t)(s.API.ListTemplatesWithResponse(t.Context())).(*apiclient.ListTemplatesResponse)
		if lr.StatusCode() != http.StatusOK || lr.JSON200 == nil {
			t.Fatalf("list templates: want 200, got %s", show(lr))
		}
		got := map[string]int{}
		for _, x := range lr.JSON200.Items {
			got[x.Name] = x.Version
		}
		if want := map[string]int{"aaa-other": 1, "greet": 3}; !maps.Equal(got, want) {
			t.Errorf("versions in the list: want %v, got %v", want, got)
		}
	})
}

// rawItemsByID indexes a page's raw items by id.
func rawItemsByID(t *testing.T, body []byte) map[string]map[string]any {
	t.Helper()
	var page struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("not a page: %v: %s", err, truncate(body))
	}
	out := make(map[string]map[string]any, len(page.Items))
	for _, it := range page.Items {
		if id, ok := it["id"].(string); ok {
			out[id] = it
		}
	}
	return out
}

// wantOrigin requires a raw notification object to carry template {name, version}.
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

// wantNoOrigin requires a raw notification object to have no template field.
func wantNoOrigin(t *testing.T, where string, m map[string]any) {
	t.Helper()
	if v, ok := m["template"]; ok {
		t.Errorf("%s: plain-content notification has template %v, want no field", where, v)
	}
}

// getRaw is GET /v1/notifications/{id}, required 200, as a raw object.
func (s *stack) getRaw(t *testing.T, id uuid.UUID) map[string]any {
	t.Helper()
	r := answered(t)(s.API.GetNotificationWithResponse(t.Context(), id))
	if r.StatusCode() != http.StatusOK {
		t.Fatalf("GET /v1/notifications/%s: want 200, got %s", id, show(r))
	}
	return rawObject(t, r.GetBody())
}

// B3: Template System, message templates with variable substitution
// (a notification records the template name and version that rendered it;
// plain content has none; an update does not touch earlier notifications).
func TestB3TemplateTraceability(t *testing.T) {
	s := startStack(t, stackOptions{})
	s.putTemplate(t, "greet", "Hello {{name}}")
	vars := map[string]string{"name": "Ada"}

	r := s.createResp(t, withTemplate("email", "greet", vars))
	if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
		t.Fatalf("create from template v1: want 201, got %s", show(r))
	}
	wantOrigin(t, "create response (v1)", rawObject(t, r.Body), "greet", 1)
	older := *r.JSON201
	if o := older.Template; o == nil || o.Name != "greet" || o.Version != 1 {
		t.Errorf("create response (v1), typed: template want {greet 1}, got %s", jsonOf(o))
	}
	wantOrigin(t, "GET (v1)", s.getRaw(t, older.Id), "greet", 1)

	r = s.createResp(t, msg("sms", "b3 trace plain"))
	if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
		t.Fatalf("create plain: want 201, got %s", show(r))
	}
	wantNoOrigin(t, "create response (plain)", rawObject(t, r.Body))
	plain := *r.JSON201
	wantNoOrigin(t, "GET (plain)", s.getRaw(t, plain.Id))

	s.replaceTemplate(t, "greet", "Hi {{name}}!")
	r = s.createResp(t, withTemplate("email", "greet", vars))
	if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
		t.Fatalf("create from template v2: want 201, got %s", show(r))
	}
	wantOrigin(t, "create response (v2)", rawObject(t, r.Body), "greet", 2)
	newer := *r.JSON201
	if newer.Content != "Hi Ada!" {
		t.Errorf("v2 content: want %q, got %q", "Hi Ada!", newer.Content)
	}
	wantOrigin(t, "GET (v2)", s.getRaw(t, newer.Id), "greet", 2)

	t.Run("the earlier notification keeps version 1 and its content", func(t *testing.T) {
		m := s.getRaw(t, older.Id)
		wantOrigin(t, "GET earlier", m, "greet", 1)
		if m["content"] != "Hello Ada" {
			t.Errorf("earlier content: want %q, got %v", "Hello Ada", m["content"])
		}
	})

	t.Run("list items carry template; plain ones have none", func(t *testing.T) {
		lr := answered(t)(s.API.ListNotificationsWithResponse(t.Context(), &apiclient.ListNotificationsParams{Limit: ptr(200)}))
		if lr.StatusCode() != http.StatusOK {
			t.Fatalf("list: want 200, got %s", show(lr))
		}
		byID := rawItemsByID(t, lr.GetBody())
		for _, c := range []struct {
			id      uuid.UUID
			version int
		}{{older.Id, 1}, {newer.Id, 2}} {
			m, ok := byID[c.id.String()]
			if !ok {
				t.Errorf("list: %s missing", c.id)
				continue
			}
			wantOrigin(t, "list item "+c.id.String(), m, "greet", c.version)
		}
		if m, ok := byID[plain.Id.String()]; !ok {
			t.Errorf("list: plain %s missing", plain.Id)
		} else {
			wantNoOrigin(t, "list item (plain)", m)
		}
	})

	t.Run("batch items carry template; plain ones have none", func(t *testing.T) {
		b := s.createBatch(t, []apiclient.NotificationCreate{withTemplate("push", "greet", vars), msg("email", "b3 trace batch plain")})
		if len(b.NotificationIds) != 2 {
			t.Fatalf("batch: want 2 notification ids, got %v", b.NotificationIds)
		}
		br := answered(t)(s.API.GetBatchWithResponse(t.Context(), b.Id, nil))
		if br.StatusCode() != http.StatusOK {
			t.Fatalf("GET batch: want 200, got %s", show(br))
		}
		byID := rawItemsByID(t, br.GetBody())
		first, second := b.NotificationIds[0], b.NotificationIds[1]
		if m, ok := byID[first.String()]; !ok {
			t.Errorf("batch items: item 0 missing")
		} else {
			wantOrigin(t, "batch item 0", m, "greet", 2)
		}
		if m, ok := byID[second.String()]; !ok {
			t.Errorf("batch items: item 1 missing")
		} else {
			wantNoOrigin(t, "batch item 1", m)
		}
		wantOrigin(t, "GET batch item 0", s.getRaw(t, first), "greet", 2)
		wantNoOrigin(t, "GET batch item 1", s.getRaw(t, second))
	})
}
