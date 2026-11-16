//go:build integration

package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/test/support"
)

// Decision 0015 ("As built") over HTTP against MySQL: the template version
// as body field and ETag, If-Match turning into 201, 200, 412 and 428, the
// detailed missing-variables refusal, and the template origin on
// notifications. It covers the Huma mapping until the held acceptance tests
// land (.agents/pr/template-versioning-acceptance.md).
func TestTemplateVersioningOverHTTP(t *testing.T) {
	db, _ := support.MySQL(t)
	h, err := NewServer(intake.New(db, nopPublisher{}, time.Hour), nil).Handler()
	if err != nil {
		t.Fatal(err)
	}
	do := func(t *testing.T, method, path, body string, header ...string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		for i := 0; i+1 < len(header); i += 2 {
			req.Header.Set(header[i], header[i+1])
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	wantTemplate := func(t *testing.T, rec *httptest.ResponseRecorder, status, version int) {
		t.Helper()
		var tpl Template
		if rec.Code != status || json.Unmarshal(rec.Body.Bytes(), &tpl) != nil {
			t.Fatalf("want %d with a template, got %d %s", status, rec.Code, rec.Body)
		}
		if tpl.Version != version || rec.Header().Get("ETag") != etagOf(uint32(version)) { //nolint:gosec // small test versions
			t.Fatalf("want version %d and ETag %s, got version %d ETag %q", version, etagOf(uint32(version)), tpl.Version, rec.Header().Get("ETag")) //nolint:gosec // small test versions
		}
	}
	wantRefusal := func(t *testing.T, rec *httptest.ResponseRecorder, status int, code string, current *int) {
		t.Helper()
		e := decodeError(t, rec)
		if rec.Code != status || e.Code != code || len(e.Details) != 1 || e.Details[0].Field != "If-Match" {
			t.Fatalf("want %d %s naming If-Match, got %d %s", status, code, rec.Code, rec.Body)
		}
		got := e.Details[0].CurrentVersion
		if (got == nil) != (current == nil) || (got != nil && *got != *current) {
			t.Fatalf("currentVersion = %v, want %v", got, current)
		}
	}
	one, two := 1, 2
	const path = "/v1/templates/greet"

	t.Run("create, refuse without If-Match, replace, refuse a stale one", func(t *testing.T) {
		wantTemplate(t, do(t, http.MethodPut, path, `{"body":"Hi {{name}}"}`), http.StatusCreated, 1)
		wantRefusal(t, do(t, http.MethodPut, path, `{"body":"Hello {{name}}"}`), http.StatusPreconditionRequired, "precondition_required", &one)
		wantTemplate(t, do(t, http.MethodPut, path, `{"body":"Hello {{name}}"}`, "If-Match", `"1"`), http.StatusOK, 2)
		wantRefusal(t, do(t, http.MethodPut, path, `{"body":"Lost"}`, "If-Match", `"1"`), http.StatusPreconditionFailed, "precondition_failed", &two)
		wantRefusal(t, do(t, http.MethodPut, "/v1/templates/ghost", `{"body":"x"}`, "If-Match", `"1"`), http.StatusPreconditionFailed, "precondition_failed", nil)
		wantTemplate(t, do(t, http.MethodGet, path, ""), http.StatusOK, 2)
	})

	t.Run("a malformed If-Match is 400 naming the header", func(t *testing.T) {
		for _, v := range []string{"*", `W/"2"`, "2", `"0"`} {
			rec := do(t, http.MethodPut, path, `{"body":"x"}`, "If-Match", v)
			if e := decodeError(t, rec); rec.Code != http.StatusBadRequest || len(e.Details) == 0 || e.Details[0].Field != "If-Match" {
				t.Fatalf("If-Match %s: got %d %s", v, rec.Code, rec.Body)
			}
		}
	})

	t.Run("missing variables are one detail with required, missing and an empty unknown", func(t *testing.T) {
		rec := do(t, http.MethodPost, "/v1/notifications", `{"to":"device-1","channel":"push","template":{"name":"greet"}}`)
		e := decodeError(t, rec)
		if rec.Code != http.StatusBadRequest || len(e.Details) != 1 {
			t.Fatalf("got %d %s", rec.Code, rec.Body)
		}
		d := e.Details[0]
		if d.Field != "template.variables" || !slices.Equal(d.Required, []string{"name"}) || !slices.Equal(d.Missing, []string{"name"}) {
			t.Fatalf("detail = %+v", d)
		}
		if !strings.Contains(rec.Body.String(), `"unknown":[]`) {
			t.Fatalf("unknown must be [] on the wire: %s", rec.Body)
		}
	})

	t.Run("a notification shows the template and version that rendered it; plain content none", func(t *testing.T) {
		rec := do(t, http.MethodPost, "/v1/notifications", `{"to":"device-1","channel":"push","template":{"name":"greet","variables":{"name":"Ada","extra":"ok"}}}`)
		var n Notification
		if rec.Code != http.StatusCreated || json.Unmarshal(rec.Body.Bytes(), &n) != nil {
			t.Fatalf("create from template: %d %s", rec.Code, rec.Body)
		}
		if n.Template == nil || n.Template.Name != "greet" || n.Template.Version != 2 || n.Content != "Hello Ada" {
			t.Fatalf("template %+v content %q, want greet v2 and Hello Ada", n.Template, n.Content)
		}
		plain := do(t, http.MethodPost, "/v1/notifications", `{"to":"device-1","channel":"push","content":"as is"}`)
		if plain.Code != http.StatusCreated || strings.Contains(plain.Body.String(), `"template"`) {
			t.Fatalf("plain create: %d %s", plain.Code, plain.Body)
		}
		list := do(t, http.MethodGet, "/v1/notifications?limit=200", "")
		var page NotificationPage
		if list.Code != http.StatusOK || json.Unmarshal(list.Body.Bytes(), &page) != nil {
			t.Fatalf("list: %d %s", list.Code, list.Body)
		}
		i := slices.IndexFunc(page.Items, func(it Notification) bool { return it.ID == n.ID })
		if i < 0 || page.Items[i].Template == nil || page.Items[i].Template.Version != 2 {
			t.Fatalf("list item for %s lacks its template: %s", n.ID, list.Body)
		}
	})
}
