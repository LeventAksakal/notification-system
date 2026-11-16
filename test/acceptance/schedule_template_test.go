//go:build integration

package acceptance

import (
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// B2: Scheduled Notifications, allow scheduling notifications for future delivery
// (intake half: a future scheduledAt becomes the due time; decision 0016).
func TestB2ScheduleAtIntake(t *testing.T) {
	s := startStack(t, stackOptions{})

	t.Run("future: status scheduled, dueAt equals scheduledAt", func(t *testing.T) {
		at := time.Now().UTC().Add(90 * time.Minute).Truncate(time.Millisecond)
		body := msg("push", "sale starts now")
		body.ScheduledAt = &at
		n := s.create(t, body)
		got := s.get(t, n.Id)
		for _, x := range []apiclient.Notification{n, got} {
			if x.Status != "scheduled" {
				t.Errorf("status: want scheduled, got %q", x.Status)
			}
			if !x.DueAt.Equal(at) {
				t.Errorf("dueAt: want %s, got %s", at.Format(time.RFC3339Nano), x.DueAt.Format(time.RFC3339Nano))
			}
			if x.ScheduledAt == nil || !x.ScheduledAt.Equal(at) {
				t.Errorf("scheduledAt: want %s, got %v", at.Format(time.RFC3339Nano), x.ScheduledAt)
			}
		}
		if st := s.storedStatus(t, n.Id); st != "pending" {
			t.Errorf("stored status of a scheduled notification: want pending, got %q", st)
		}
	})

	t.Run("past: status pending", func(t *testing.T) {
		body := msg("sms", "late but fine")
		body.ScheduledAt = ptr(time.Now().UTC().Add(-time.Hour).Truncate(time.Second))
		n := s.create(t, body)
		if got := s.get(t, n.Id); got.Status != "pending" {
			t.Errorf("status: want pending, got %q", got.Status)
		}
	})

	t.Run("beyond the horizon: 400 naming scheduledAt", func(t *testing.T) {
		body := msg("email", "too far")
		body.ScheduledAt = ptr(time.Now().UTC().Add(notification.ScheduleHorizon + 2*time.Hour).Truncate(time.Second))
		wantError(t, s.postCreate(t, body), http.StatusBadRequest, "invalid_request", "scheduledAt")
	})
}

// putTemplateResp sends PUT /v1/templates/{name}, with If-Match when ifMatch
// is not empty; the caller checks the answer.
func (s *stack) putTemplateResp(t *testing.T, name, body, ifMatch string) *apiclient.PutTemplateResponse {
	t.Helper()
	var p *apiclient.PutTemplateParams
	if ifMatch != "" {
		p = &apiclient.PutTemplateParams{IfMatch: &ifMatch}
	}
	r, err := s.API.PutTemplateWithResponse(t.Context(), name, p, apiclient.TemplateWrite{Body: body})
	if err != nil {
		t.Fatalf("PUT /v1/templates/%s (If-Match %q): %v", name, ifMatch, err)
	}
	return r
}

// putTemplate creates a template (PUT without If-Match), requires 201.
func (s *stack) putTemplate(t *testing.T, name, body string) apiclient.Template {
	t.Helper()
	r := s.putTemplateResp(t, name, body, "")
	if r.StatusCode() != http.StatusCreated || r.JSON201 == nil {
		t.Fatalf("PUT /v1/templates/%s (create): want 201, got %s", name, show(r))
	}
	return *r.JSON201
}

// replaceTemplate replaces an existing template: it reads the current ETag,
// sends it as If-Match, and requires 200 and a version one higher.
func (s *stack) replaceTemplate(t *testing.T, name, body string) apiclient.Template {
	t.Helper()
	g := answered(t)(s.API.GetTemplateWithResponse(t.Context(), name)).(*apiclient.GetTemplateResponse)
	if g.StatusCode() != http.StatusOK || g.JSON200 == nil {
		t.Fatalf("GET /v1/templates/%s before replacing: want 200, got %s", name, show(g))
	}
	etag := g.HTTPResponse.Header.Get("ETag")
	if etag == "" {
		t.Fatalf("GET /v1/templates/%s: no ETag header", name)
	}
	r := s.putTemplateResp(t, name, body, etag)
	if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
		t.Fatalf("PUT /v1/templates/%s with If-Match %s: want 200, got %s", name, etag, show(r))
	}
	if r.JSON200.Version != g.JSON200.Version+1 {
		t.Errorf("replace %s: version want %d, got %d", name, g.JSON200.Version+1, r.JSON200.Version)
	}
	return *r.JSON200
}

func withTemplate(channel, name string, vars map[string]string) apiclient.NotificationCreate {
	ref := apiclient.TemplateRef{Name: name}
	if vars != nil {
		ref.Variables = &vars
	}
	return apiclient.NotificationCreate{To: recipientFor(channel), Channel: apiclient.Channel(channel), Template: &ref}
}

// B3: Template System, message templates with variable substitution
// (template CRUD).
func TestB3TemplateCRUD(t *testing.T) {
	s := startStack(t, stackOptions{})

	body := "Hello {{name}}, your code is {{code}}."
	tp := s.putTemplate(t, "welcome", body)
	if tp.Name != "welcome" || tp.Body != body {
		t.Errorf("PUT response: got name %q body %q", tp.Name, tp.Body)
	}
	vars := slices.Clone(tp.Variables)
	slices.Sort(vars)
	if !slices.Equal(vars, []string{"code", "name"}) {
		t.Errorf("variables: want [code name], got %v", tp.Variables)
	}

	r, err := s.API.GetTemplateWithResponse(t.Context(), "welcome")
	if err != nil {
		t.Fatalf("GET template: %v", err)
	}
	if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
		t.Fatalf("GET template: want 200, got %s", show(r))
	}
	got := *r.JSON200
	if got.Body != body {
		t.Errorf("GET body: want %q, got %q", body, got.Body)
	}

	s.putTemplate(t, "receipt", "Thanks {{name}}")
	lr, err := s.API.ListTemplatesWithResponse(t.Context())
	if err != nil {
		t.Fatalf("list templates: %v", err)
	}
	if lr.StatusCode() != http.StatusOK || lr.JSON200 == nil {
		t.Fatalf("list templates: want 200, got %s", show(lr))
	}
	var names []string
	for _, x := range lr.JSON200.Items {
		names = append(names, x.Name)
	}
	if !slices.Equal(names, []string{"receipt", "welcome"}) {
		t.Errorf("list: want [receipt welcome] by name, got %v", names)
	}

	replaced := s.replaceTemplate(t, "welcome", "Hi {{name}}")
	if replaced.Body != "Hi {{name}}" {
		t.Errorf("replace: body %q", replaced.Body)
	}

	if r := answered(t)(s.API.DeleteTemplateWithResponse(t.Context(), "welcome")); r.StatusCode() != http.StatusNoContent {
		t.Fatalf("DELETE: want 204, got %s", show(r))
	}
	wantError(t, answered(t)(s.API.GetTemplateWithResponse(t.Context(), "welcome")), http.StatusNotFound, "not_found")
	wantError(t, answered(t)(s.API.DeleteTemplateWithResponse(t.Context(), "welcome")), http.StatusNotFound, "not_found")
}

// B3: Template System, message templates with variable substitution
// (a template plus variables produces the rendered content; a missing variable is refused).
func TestB3CreateFromTemplate(t *testing.T) {
	s := startStack(t, stackOptions{})
	s.putTemplate(t, "welcome", "Hello {{name}}, your code is {{code}}.")

	n := s.create(t, withTemplate("email", "welcome", map[string]string{"name": "Ada", "code": "4821"}))
	want := "Hello Ada, your code is 4821."
	if n.Content != want {
		t.Errorf("rendered content in the response: want %q, got %q", want, n.Content)
	}
	if got := s.get(t, n.Id); got.Content != want {
		t.Errorf("rendered content stored: want %q, got %q", want, got.Content)
	}

	t.Run("replacing the template does not change an accepted notification", func(t *testing.T) {
		s.replaceTemplate(t, "welcome", "Changed {{name}} {{code}}")
		if got := s.get(t, n.Id); got.Content != want {
			t.Errorf("after replacing the template: want %q, got %q", want, got.Content)
		}
	})

	t.Run("missing variable: 400 naming it", func(t *testing.T) {
		r := s.postCreate(t, withTemplate("email", "welcome", map[string]string{"name": "Ada"}))
		e := wantError(t, r, http.StatusBadRequest, "invalid_request", "template.variables")
		if !slices.ContainsFunc(deref(e.Details), func(d apiclient.ErrorDetail) bool {
			return d.Field == "template.variables" && slices.Equal(deref(d.Missing), []string{"code"})
		}) {
			t.Errorf("details: want template.variables with missing [code], got %s", jsonOf(e.Details))
		}
	})

	t.Run("unknown template: 400 naming template.name", func(t *testing.T) {
		r := s.postCreate(t, withTemplate("sms", "no-such-template", map[string]string{"name": "Ada"}))
		wantError(t, r, http.StatusBadRequest, "invalid_request", "template.name")
	})

	t.Run("rendering past the channel limit: 400", func(t *testing.T) {
		// 1000 fixed characters plus a 700-character variable: 1700 > sms limit 1600.
		s.putTemplate(t, "long", strings.Repeat("x", 1000)+"{{tail}}")
		at := s.create(t, withTemplate("sms", "long", map[string]string{"tail": strings.Repeat("y", notification.MaxSMSContentLen-1000)}))
		if len([]rune(at.Content)) != notification.MaxSMSContentLen {
			t.Errorf("rendering at the limit: want %d characters, got %d", notification.MaxSMSContentLen, len([]rune(at.Content)))
		}
		r := s.postCreate(t, withTemplate("sms", "long", map[string]string{"tail": strings.Repeat("y", 700)}))
		wantError(t, r, http.StatusBadRequest, "invalid_request")
	})
}
