//go:build integration

package acceptance

import (
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/core"
)

// B2: Scheduled Notifications, allow scheduling notifications for future delivery
// (intake half: a future scheduledAt becomes the due time; decision 0016).
func TestB2ScheduleAtIntake(t *testing.T) {
	s := startStack(t, stackOptions{})

	t.Run("future: status scheduled, dueAt equals scheduledAt", func(t *testing.T) {
		at := time.Now().UTC().Add(90 * time.Minute).Truncate(time.Millisecond)
		body := msg("push", "sale starts now")
		body["scheduledAt"] = at.Format(time.RFC3339Nano)
		n := s.create(t, body)
		got := s.get(t, n.ID)
		for _, x := range []notification{n, got} {
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
		if st := s.storedStatus(t, n.ID); st != "pending" {
			t.Errorf("stored status of a scheduled notification: want pending, got %q", st)
		}
	})

	t.Run("past: status pending", func(t *testing.T) {
		body := msg("sms", "late but fine")
		body["scheduledAt"] = time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
		n := s.create(t, body)
		if got := s.get(t, n.ID); got.Status != "pending" {
			t.Errorf("status: want pending, got %q", got.Status)
		}
	})

	t.Run("beyond the horizon: 400 naming scheduledAt", func(t *testing.T) {
		body := msg("email", "too far")
		body["scheduledAt"] = time.Now().UTC().Add(core.ScheduleHorizon + 2*time.Hour).Format(time.RFC3339)
		wantError(t, s.postJSON(t, "/v1/notifications", body), http.StatusBadRequest, "invalid_request", "scheduledAt")
	})
}

// putTemplate stores a template, requires 200.
func (s *stack) putTemplate(t *testing.T, name, body string) template {
	t.Helper()
	r := s.do(t, http.MethodPut, "/v1/templates/"+name, map[string]any{"body": body})
	if r.Status != http.StatusOK {
		t.Fatalf("PUT /v1/templates/%s: want 200, got %s", name, r)
	}
	var tp template
	r.decode(t, &tp)
	return tp
}

func withTemplate(channel, name string, vars map[string]any) map[string]any {
	ref := map[string]any{"name": name}
	if vars != nil {
		ref["variables"] = vars
	}
	return map[string]any{"to": recipientFor(channel), "channel": channel, "template": ref}
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

	r := s.getJSON(t, "/v1/templates/welcome")
	if r.Status != http.StatusOK {
		t.Fatalf("GET template: want 200, got %s", r)
	}
	var got template
	r.decode(t, &got)
	if got.Body != body {
		t.Errorf("GET body: want %q, got %q", body, got.Body)
	}

	s.putTemplate(t, "receipt", "Thanks {{name}}")
	r = s.getJSON(t, "/v1/templates")
	if r.Status != http.StatusOK {
		t.Fatalf("list templates: want 200, got %s", r)
	}
	var list struct {
		Items []template `json:"items"`
	}
	r.decode(t, &list)
	var names []string
	for _, x := range list.Items {
		names = append(names, x.Name)
	}
	if !slices.Equal(names, []string{"receipt", "welcome"}) {
		t.Errorf("list: want [receipt welcome] by name, got %v", names)
	}

	replaced := s.putTemplate(t, "welcome", "Hi {{name}}")
	if replaced.Body != "Hi {{name}}" {
		t.Errorf("replace: body %q", replaced.Body)
	}

	if r := s.do(t, http.MethodDelete, "/v1/templates/welcome", nil); r.Status != http.StatusNoContent {
		t.Fatalf("DELETE: want 204, got %s", r)
	}
	wantError(t, s.getJSON(t, "/v1/templates/welcome"), http.StatusNotFound, "not_found")
	wantError(t, s.do(t, http.MethodDelete, "/v1/templates/welcome", nil), http.StatusNotFound, "not_found")
}

// B3: Template System, message templates with variable substitution
// (a template plus variables produces the rendered content; a missing variable is refused).
func TestB3CreateFromTemplate(t *testing.T) {
	s := startStack(t, stackOptions{})
	s.putTemplate(t, "welcome", "Hello {{name}}, your code is {{code}}.")

	n := s.create(t, withTemplate("email", "welcome", map[string]any{"name": "Ada", "code": "4821"}))
	want := "Hello Ada, your code is 4821."
	if n.Content != want {
		t.Errorf("rendered content in the response: want %q, got %q", want, n.Content)
	}
	if got := s.get(t, n.ID); got.Content != want {
		t.Errorf("rendered content stored: want %q, got %q", want, got.Content)
	}

	t.Run("replacing the template does not change an accepted notification", func(t *testing.T) {
		s.putTemplate(t, "welcome", "Changed {{name}} {{code}}")
		if got := s.get(t, n.ID); got.Content != want {
			t.Errorf("after replacing the template: want %q, got %q", want, got.Content)
		}
	})

	t.Run("missing variable: 400 naming it", func(t *testing.T) {
		r := s.postJSON(t, "/v1/notifications", withTemplate("email", "welcome", map[string]any{"name": "Ada"}))
		wantError(t, r, http.StatusBadRequest, "invalid_request", "template.variables.code")
	})

	t.Run("unknown template: 400 naming template.name", func(t *testing.T) {
		r := s.postJSON(t, "/v1/notifications", withTemplate("sms", "no-such-template", map[string]any{"name": "Ada"}))
		wantError(t, r, http.StatusBadRequest, "invalid_request", "template.name")
	})

	t.Run("rendering past the channel limit: 400", func(t *testing.T) {
		// 1000 fixed characters plus a 700-character variable: 1700 > sms limit 1600.
		s.putTemplate(t, "long", strings.Repeat("x", 1000)+"{{tail}}")
		at := s.create(t, withTemplate("sms", "long", map[string]any{"tail": strings.Repeat("y", core.MaxSMSContentLen-1000)}))
		if len([]rune(at.Content)) != core.MaxSMSContentLen {
			t.Errorf("rendering at the limit: want %d characters, got %d", core.MaxSMSContentLen, len([]rune(at.Content)))
		}
		r := s.postJSON(t, "/v1/notifications", withTemplate("sms", "long", map[string]any{"tail": strings.Repeat("y", 700)}))
		wantError(t, r, http.StatusBadRequest, "invalid_request")
	})
}
