package intake

import (
	"slices"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// storedTemplates turns name -> body into the rows Validate reads, each at
// version 1.
func storedTemplates(bodies map[string]string) map[string]sqlc.Template {
	out := make(map[string]sqlc.Template, len(bodies))
	for name, body := range bodies {
		out[name] = sqlc.Template{Name: name, Body: body, Version: 1}
	}
	return out
}

var renderNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func TestMissingVariablesAreOneDetailListingRequiredMissingAndUnknown(t *testing.T) {
	templates := map[string]sqlc.Template{"order": {Name: "order", Version: 4,
		Body: "Hi {{name}}, order {{ order }} ships {{when}}. Thanks {{name}}."}}
	r := Request{To: "t", Channel: notification.ChannelPush, Template: &TemplateRef{Name: "order",
		Variables: map[string]string{"name": "Ada", "zeta": "1", "ordr": "42"}}}

	_, errs := Validate(r, templates, renderNow)
	if len(errs) != 1 {
		t.Fatalf("want one detail, got %+v", errs)
	}
	d := errs[0]
	if d.Field != "template.variables" || d.Reason == "" || d.Variables == nil {
		t.Fatalf("detail = %+v", d)
	}
	for _, c := range []struct {
		what      string
		got, want []string
	}{
		{"required (first appearance, once)", d.Variables.Required, []string{"name", "order", "when"}},
		{"missing (first appearance)", d.Variables.Missing, []string{"order", "when"}},
		{"unknown (sorted)", d.Variables.Unknown, []string{"ordr", "zeta"}},
	} {
		if !slices.Equal(c.got, c.want) {
			t.Errorf("%s = %v, want %v", c.what, c.got, c.want)
		}
	}
}

func TestMissingVariablesWithNoUnknownListAnEmptyUnknown(t *testing.T) {
	r := Request{To: "t", Channel: notification.ChannelPush, Template: &TemplateRef{Name: "w"}}
	_, errs := Validate(r, storedTemplates(map[string]string{"w": "Hello {{name}}"}), renderNow)
	if len(errs) != 1 || errs[0].Variables == nil {
		t.Fatalf("got %+v", errs)
	}
	if u := errs[0].Variables.Unknown; u == nil || len(u) != 0 {
		t.Fatalf("unknown = %#v, want an empty, non-nil slice (the API shows [])", u)
	}
}

func TestUnknownVariablesAloneAreAcceptedAndTheOriginIsRecorded(t *testing.T) {
	templates := map[string]sqlc.Template{"w": {Name: "w", Body: "Hi {{name}}", Version: 7}}
	r := Request{To: "t", Channel: notification.ChannelPush, Template: &TemplateRef{Name: "w",
		Variables: map[string]string{"name": "Ada", "unused": "x"}}}
	a, errs := Validate(r, templates, renderNow)
	if len(errs) > 0 {
		t.Fatalf("refused: %+v", errs)
	}
	if a.Content != "Hi Ada" {
		t.Fatalf("content = %q", a.Content)
	}
	if a.Template == nil || *a.Template != (TemplateOrigin{Name: "w", Version: 7}) {
		t.Fatalf("origin = %+v, want w at version 7", a.Template)
	}
}

func TestPlainContentHasNoTemplateOrigin(t *testing.T) {
	a, errs := Validate(Request{To: "t", Channel: notification.ChannelPush, Content: ptr("hi")}, nil, renderNow)
	if len(errs) > 0 || a.Template != nil {
		t.Fatalf("errs %+v origin %+v", errs, a.Template)
	}
}
