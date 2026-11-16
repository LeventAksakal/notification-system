package intake

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// placeholder is `{{name}}`, spaces allowed inside the braces (decision 0015).
var placeholder = regexp.MustCompile(`\{\{\s*([A-Za-z0-9_.-]+)\s*\}\}`)

// TemplateOrigin names the template and version that rendered a
// notification's content (decision 0015, traceability).
type TemplateOrigin struct {
	Name    string
	Version uint32
}

// VariableReport is attached to a refused `template.variables`: what the
// template needs and what the request sent, so the caller can fix it in one go.
type VariableReport struct {
	Required []string // the template's variables, in order of first appearance
	Missing  []string // required and not sent, in order of first appearance
	Unknown  []string // sent and not used by the template, sorted; never nil
}

// Variables lists the variable names of a template body in order of first
// appearance, each once; an empty slice when there are none.
func Variables(body string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, m := range placeholder.FindAllStringSubmatch(body, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	return out
}

// Render substitutes every placeholder with its value. Values are inserted
// as they are, never rendered again. missing lists the variables the body
// needs that vars lacks, in order of first appearance; content is only
// meaningful when missing is empty. Extra variables are ignored.
func Render(body string, vars map[string]string) (content string, missing []string) {
	seen := map[string]bool{}
	var b strings.Builder
	last := 0
	for _, m := range placeholder.FindAllStringSubmatchIndex(body, -1) {
		name := body[m[2]:m[3]]
		b.WriteString(body[last:m[0]])
		last = m[1]
		v, ok := vars[name]
		if !ok {
			if !seen[name] {
				seen[name] = true
				missing = append(missing, name)
			}
			continue
		}
		b.WriteString(v)
	}
	b.WriteString(body[last:])
	return b.String(), missing
}

// unknownVariables lists the names in vars that required does not hold,
// sorted; an empty slice when there are none.
func unknownVariables(required []string, vars map[string]string) []string {
	out := []string{}
	for name := range vars {
		if !slices.Contains(required, name) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// renderRef renders a template reference against the stored templates. The
// origin is the template row that was rendered (body and version come from
// the same row, so the version recorded is the one rendered). Variables the
// template does not use are accepted; they are listed only when the request
// is refused for missing ones, where they usually reveal a typo.
func renderRef(ref TemplateRef, templates map[string]sqlc.Template) (string, *TemplateOrigin, []FieldError) {
	var errs []FieldError
	if len(ref.Variables) > notification.MaxTemplateVariables {
		errs = append(errs, FieldError{Field: "template.variables", Reason: fmt.Sprintf("at most %d variables", notification.MaxTemplateVariables)})
	}
	for name, v := range ref.Variables {
		if utf8.RuneCountInString(v) > notification.MaxTemplateVariableLen {
			errs = append(errs, FieldError{Field: "template.variables." + name,
				Reason: fmt.Sprintf("must be at most %d characters", notification.MaxTemplateVariableLen)})
		}
	}
	t, ok := templates[ref.Name]
	if !ok {
		return "", nil, append(errs, FieldError{Field: "template.name", Reason: "no template with this name"})
	}
	content, missing := Render(t.Body, ref.Variables)
	if len(missing) > 0 {
		required := Variables(t.Body)
		report := &VariableReport{Required: required, Missing: missing, Unknown: unknownVariables(required, ref.Variables)}
		reason := "missing " + strings.Join(missing, ", ")
		if len(report.Unknown) > 0 {
			reason += "; not used by the template: " + strings.Join(report.Unknown, ", ")
		}
		errs = append(errs, FieldError{Field: "template.variables", Reason: reason, Variables: report})
	}
	return content, &TemplateOrigin{Name: t.Name, Version: t.Version}, errs
}
