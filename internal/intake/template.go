package intake

import (
	"regexp"
	"strings"
)

// placeholder is `{{name}}`, spaces allowed inside the braces (decision 0015).
var placeholder = regexp.MustCompile(`\{\{\s*([A-Za-z0-9_.-]+)\s*\}\}`)

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
