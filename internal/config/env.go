package config

import (
	"os"
	"slices"
	"strings"
)

// localDSNPlaceholder matches the local-development DSN in docker-compose.yml.
// It is a fake value for local compose, not a real secret.
const localDSNPlaceholder = "app:local-dev@tcp(mysql:3306)/app"

// EnvExample renders every dial, in declaration order, as an env file: a
// comment with the doc line, then the setting. Settings with a default are
// commented out (the default applies when unset); a required setting is
// written with a placeholder for local compose. It is generated from Dials so
// it cannot go stale.
func EnvExample() string {
	var b strings.Builder
	b.WriteString("# Every application setting, generated from internal/config (go run ./cmd/app env).\n")
	b.WriteString("# The app process reads these; docker compose reads its own few variables from .env.\n")
	for _, d := range Dials {
		b.WriteString("\n# ")
		b.WriteString(d.Doc)
		b.WriteString("\n")
		if d.Secret {
			b.WriteString("# Secret: or set " + d.Env + FileSuffix + "=/path/to/file (a file mounted by the secrets manager;\n" +
				"# its content, one trailing newline trimmed, is the value). Never both.\n")
		}
		if d.Required && d.Default == "" {
			b.WriteString(d.Env + "=" + placeholder(d) + "\n")
			continue
		}
		b.WriteString("# " + d.Env + "=" + d.Default + "\n")
	}
	return b.String()
}

// Check validates environ (os.Environ's KEY=value form) against Dials without
// connecting to anything: everything Load checks, plus every APP_ variable
// that is not a declared setting (or a secret's _FILE form), which is how a
// typo in a deploy manifest is caught. It returns one line per problem, or,
// when there is none, one line per setting with its value and source; a
// secret's value is printed as "(set)", never itself.
func Check(environ []string) (lines []string, ok bool) {
	return check(environ, os.ReadFile)
}

func check(environ []string, readFile func(string) ([]byte, error)) ([]string, bool) {
	vars := make(map[string]string, len(environ))
	known := make(map[string]bool, 2*len(Dials))
	for _, d := range Dials {
		known[d.Env] = true
		if d.Secret {
			known[d.Env+FileSuffix] = true
		}
	}
	var problems []string
	for _, kv := range environ {
		name, value, _ := strings.Cut(kv, "=")
		vars[name] = value
		if strings.HasPrefix(name, "APP_") && !known[name] {
			problems = append(problems, name+": unknown setting (not declared in internal/config; see app env)")
		}
	}
	slices.Sort(problems)
	_, settings, errs := load(func(k string) string { return vars[k] }, readFile)
	for _, err := range errs {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		return problems, false
	}
	lines := make([]string, len(Dials))
	for i, d := range Dials {
		raw := settings[i].raw
		if d.Secret && raw != "" {
			raw = "(set)"
		}
		lines[i] = d.Env + "=" + raw + "  # " + string(settings[i].src)
	}
	return lines, true
}

func placeholder(d Dial) string {
	if d.Secret {
		return localDSNPlaceholder
	}
	return "change-me"
}
