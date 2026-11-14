package config

import "strings"

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
		if d.Required && d.Default == "" {
			b.WriteString(d.Env + "=" + placeholder(d) + "\n")
			continue
		}
		b.WriteString("# " + d.Env + "=" + d.Default + "\n")
	}
	return b.String()
}

func placeholder(d Dial) string {
	if d.Secret {
		return localDSNPlaceholder
	}
	return "change-me"
}
