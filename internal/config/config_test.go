package config

import (
	"strings"
	"testing"
	"time"
)

func env(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }

func TestEveryDialIsDocumentedAndHasADefaultOrIsInjected(t *testing.T) {
	for _, d := range Dials {
		if !strings.HasPrefix(d.Env, "APP_") || d.Doc == "" {
			t.Errorf("%s: needs the APP_ prefix and a doc line", d.Env)
		}
		if d.Secret && d.Default != "" {
			t.Errorf("%s: a secret must not have a committed default", d.Env)
		}
		if d.Default == "" && !d.Required && d.Env != "APP_OTLP_ENDPOINT" {
			t.Errorf("%s: needs a default or Required", d.Env)
		}
	}
}

func TestDefaultsLoadOnceSecretsAreInjected(t *testing.T) {
	c, err := Load(env(map[string]string{"APP_DATABASE_DSN": "u:p@tcp(db:3306)/app"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.ShutdownTimeout != 10*time.Second || c.DBMaxOpenConns != 10 {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

func TestLoadReportsEveryProblemAtOnce(t *testing.T) {
	_, err := Load(env(map[string]string{"APP_DB_MAX_OPEN_CONNS": "0", "APP_SHUTDOWN_TIMEOUT": "forever"}))
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"APP_DATABASE_DSN is required", "APP_DB_MAX_OPEN_CONNS", "APP_SHUTDOWN_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
