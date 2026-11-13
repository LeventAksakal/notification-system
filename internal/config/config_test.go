package config

import (
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/core"
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

func TestDeliveryDialsDefaultToTheContractTable(t *testing.T) {
	c, err := Load(env(map[string]string{"APP_DATABASE_DSN": "u:p@tcp(db:3306)/app"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range core.Channels {
		if c.ProviderURL(ch) != "http://localhost:8090/send" {
			t.Errorf("provider URL for %s: %q", ch, c.ProviderURL(ch))
		}
	}
	if c.PriorityWeights != [3]int{70, 20, 10} || c.BreakerFailureRatio != 0.5 || c.MaxAttempts != 5 ||
		c.RetryCap != 5*time.Minute || c.IdempotencyRetention != 24*time.Hour || c.WSSendBuffer != 256 {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

func TestBrokenDeliveryDialsAreRefused(t *testing.T) {
	for name, kv := range map[string][2]string{
		"two weights":          {"APP_PRIORITY_WEIGHTS", "70,30"},
		"weight over 100":      {"APP_PRIORITY_WEIGHTS", "101,0,0"},
		"weights sum to zero":  {"APP_PRIORITY_WEIGHTS", "0,0,0"},
		"ratio above one":      {"APP_BREAKER_FAILURE_RATIO", "1.5"},
		"ratio not a number":   {"APP_BREAKER_FAILURE_RATIO", "half"},
		"provider URL no host": {"APP_PROVIDER_URL_SMS", "send"},
		"retry cap below base": {"APP_RETRY_CAP", "10ms"},
		"zero concurrency":     {"APP_WORKER_CONCURRENCY", "0"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(env(map[string]string{"APP_DATABASE_DSN": "x", kv[0]: kv[1]}))
			if err == nil || !strings.Contains(err.Error(), kv[0]) {
				t.Fatalf("want an error naming %s, got %v", kv[0], err)
			}
		})
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
