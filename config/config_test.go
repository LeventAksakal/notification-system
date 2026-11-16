package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func env(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }

func TestEveryDialIsDocumentedAndHasADefaultOrIsInjected(t *testing.T) {
	for _, d := range Dials {
		prefix := "APP_"
		if d.PassThrough {
			prefix = "OTEL_" // a library's own variable, declared here only to document and validate it
		}
		if !strings.HasPrefix(d.Env, prefix) || d.Doc == "" {
			t.Errorf("%s: needs the %s prefix and a doc line", d.Env, prefix)
		}
		if d.PassThrough && (d.Secret || d.Default == "") {
			t.Errorf("%s: a pass-through setting is not a secret and its default is the library's own", d.Env)
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
	if c.HTTPAddr != ":8080" || c.ShutdownTimeout != 10*time.Second || c.DBMaxOpenConns != 10 || c.RequestTimeout != 15*time.Second {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

func TestDeliveryDialsDefaultToTheContractTable(t *testing.T) {
	c, err := Load(env(map[string]string{"APP_DATABASE_DSN": "u:p@tcp(db:3306)/app"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range notification.Channels {
		if c.ProviderURL(ch) != "http://localhost:8090/send" {
			t.Errorf("provider URL for %s: %q", ch, c.ProviderURL(ch))
		}
	}
	if c.PriorityWeights != [3]int{70, 20, 10} || c.BreakerFailures != 5 || c.MaxAttempts != 10 ||
		c.RetryCap != 5*time.Minute || c.IdempotencyRetention != 24*time.Hour || c.WSSendBuffer != 256 ||
		c.RateLimitHorizon != 200*time.Millisecond || c.RateLimitHeadroomPct != 5 {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

func TestBrokenDeliveryDialsAreRefused(t *testing.T) {
	for name, kv := range map[string][2]string{
		"two weights":               {"APP_PRIORITY_WEIGHTS", "70,30"},
		"weight over 100":           {"APP_PRIORITY_WEIGHTS", "101,0,0"},
		"weights sum to zero":       {"APP_PRIORITY_WEIGHTS", "0,0,0"},
		"breaker failures zero":     {"APP_BREAKER_FAILURES", "0"},
		"breaker failures a ratio":  {"APP_BREAKER_FAILURES", "0.5"},
		"provider URL no host":      {"APP_PROVIDER_URL_SMS", "send"},
		"retry cap below base":      {"APP_RETRY_CAP", "10ms"},
		"zero concurrency":          {"APP_WORKER_CONCURRENCY", "0"},
		"horizon above 2s":          {"APP_RATE_LIMIT_HORIZON", "3s"},
		"horizon below 10ms":        {"APP_RATE_LIMIT_HORIZON", "5ms"},
		"lease below 1s":            {"APP_LEASE_DURATION", "500ms"},
		"request timeout below 1s":  {"APP_REQUEST_TIMEOUT", "500ms"},
		"request timeout above 5m":  {"APP_REQUEST_TIMEOUT", "6m"},
		"retention below an hour":   {"APP_NOTIFICATION_RETENTION", "30m"},
		"retention below the key's": {"APP_NOTIFICATION_RETENTION", "2h"}, // the key's default is 24h
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

// secretFile writes content to a file in a fresh temp dir and returns its path.
func secretFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestASecretIsReadFromTheFileItsFileVariableNames(t *testing.T) {
	const dsn = "u:s3cret-file@tcp(db:3306)/app"
	missing := filepath.Join(t.TempDir(), "absent")
	for name, tc := range map[string]struct {
		kv      map[string]string
		want    string // the DSN loaded, when wantErr is empty
		wantErr []string
	}{
		"file content is the value": {kv: map[string]string{"APP_DATABASE_DSN_FILE": secretFile(t, dsn)}, want: dsn},
		"one trailing newline is trimmed": {
			kv: map[string]string{"APP_DATABASE_DSN_FILE": secretFile(t, dsn+"\n")}, want: dsn},
		"only one trailing newline is trimmed": {
			kv: map[string]string{"APP_DATABASE_DSN_FILE": secretFile(t, dsn+"\n\n")}, want: dsn + "\n"},
		"a CRLF ending counts as one newline": {
			kv: map[string]string{"APP_DATABASE_DSN_FILE": secretFile(t, dsn+"\r\n")}, want: dsn},
		"the plain variable still works": {kv: map[string]string{"APP_DATABASE_DSN": dsn}, want: dsn},
		"both forms set": {
			kv:      map[string]string{"APP_DATABASE_DSN": dsn, "APP_DATABASE_DSN_FILE": secretFile(t, dsn)},
			wantErr: []string{"APP_DATABASE_DSN and APP_DATABASE_DSN_FILE are both set"}},
		"missing file names the variable and the path": {
			kv:      map[string]string{"APP_DATABASE_DSN_FILE": missing},
			wantErr: []string{"APP_DATABASE_DSN_FILE", missing}},
		"empty file": {
			kv:      map[string]string{"APP_DATABASE_DSN_FILE": secretFile(t, "\n")},
			wantErr: []string{"APP_DATABASE_DSN_FILE", "empty"}},
		"a non-secret setting has no file form": {
			kv:   map[string]string{"APP_DATABASE_DSN": dsn, "APP_HTTP_ADDR_FILE": secretFile(t, ":9999")},
			want: dsn},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := Load(env(tc.kv))
			if len(tc.wantErr) > 0 {
				if err == nil {
					t.Fatalf("want an error mentioning %q", tc.wantErr)
				}
				for _, w := range tc.wantErr {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("error %q does not mention %q", err, w)
					}
				}
				if strings.Contains(err.Error(), "s3cret") {
					t.Errorf("error %q shows the secret", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.DatabaseDSN != tc.want {
				t.Errorf("DSN %q, want %q", c.DatabaseDSN, tc.want)
			}
			if c.HTTPAddr != ":8080" {
				t.Errorf("HTTP addr %q, want the default", c.HTTPAddr)
			}
		})
	}
}

// The trace sampler is the SDK's own variable; config only refuses what the
// SDK would silently replace with "sample everything" (decision 0019).
func TestTraceSamplerVariablesAreValidatedNotHeld(t *testing.T) {
	const dsn = "u:p@tcp(db:3306)/app"
	for name, tc := range map[string]struct {
		sampler, arg string
		refused      string // "" when the pair loads
	}{
		"unset: the SDK's own default":         {},
		"production setting":                   {sampler: "parentbased_traceidratio", arg: "0.02"},
		"the SDK lower-cases and trims":        {sampler: " ParentBased_TraceIDRatio ", arg: " 0.5 "},
		"a typo would trace everything":        {sampler: "parentbased_traceratio", refused: "OTEL_TRACES_SAMPLER: unknown sampler"},
		"a ratio above one is refused":         {sampler: "parentbased_traceidratio", arg: "1.5", refused: "OTEL_TRACES_SAMPLER_ARG: 1.5 outside [0..1]"},
		"a percentage is not a ratio":          {sampler: "parentbased_traceidratio", arg: "5%", refused: "OTEL_TRACES_SAMPLER_ARG: not a number"},
		"zero keeps only what the parent kept": {sampler: "parentbased_traceidratio", arg: "0"},
		"NaN parses but is no ratio":           {sampler: "parentbased_traceidratio", arg: "NaN", refused: "OTEL_TRACES_SAMPLER_ARG: NaN outside [0..1]"},
		"a ratio alone keeps 100%":             {arg: "0.02", refused: "OTEL_TRACES_SAMPLER_ARG 0.02 is ignored by OTEL_TRACES_SAMPLER parentbased_always_on"},
		"a ratio with an always sampler":       {sampler: "always_on", arg: "0.02", refused: "is ignored"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(env(map[string]string{"APP_DATABASE_DSN": dsn, "OTEL_TRACES_SAMPLER": tc.sampler, "OTEL_TRACES_SAMPLER_ARG": tc.arg}))
			switch {
			case tc.refused == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case tc.refused != "" && (err == nil || !strings.Contains(err.Error(), tc.refused)):
				t.Fatalf("want an error containing %q, got %v", tc.refused, err)
			}
		})
	}
}
