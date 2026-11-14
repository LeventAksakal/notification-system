package config

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// settingLines returns the uncommented, non-empty lines of an env file.
func settingLines(out string) []string {
	var lines []string
	for _, l := range strings.Split(out, "\n") {
		if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
			lines = append(lines, l)
		}
	}
	return lines
}

func TestEnvExampleListsEverySettingOnceWithItsDefault(t *testing.T) {
	out := EnvExample()
	for _, d := range Dials {
		t.Run(d.Env, func(t *testing.T) {
			if n := strings.Count(out, d.Env+"="); n != 1 {
				t.Fatalf("%s appears %d times, want 1", d.Env, n)
			}
			if d.Default != "" && !strings.Contains(out, "\n# "+d.Env+"="+d.Default+"\n") {
				t.Errorf("%s is not written commented out with its default %q", d.Env, d.Default)
			}
			if d.Required && d.Default == "" && !strings.Contains(out, "\n"+d.Env+"=") {
				t.Errorf("%s is required and must be written uncommented", d.Env)
			}
		})
	}
}

func TestEnvExampleIsDeterministicAndEndsWithANewline(t *testing.T) {
	a, b := EnvExample(), EnvExample()
	if a != b {
		t.Error("two renderings differ")
	}
	if !strings.HasSuffix(a, "\n") {
		t.Error("output does not end with a newline")
	}
}

func TestEnvExampleParsesAsAnEnvFile(t *testing.T) {
	for _, l := range settingLines(EnvExample()) {
		t.Run(l, func(t *testing.T) {
			name, _, ok := strings.Cut(l, "=")
			if !ok || name == "" || strings.ContainsAny(name, " \t") {
				t.Errorf("not NAME=value: %q", l)
			}
		})
	}
}

func TestEnvExampleUncommentedLinesAreAWorkingMinimalConfiguration(t *testing.T) {
	kv := map[string]string{}
	for _, l := range settingLines(EnvExample()) {
		name, value, _ := strings.Cut(l, "=")
		kv[name] = value
	}
	if _, err := Load(env(kv)); err != nil {
		t.Fatalf("loading the example alone failed: %v", err)
	}
}

func TestReadmeNamesEverySetting(t *testing.T) {
	raw, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	readme := string(raw)
	var missing []string
	for _, d := range Dials {
		if !strings.Contains(readme, d.Env) {
			missing = append(missing, d.Env)
		}
	}
	if len(missing) > 0 {
		t.Errorf("README.md does not mention: %s", strings.Join(missing, ", "))
	}
}

func TestEnvCheckReportsProblemsOrEverySettingWithItsSource(t *testing.T) {
	const secret = "u:hunter2-very-secret@tcp(db:3306)/app" //nolint:gosec // G101: a fake secret the report must not show
	for name, tc := range map[string]struct {
		environ []string
		ok      bool
		want    []string // lines that must be in the report
	}{
		"defaults and an injected secret": {
			environ: []string{"APP_DATABASE_DSN=" + secret, "PATH=/bin", "HOME=/root"},
			ok:      true,
			want: []string{"APP_DATABASE_DSN=(set)  # environment", "APP_HTTP_ADDR=:8080  # default",
				"APP_RATE_LIMIT_HEADROOM_PERCENT=5  # default"},
		},
		"secret from a file, a setting from the environment": {
			environ: []string{"APP_DATABASE_DSN_FILE=" + secretFile(t, secret+"\n"), "APP_WORKER_CONCURRENCY=8"},
			ok:      true,
			want:    []string{"APP_DATABASE_DSN=(set)  # file", "APP_WORKER_CONCURRENCY=8  # environment"},
		},
		"a typo is an unknown setting": {
			environ: []string{"APP_DATABASE_DSN=" + secret, "APP_WORKER_CONCURENCY=8"},
			want:    []string{"APP_WORKER_CONCURENCY: unknown setting (not declared in internal/config; see app env)"},
		},
		"a file form of a non-secret setting is unknown": {
			environ: []string{"APP_DATABASE_DSN=" + secret, "APP_HTTP_ADDR_FILE=/run/secrets/addr"},
			want:    []string{"APP_HTTP_ADDR_FILE: unknown setting (not declared in internal/config; see app env)"},
		},
		"what Load refuses, all at once": {
			environ: []string{"APP_DB_MAX_OPEN_CONNS=0", "APP_XYZ=1"},
			want: []string{"APP_XYZ: unknown setting (not declared in internal/config; see app env)",
				"APP_DATABASE_DSN is required: " + Dials[1].Doc, "APP_DB_MAX_OPEN_CONNS: 0 outside [1..200]"},
		},
		"both forms of the secret": {
			environ: []string{"APP_DATABASE_DSN=" + secret, "APP_DATABASE_DSN_FILE=" + secretFile(t, secret)},
			want:    []string{"APP_DATABASE_DSN and APP_DATABASE_DSN_FILE are both set; set one"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			lines, ok := Check(tc.environ)
			out := strings.Join(lines, "\n")
			if ok != tc.ok {
				t.Errorf("ok %v, want %v; report:\n%s", ok, tc.ok, out)
			}
			for _, w := range tc.want {
				if !slices.Contains(lines, w) {
					t.Errorf("report lacks %q:\n%s", w, out)
				}
			}
			if tc.ok && len(lines) != len(Dials) {
				t.Errorf("%d lines, want one per setting (%d)", len(lines), len(Dials))
			}
			if strings.Contains(out, "hunter2") {
				t.Errorf("report shows the secret:\n%s", out)
			}
		})
	}
}

func TestEnvExampleNamesTheFileFormOfEverySecret(t *testing.T) {
	out := EnvExample()
	for _, d := range Dials {
		if d.Secret && !strings.Contains(out, d.Env+FileSuffix+"=") {
			t.Errorf("%s: the %s form is not mentioned", d.Env, FileSuffix)
		}
	}
}
