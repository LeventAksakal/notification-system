package config

import (
	"os"
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
