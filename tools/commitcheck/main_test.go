package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheck(t *testing.T) {
	for _, c := range []struct {
		name, subject string
		ok            bool
	}{
		{"type and description", "fix: retry a release", true},
		{"scope", "feat(api): spec 1.3.0", true},
		{"two scopes", "docs(owner-items, log): what stayed open", true},
		{"breaking change", "feat(api)!: drop the v0 routes", true},
		{"autosquash marker", "fixup! feat(api): spec 1.3.0", true},
		{"no type", "retry a release", false},
		{"unknown type", "bugfix: retry a release", false},
		{"upper-case scope", "fix(API): retry", false},
		{"no space after the colon", "fix:retry", false},
		{"no description", "fix: ", false},
		{"full stop", "fix: retry a release.", false},
		{"merge commit", "Merge branch 'main' into feature", false},
		{"empty", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if why := check(c.subject); (why == "") != c.ok {
				t.Errorf("check(%q) = %q, want ok=%v", c.subject, why, c.ok)
			}
		})
	}
}

func TestMessageFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "COMMIT_EDITMSG")
	write := func(s string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("# a comment git leaves in the template\nfix(worker): give the slot up\n\nBody text is free.\n")
	if bad, err := run(t.Context(), []string{path}); err != nil || len(bad) != 0 {
		t.Errorf("good message: bad=%v err=%v", bad, err)
	}
	write("gave the slot up\n")
	if bad, err := run(t.Context(), []string{path}); err != nil || len(bad) != 1 {
		t.Errorf("bad message: bad=%v err=%v", bad, err)
	}
}
