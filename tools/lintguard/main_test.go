package main

import "testing"

func TestFalsePass(t *testing.T) {
	cases := map[string]string{
		"0 issues.": "",
		"level=error msg=\"[linters_context] typechecking error: could not load export data\"\n0 issues.": "typechecking error",
		"internal/a.go:3:1: unused (unused)\n1 issues.":                                                   "",
	}
	for out, want := range cases {
		if got := FalsePass(out); got != want {
			t.Errorf("FalsePass(%q) = %q, want %q", out, got, want)
		}
	}
}
