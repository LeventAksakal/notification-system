package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestHookDoesNotUseGoRun guards against a real failure: `go run` turns
// the guard's exit code 2 into 1, and Claude Code treats exit 1 as a
// non-blocking error, so every deny would be silently ignored. The hook must
// call the built binary directly and map any failure to exit 2 (fail closed).
func TestHookDoesNotUseGoRun(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", ".claude", "settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var s struct {
		Hooks map[string][]struct {
			Hooks []struct {
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, group := range s.Hooks["PreToolUse"] {
		for _, h := range group.Hooks {
			if !strings.Contains(h.Command, "agentguard") {
				continue
			}
			found = true
			if strings.Contains(h.Command, "go run") {
				t.Errorf("PreToolUse guard uses `go run`, which rewrites exit 2 to 1 and makes denies non-blocking: %s", h.Command)
			}
			if !strings.HasSuffix(strings.TrimSpace(h.Command), "|| exit 2") {
				t.Errorf("PreToolUse guard must fail closed with `|| exit 2`: %s", h.Command)
			}
		}
	}
	if !found {
		t.Fatal("no PreToolUse hook runs agentguard")
	}
}
