package policy

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestDecide(t *testing.T) {
	p, err := LoadPolicy(filepath.Join("..", "..", "..", ".claude", "agent-policy.json"))
	if err != nil {
		t.Fatal(err)
	}
	root, _ := filepath.Abs(filepath.Join("..", "..", ".."))
	abs := func(rel string) string { return filepath.Join(root, filepath.FromSlash(rel)) }

	edit := func(agent, rel string) Input {
		return Input{ToolName: "Edit", AgentType: agent, ToolInput: ToolInput{FilePath: abs(rel)}}
	}
	bash := func(agent, cmd string) Input {
		return Input{ToolName: "Bash", AgentType: agent, ToolInput: ToolInput{Command: cmd}}
	}

	cases := []struct {
		name    string
		in      Input
		allow   bool
		reasons string // substring expected in the deny reason
	}{
		{"coordinator edits app code", edit("", "internal/sender/send.go"), true, ""},
		{"coordinator edits a contract", edit("", "api/openapi.yaml"), true, ""},
		{"coordinator cannot hand-edit sqlc output", edit("", "internal/store/sqlc/query.sql.go"), false, "go generate"},
		{"nobody hand-edits *.gen.go", edit("implementer", "internal/api/server.gen.go"), false, "generated"},
		{"generated migrations", edit("", "migrations/20261110_init.sql"), false, "generated"},

		{"implementer writes app code", edit("implementer", "internal/sender/send.go"), true, ""},
		{"implementer writes unit tests", edit("implementer", "internal/sender/send_test.go"), true, ""},
		{"implementer cannot touch the judge", edit("implementer", "test/acceptance/send_once_test.go"), false, "may not write"},
		{"implementer cannot change a contract", edit("implementer", "schema/messages.sql"), false, "frozen contract"},
		{"implementer cannot change the core types", edit("implementer", "internal/core/types.go"), false, "frozen contract"},
		{"implementer cannot edit the guard", edit("implementer", "tools/agentguard/main.go"), false, "may not write"},
		{"implementer cannot edit decision records", edit("implementer", "docs/decisions/0001-record-decisions-as-adrs.md"), false, "may not write"},
		{"implementer writes an owner item", edit("implementer", ".agents/owner-items/0001-limit.md"), true, ""},

		{"test-writer writes acceptance tests", edit("test-writer", "test/acceptance/send_once_test.go"), true, ""},
		{"test-writer cannot write app code", edit("test-writer", "internal/sender/send.go"), false, "may not write"},

		{"researcher writes a log", edit("researcher", ".agents/logs/2026-11/10.md"), true, ""},
		{"researcher cannot edit code", edit("researcher", "internal/a.go"), false, "may not write"},
		{"unknown role is denied with instructions", edit("general-purpose", "internal/a.go"), false, "no profile"},
		{"subagent cannot write outside the repo", Input{ToolName: "Write", AgentType: "implementer", ToolInput: ToolInput{FilePath: filepath.Join(filepath.Dir(root), "elsewhere.go")}}, false, "outside the repo"},
		{"relative path is resolved against the repo", Input{ToolName: "Write", AgentType: "implementer", ToolInput: ToolInput{FilePath: "internal/a.go"}}, true, ""},
		{"notebook path is checked too", Input{ToolName: "NotebookEdit", AgentType: "researcher", ToolInput: ToolInput{NotebookPath: abs("notes/a.ipynb")}}, false, "may not write"},

		{"researcher reads docs", bash("researcher", "go doc net/http.ResponseController"), true, ""},
		{"researcher cannot chain a write", bash("researcher", "go doc fmt && rm -rf internal"), false, "may run only"},
		{"researcher cannot redirect into a file", bash("researcher", "git log > out.txt"), false, "writes a file"},
		{"researcher may discard stderr", bash("researcher", "git log --oneline 2>&1"), true, ""},
		{"researcher cannot use substitution", bash("researcher", "git show $(rm x)"), false, "substitution"},
		{"verifier runs tests piped through a reader", bash("verifier", "go test ./... | git diff --stat"), true, ""},
		{"verifier cannot tee into a file", bash("verifier", "go test ./... | tee out.log"), false, "may run only"},
		{"verifier runs compose", bash("verifier", "docker compose up -d --wait"), true, ""},
		{"implementer runs any safe command", bash("implementer", "go build ./... && go vet ./internal/..."), true, ""},
		{"force push denied for everyone", bash("", "git push --force origin main"), false, "denied for every agent"},
		{"volume wipe denied for everyone", bash("implementer", "docker  compose   down -v"), false, "denied for every agent"},
		{"hook bypass denied", bash("", "git commit --no-verify -m x"), false, "denied for every agent"},

		{"reads are never blocked", Input{ToolName: "Read", AgentType: "researcher"}, true, ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := Decide(p, root, c.in)
			if d.Allow != c.allow {
				t.Fatalf("allow = %v, want %v (reason: %s)", d.Allow, c.allow, d.Reason)
			}
			if !c.allow && !strings.Contains(d.Reason, c.reasons) {
				t.Fatalf("reason %q does not mention %q", d.Reason, c.reasons)
			}
		})
	}
}

func TestGlob(t *testing.T) {
	cases := []struct {
		glob, path string
		want       bool
	}{
		{"**/*.gen.go", "a.gen.go", true},
		{"**/*.gen.go", "internal/api/server.gen.go", true},
		{"**/*.gen.go", "internal/api/server.go", false},
		{"test/acceptance/**", "test/acceptance/x/y_test.go", true},
		{"test/acceptance/**", "test/unit/y_test.go", false},
		{"internal/*.go", "internal/a/b.go", false},
		{"README.md", "README.md", true},
	}
	for _, c := range cases {
		if got := globRe(c.glob).MatchString(c.path); got != c.want {
			t.Errorf("glob %q on %q = %v, want %v", c.glob, c.path, got, c.want)
		}
	}
}
