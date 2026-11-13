// Command agentguard is a Claude Code PreToolUse hook that enforces the
// role profiles in .claude/agent-policy.json.
//
// It never asks: a call is either allowed (exit 0, no output) or denied
// (exit 2, with a message on stderr that tells the agent what to do instead).
// Denies are cheap and immediate, so autonomous runs never stall on a prompt.
//
// Set AGENTGUARD_DUMP=<file> to append each raw hook input to a file; used
// once to confirm the input field names (agent_type, tool_input.file_path).
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/LeventAksakal/notification-system/tools/internal/policy"
)

func main() {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fail("agentguard: cannot read hook input: " + err.Error())
	}
	if dump := os.Getenv("AGENTGUARD_DUMP"); dump != "" {
		appendFile(dump, raw)
	}

	var in policy.Input
	if err := json.Unmarshal(raw, &in); err != nil {
		fail("agentguard: hook input is not JSON: " + err.Error())
	}

	root := os.Getenv("CLAUDE_PROJECT_DIR")
	if root == "" {
		root = in.Cwd
	}
	p, err := policy.LoadPolicy(filepath.Join(root, ".claude", "agent-policy.json"))
	if err != nil {
		fail("agentguard: " + err.Error())
	}

	if d := policy.Decide(p, root, in); !d.Allow {
		fail(d.Reason)
	}
}

// fail denies the tool call. Exit code 2 blocks it, and Claude Code shows
// stderr to the agent.
func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

func appendFile(path string, b []byte) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(b, '\n'))
}
