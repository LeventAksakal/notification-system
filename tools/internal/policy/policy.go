package policy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Input is the part of the PreToolUse hook input the guard reads.
// agent_type is empty in the main thread (the coordinator).
type Input struct {
	Cwd       string    `json:"cwd"`
	ToolName  string    `json:"tool_name"`
	ToolInput ToolInput `json:"tool_input"`
	AgentType string    `json:"agent_type"`
}

// ToolInput holds the fields of Edit, Write, NotebookEdit and Bash calls.
type ToolInput struct {
	FilePath     string `json:"file_path"`
	NotebookPath string `json:"notebook_path"`
	Command      string `json:"command"`
}

// Policy mirrors .claude/agent-policy.json.
type Policy struct {
	Generated []string           `json:"generated"`
	Contracts []string           `json:"contracts"`
	BashDeny  []string           `json:"bash_deny"`
	Profiles  map[string]Profile `json:"profiles"`
}

// Profile is what one subagent role may do. A nil BashAllow means any
// command not on the global deny list; a non-nil one is an allowlist.
type Profile struct {
	WriteAllow []string `json:"write_allow"`
	WriteDeny  []string `json:"write_deny"`
	BashAllow  []string `json:"bash_allow"`
}

// Decision is the guard's verdict.
type Decision struct {
	Allow  bool
	Reason string
}

var allow = Decision{Allow: true}

func deny(format string, a ...any) Decision {
	return Decision{Reason: "agentguard: " + fmt.Sprintf(format, a...)}
}

// LoadPolicy reads and parses the policy file.
func LoadPolicy(path string) (Policy, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Policy{}, fmt.Errorf("read policy: %w", err)
	}
	var p Policy
	if err := json.Unmarshal(b, &p); err != nil {
		return Policy{}, fmt.Errorf("parse policy %s: %w", path, err)
	}
	return p, nil
}

// Decide applies the policy to one tool call.
func Decide(p Policy, root string, in Input) Decision {
	switch in.ToolName {
	case "Edit", "Write", "MultiEdit", "NotebookEdit":
		target := in.ToolInput.FilePath
		if target == "" {
			target = in.ToolInput.NotebookPath
		}
		return decideWrite(p, root, in.AgentType, target)
	case "Bash":
		return decideBash(p, in.AgentType, in.ToolInput.Command)
	default:
		return allow
	}
}

func decideWrite(p Policy, root, agent, target string) Decision {
	rel, inside := relPath(root, target)
	if matchAny(p.Generated, rel) {
		return deny("%s is generated; change its source and run `go generate ./...` (or `go tool task gen`) instead", rel)
	}
	if agent == "" {
		return allow // coordinator: may edit contracts and everything not generated
	}
	prof, ok := p.Profiles[agent]
	if !ok {
		return deny("role %q has no profile in .claude/agent-policy.json; add one or ask the coordinator to make this change", agent)
	}
	if !inside {
		return deny("role %q may not write outside the repo (%s)", agent, target)
	}
	if matchAny(p.Contracts, rel) {
		return deny("%s is a frozen contract; only the coordinator changes it. Write an owner item in .agents/owner-items/ describing the change you need, and continue against the current contract", rel)
	}
	if matchAny(prof.WriteDeny, rel) || !matchAny(prof.WriteAllow, rel) {
		return deny("role %q may not write %s (allowed: %s)", agent, rel, strings.Join(prof.WriteAllow, ", "))
	}
	return allow
}

func decideBash(p Policy, agent, cmd string) Decision {
	norm := strings.Join(strings.Fields(cmd), " ")
	for _, bad := range p.BashDeny {
		if strings.Contains(norm, bad) {
			return deny("command contains %q, which is denied for every agent; find a non-destructive way or record an owner item", bad)
		}
	}
	if agent == "" {
		return allow
	}
	prof, ok := p.Profiles[agent]
	if !ok {
		return deny("role %q has no profile in .claude/agent-policy.json", agent)
	}
	if prof.BashAllow == nil {
		return allow
	}
	if strings.Contains(norm, "$(") || strings.Contains(norm, "`") {
		return deny("role %q may not use command substitution", agent)
	}
	for _, seg := range splitSegments(norm) {
		if writesFile(seg) {
			return deny("role %q is read-only; %q writes a file", agent, seg)
		}
		if !hasPrefixAny(seg, prof.BashAllow) {
			return deny("role %q may run only: %s (got %q)", agent, strings.Join(prof.BashAllow, ", "), seg)
		}
	}
	return allow
}

var segSplit = regexp.MustCompile(`&&|\|\||;|\||\n`)

func splitSegments(cmd string) []string {
	var out []string
	for _, s := range segSplit.Split(cmd, -1) {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func writesFile(seg string) bool {
	s := strings.NewReplacer("2>&1", "", ">/dev/null", "", "> /dev/null", "", "2>/dev/null", "").Replace(seg)
	return strings.Contains(s, ">")
}

func hasPrefixAny(seg string, prefixes []string) bool {
	for _, p := range prefixes {
		if seg == p || strings.HasPrefix(seg, p+" ") {
			return true
		}
	}
	return false
}

// relPath returns target relative to root with forward slashes, and
// whether it lies inside root. Works for Windows and POSIX paths.
func relPath(root, target string) (string, bool) {
	if !filepath.IsAbs(target) {
		target = filepath.Join(root, target)
	}
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return filepath.ToSlash(target), false
	}
	rel = filepath.ToSlash(rel)
	return rel, rel != ".." && !strings.HasPrefix(rel, "../")
}

func matchAny(patterns []string, rel string) bool {
	for _, p := range patterns {
		if globRe(p).MatchString(rel) {
			return true
		}
	}
	return false
}

// globRe turns a gitignore-style glob into a regexp: ** crosses
// directories, * and ? do not.
func globRe(glob string) *regexp.Regexp {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(glob); i++ {
		switch c := glob[i]; {
		case strings.HasPrefix(glob[i:], "**/"):
			b.WriteString("(?:.*/)?")
			i += 2
		case strings.HasPrefix(glob[i:], "**"):
			b.WriteString(".*")
			i++
		case c == '*':
			b.WriteString("[^/]*")
		case c == '?':
			b.WriteString("[^/]")
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString("$")
	return regexp.MustCompile(b.String())
}
