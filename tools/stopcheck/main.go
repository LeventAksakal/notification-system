// Command stopcheck is a Claude Code Stop hook: before an agent may end its
// turn, the Go packages it touched must pass go vet (which typechecks code
// and tests). It checks only the
// touched packages, so it stays fast (the full gate runs once, at merge).
//
// Exit 2 blocks the stop and shows stderr to the agent. If the hook already
// blocked once this turn (stop_hook_active), it lets the agent stop and says
// so, so a broken build can never trap an agent in a loop.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"time"
)

func main() {
	var in struct {
		StopHookActive bool `json:"stop_hook_active"`
	}
	raw, _ := io.ReadAll(os.Stdin)
	_ = json.Unmarshal(raw, &in)

	// A hook that hangs traps the agent; give the whole check a deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pkgs, err := touchedPackages(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "stopcheck: cannot list changes:", err)
		os.Exit(0) // never trap the agent on a git problem
	}
	if len(pkgs) == 0 {
		return
	}
	// vet alone: it typechecks code and tests, and unlike `go build` it
	// accepts test-only packages (build would block every stop).
	for _, step := range [][]string{append([]string{"vet"}, pkgs...)} {
		out, err := exec.CommandContext(ctx, "go", step...).CombinedOutput()
		if err == nil {
			continue
		}
		if in.StopHookActive {
			fmt.Fprintf(os.Stderr, "stopcheck: go %s still fails after one retry; stopping anyway. Record it in the hand-back.\n%s", step[0], out)
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "stopcheck: go %s fails on packages you touched; fix before finishing:\n%s", step[0], out)
		os.Exit(2)
	}
}

// touchedPackages returns ./dir patterns for .go files changed against HEAD
// or untracked, skipping deleted files. --relative keeps paths relative to
// the module even when it sits below the repository root.
func touchedPackages(ctx context.Context) ([]string, error) {
	changed, err := git(ctx, "diff", "--name-only", "--relative", "--diff-filter=d", "HEAD")
	if err != nil {
		return nil, err
	}
	untracked, err := git(ctx, "ls-files", "--others", "--exclude-standard")
	if err != nil {
		return nil, err
	}
	return PackagesOf(append(changed, untracked...)), nil
}

// PackagesOf maps file paths to sorted, unique ./dir package patterns.
func PackagesOf(files []string) []string {
	seen := map[string]bool{}
	for _, f := range files {
		if !strings.HasSuffix(f, ".go") {
			continue
		}
		seen["./"+path.Dir(f)] = true
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, strings.TrimSuffix(p, "/."))
	}
	sort.Strings(out)
	return out
}

func git(ctx context.Context, args ...string) ([]string, error) {
	b, err := exec.CommandContext(ctx, "git", args...).Output()
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(b)), nil
}
