// Command ownercheck fails a lane's merge if its commits changed files its
// role may not write. It backs up the agentguard hook, which sees only tool
// calls: a write made through Bash or a script (`cat > x`, `bash fix.sh`)
// never passes the hook, but it always shows up in the diff.
//
// Usage: go run ./tools/ownercheck -role implementer -base main [-head HEAD]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/LeventAksakal/notification-system/tools/internal/policy"
)

func main() {
	role := flag.String("role", "", "role whose lane is checked (implementer, test-writer, ...)")
	base := flag.String("base", "main", "branch the lane forked from")
	head := flag.String("head", "HEAD", "lane tip")
	flag.Parse()
	if *role == "" {
		fmt.Fprintln(os.Stderr, "ownercheck: -role is required")
		os.Exit(2)
	}

	root, err := git("rev-parse", "--show-toplevel")
	must(err)
	p, err := policy.LoadPolicy(filepath.Join(root, ".claude", "agent-policy.json"))
	must(err)
	out, err := git("diff", "--name-only", *base+"..."+*head)
	must(err)

	bad := 0
	for _, f := range strings.Fields(out) {
		in := policy.Input{ToolName: "Write", AgentType: *role, ToolInput: policy.ToolInput{FilePath: filepath.Join(root, filepath.FromSlash(f))}}
		if d := policy.Decide(p, root, in); !d.Allow {
			fmt.Println(d.Reason)
			bad++
		}
	}
	if bad > 0 {
		fmt.Fprintf(os.Stderr, "ownercheck: %d file(s) outside the %s lane; revert them or have the owning role make the change\n", bad, *role)
		os.Exit(1)
	}
	fmt.Printf("ownercheck: %s lane clean (%s...%s)\n", *role, *base, *head)
}

func git(args ...string) (string, error) {
	b, err := exec.CommandContext(context.Background(), "git", args...).Output()
	return strings.TrimSpace(string(b)), err
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ownercheck:", err)
		os.Exit(2)
	}
}
