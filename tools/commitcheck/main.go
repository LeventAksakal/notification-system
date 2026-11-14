// Command commitcheck holds commit subjects to the Conventional Commits
// format this repository's history follows:
//
//	type(scope)!: description
//
// with one type from the list below, an optional lower-case scope and an
// optional "!" for a breaking change.
//
//	commitcheck <file>            the commit-msg hook: check the message in <file>
//	commitcheck -range A..B       CI: check every commit subject in the range
//
// It needs nothing installed beyond Go and git, like the repository's other
// checks. Merge commits are refused on purpose: main moves by fast-forward.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

// types are the Conventional Commits types in use here (the Angular set).
var types = []string{"feat", "fix", "docs", "test", "chore", "build", "ci", "refactor", "perf", "style", "revert"}

var subject = regexp.MustCompile(`^(` + strings.Join(types, "|") + `)(\([a-z0-9][a-z0-9,/ -]*\))?!?: \S.*$`)

// check returns what is wrong with a commit subject, or "".
func check(s string) string {
	switch {
	case s == "":
		return "empty subject"
	case strings.HasPrefix(s, "fixup! "), strings.HasPrefix(s, "squash! "), strings.HasPrefix(s, "amend! "):
		return "" // autosquash markers; they never reach main
	case !subject.MatchString(s):
		return "want type(scope): description, type one of " + strings.Join(types, ", ")
	case strings.HasSuffix(s, "."):
		return "no full stop at the end of the subject"
	}
	return ""
}

// firstLine is the subject of a commit message file: its first line that is
// not a comment.
func firstLine(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // the path git hands the hook
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := sc.Text(); !strings.HasPrefix(line, "#") {
			return strings.TrimSpace(line), nil
		}
	}
	return "", sc.Err()
}

func run(ctx context.Context, args []string) (bad []string, err error) {
	fs := flag.NewFlagSet("commitcheck", flag.ContinueOnError)
	rng := fs.String("range", "", "check every commit subject in this git revision range")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if *rng == "" {
		if fs.NArg() != 1 {
			return nil, fmt.Errorf("usage: commitcheck <message file> | commitcheck -range A..B")
		}
		s, err := firstLine(fs.Arg(0))
		if err != nil {
			return nil, err
		}
		if why := check(s); why != "" {
			bad = append(bad, fmt.Sprintf("%q: %s", s, why))
		}
		return bad, nil
	}
	out, err := exec.CommandContext(ctx, "git", "log", "--format=%h %s", *rng).Output() //nolint:gosec // a revision range from the caller
	if err != nil {
		return nil, fmt.Errorf("git log %s: %w", *rng, err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		hash, s, _ := strings.Cut(line, " ")
		if why := check(s); why != "" {
			bad = append(bad, fmt.Sprintf("%s %q: %s", hash, s, why))
		}
	}
	return bad, nil
}

func main() {
	bad, err := run(context.Background(), os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "commitcheck:", err)
		os.Exit(2)
	}
	if len(bad) > 0 {
		fmt.Fprintln(os.Stderr, "commitcheck: commit subject not in the conventional format")
		for _, b := range bad {
			fmt.Fprintln(os.Stderr, "  "+b)
		}
		os.Exit(1)
	}
}
