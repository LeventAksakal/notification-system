// Command apibreaking fails when the generated api/openapi.yaml breaks clients
// of a base revision without saying so in its version.
//
//	apibreaking -base main          task api:breaking
//	apibreaking -base <sha>         CI: the pull request's base commit
//
// It compares the base's info.version with the head's (semantic versioning,
// as the version doc line in internal/httpapi/api.go asks):
//   - same major version: oasdiff must find no breaking change (--fail-on ERR);
//   - a higher major version: the breaking changes are printed and allowed;
//   - a lower major version: refused.
//
// It needs nothing beyond Go and git, like the repository's other checks.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

const spec = "api/openapi.yaml"

func main() {
	base := flag.String("base", "main", "git revision whose api/openapi.yaml clients use")
	flag.Parse()
	if err := run(*base); err != nil {
		fmt.Fprintln(os.Stderr, "apibreaking:", err)
		os.Exit(1)
	}
}

func run(base string) error {
	old, err := exec.Command("git", "show", base+":"+spec).Output()
	if err != nil {
		return fmt.Errorf("read %s at %s: %w", spec, base, err)
	}
	head, err := os.ReadFile(spec)
	if err != nil {
		return err
	}
	baseMajor, err := major(old)
	if err != nil {
		return fmt.Errorf("%s at %s: %w", spec, base, err)
	}
	headMajor, err := major(head)
	if err != nil {
		return fmt.Errorf("%s: %w", spec, err)
	}
	mode, err := decide(baseMajor, headMajor)
	if err != nil {
		return err
	}

	dir, err := os.MkdirTemp("", "apibreaking")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	oldPath := filepath.Join(dir, "base-openapi.yaml")
	if err := os.WriteFile(oldPath, old, 0o600); err != nil {
		return err
	}
	args := []string{"tool", "-modfile=tools.mod", "oasdiff", "breaking", "--flatten-params"}
	if mode == strict {
		args = append(args, "--fail-on", "ERR")
	} else {
		fmt.Printf("major version %d -> %d: breaking changes are allowed and listed below\n", baseMajor, headMajor)
	}
	cmd := exec.Command("go", append(args, oldPath, spec)...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("oasdiff found changes that break clients of %s; keep them compatible or bump the major version (internal/httpapi/api.go Version): %w", base, err)
	}
	return nil
}

type checkMode int

const (
	strict checkMode = iota // same major: no breaking change allowed
	report                  // major bumped: list the breaking changes, pass
)

func decide(baseMajor, headMajor int) (checkMode, error) {
	switch {
	case headMajor == baseMajor:
		return strict, nil
	case headMajor > baseMajor:
		return report, nil
	default:
		return strict, fmt.Errorf("major version went down from %d to %d", baseMajor, headMajor)
	}
}

// major reads info.version from a generated OpenAPI YAML document: the first
// "  version:" line under the top-level "info:" key.
func major(doc []byte) (int, error) {
	sc := bufio.NewScanner(bytes.NewReader(doc))
	inInfo := false
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, " ") && line != "" {
			inInfo = line == "info:"
			continue
		}
		if v, ok := strings.CutPrefix(line, "  version: "); ok && inInfo {
			v = strings.Trim(strings.TrimSpace(v), `"'`)
			m, _, _ := strings.Cut(v, ".")
			n, err := strconv.Atoi(m)
			if err != nil {
				return 0, fmt.Errorf("info.version %q is not semantic versioning", v)
			}
			return n, nil
		}
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, errors.New("no info.version")
}
