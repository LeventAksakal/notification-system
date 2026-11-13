// Command lintguard runs golangci-lint and fails if the linter could not
// typecheck the code, even when it reports "0 issues".
//
// Why: golangci-lint built with an older Go cannot read newer
// export data; it logs a typecheck error and still prints "0 issues" and
// exits 0, a silent false pass. Written in Go (not grep) so it runs the same
// on Windows, macOS and Linux.
//
// Usage: go run ./tools/lintguard [golangci-lint args...]
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// falsePassMarkers are output lines that mean the linter did not really run.
var falsePassMarkers = []string{
	"typechecking error",
	"could not load export data",
	"unsupported version",
	"go version mismatch",
}

func main() {
	args := append([]string{"tool", "-modfile=tools.mod", "golangci-lint", "run"}, os.Args[1:]...)
	cmd := exec.CommandContext(context.Background(), "go", args...)
	var buf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &buf)
	cmd.Stderr = io.MultiWriter(os.Stderr, &buf)
	err := cmd.Run()

	if m := FalsePass(buf.String()); m != "" {
		fmt.Fprintf(os.Stderr, "lintguard: linter output contains %q: it did not typecheck the code, so its result is not a pass. Rebuild golangci-lint with the toolchain in go.mod.\n", m)
		os.Exit(3)
	}
	if err != nil {
		os.Exit(1)
	}
}

// FalsePass returns the first marker found in out, or "".
func FalsePass(out string) string {
	low := strings.ToLower(out)
	for _, m := range falsePassMarkers {
		if strings.Contains(low, m) {
			return m
		}
	}
	return ""
}
