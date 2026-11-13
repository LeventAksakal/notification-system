// Package adrcheck holds the decision-record rules as a test, so stale ADRs
// cannot mislead agents: agents over-follow superseded decisions, so a
// superseded ADR must be marked, redirected and kept out of the index.
package adrcheck

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const dir = "../../docs/decisions"

var (
	adrName   = regexp.MustCompile(`^\d{4}-[a-z0-9-]+\.md$`)
	fmField   = regexp.MustCompile(`(?m)^([a-z-]+):\s*(.*)$`)
	indexLink = regexp.MustCompile(`\]\(((?:superseded/)?\d{4}-[a-z0-9-]+\.md)\)`)
	statuses  = map[string]bool{"proposed": true, "accepted": true, "superseded": true, "rejected": true}
)

type adr struct {
	file, status, supersededBy, body string
}

func load(t *testing.T, sub string) []adr {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, sub))
	if err != nil {
		t.Fatal(err)
	}
	var out []adr
	for _, e := range entries {
		if e.IsDir() || !adrName.MatchString(e.Name()) || strings.HasPrefix(e.Name(), "0000-") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, sub, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		parts := strings.SplitN(strings.ReplaceAll(string(b), "\r\n", "\n"), "---\n", 3)
		if len(parts) != 3 || parts[0] != "" {
			t.Errorf("%s: missing front matter", e.Name())
			continue
		}
		a := adr{file: filepath.ToSlash(filepath.Join(sub, e.Name())), body: strings.TrimSpace(parts[2])}
		for _, m := range fmField.FindAllStringSubmatch(parts[1], -1) {
			switch m[1] {
			case "status":
				a.status = strings.Fields(m[2] + " ")[0]
			case "superseded-by":
				a.supersededBy = strings.TrimSpace(m[2])
			}
		}
		out = append(out, a)
	}
	return out
}

func TestDecisionRecords(t *testing.T) {
	live := load(t, "")
	old := load(t, "superseded")

	for _, a := range live {
		if !statuses[a.status] {
			t.Errorf("%s: status %q is not one of proposed|accepted|superseded|rejected", a.file, a.status)
		}
		if a.status == "superseded" {
			t.Errorf("%s: superseded ADRs live in docs/decisions/superseded/ so search and agents skip them", a.file)
		}
	}
	for _, a := range old {
		if a.status != "superseded" || a.supersededBy == "" {
			t.Errorf("%s: needs status: superseded and a superseded-by: link", a.file)
		}
		if !strings.HasPrefix(a.body, "> **Superseded by") {
			t.Errorf("%s: first body line must be the banner `> **Superseded by …**` so an agent that opens it is redirected", a.file)
		}
	}

	idx, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	for _, m := range indexLink.FindAllStringSubmatch(string(idx), -1) {
		listed[m[1]] = true
	}
	for _, a := range append(live, old...) {
		switch {
		case a.status == "accepted" && !listed[a.file]:
			t.Errorf("index README.md does not list accepted ADR %s", a.file)
		case a.status != "accepted" && listed[a.file]:
			t.Errorf("index README.md lists %s (status %s); the index holds accepted ADRs only", a.file, a.status)
		}
	}
}
