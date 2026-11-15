// Command schemadiff turns schema/ (the desired state) into goose migrations
// with sqldef, and checks that migrations/ and schema/ agree.
//
// schema/ may hold any number of .sql files in any folders; they are read in
// path order, so a table that others reference must sort first. The current
// state is a scratch MySQL that has migrations/ applied (task migrate:diff
// prepares it).
//
//	schemadiff -name add_note    write migrations/<UTC time>_add_note.sql
//	schemadiff -check            exit 1 if schema/ differs from the scratch database
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func main() {
	name := flag.String("name", "", "migration name (writes the next migration)")
	check := flag.Bool("check", false, "fail if schema/ and the scratch database differ")
	host := flag.String("host", "127.0.0.1", "scratch MySQL host")
	port := flag.String("port", "33306", "scratch MySQL port")
	db := flag.String("db", "dev", "scratch database")
	flag.Parse()
	conn := []string{"-uroot", "-h" + *host, "-P" + *port, *db}

	if err := run(*name, *check, conn); err != nil {
		fmt.Fprintln(os.Stderr, "schemadiff:", err)
		os.Exit(1)
	}
}

func run(name string, check bool, conn []string) error {
	if (name == "") == !check {
		return errors.New("give exactly one of -name or -check")
	}
	tmp, err := os.MkdirTemp("", "schemadiff")
	if err != nil {
		return fmt.Errorf("making a temp folder: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmp) }()

	desired := filepath.Join(tmp, "desired.sql")
	if err := concatSchema("schema", desired); err != nil {
		return err
	}

	if check {
		out, code, err := mysqldef(append([]string{"--check", "--enable-drop", "--file", desired}, conn...)...)
		if err != nil {
			return err
		}
		if code == 2 {
			return fmt.Errorf("schema/ and migrations/ disagree; run task migrate:diff -- <name>\n%s", out)
		}
		if code != 0 {
			return fmt.Errorf("mysqldef exited %d\n%s", code, out)
		}
		return nil
	}

	up, err := statements(append([]string{"--dry-run", "--enable-drop", "--file", desired}, conn...)...)
	if err != nil {
		return err
	}
	if up == "" {
		fmt.Println("schema/ matches migrations/; nothing to write")
		return nil
	}
	// Down: apply Up to the scratch database, then diff its export back to
	// the export from before. Both sides are MySQL's own spelling, so the
	// diff holds only the real change.
	before, err := export(conn, filepath.Join(tmp, "before.sql"))
	if err != nil {
		return err
	}
	if _, err := statements(append([]string{"--apply", "--enable-drop", "--file", desired}, conn...)...); err != nil {
		return fmt.Errorf("applying the change to the scratch database: %w", err)
	}
	after, err := export(conn, filepath.Join(tmp, "after.sql"))
	if err != nil {
		return err
	}
	down, err := statements("--enable-drop", "--file", before, after) // offline: from after back to before
	if err != nil {
		return err
	}

	path := filepath.Join("migrations", time.Now().UTC().Format("20060102150405")+"_"+name+".sql")
	body := "-- +goose Up\n" + up + "\n-- +goose Down\n" + down
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	fmt.Println("wrote", path)
	return nil
}

// export writes the scratch database's schema to path and returns path.
func export(conn []string, path string) (string, error) {
	out, code, err := mysqldef(append([]string{"--export"}, conn...)...)
	if err != nil || code != 0 {
		return "", fmt.Errorf("exporting the scratch schema (exit %d): %w\n%s", code, err, out)
	}
	if err := os.WriteFile(path, []byte(out), 0o600); err != nil {
		return "", fmt.Errorf("writing %s: %w", path, err)
	}
	return path, nil
}

// concatSchema writes every .sql file under dir, in path order, into out.
func concatSchema(dir, out string) error {
	var files []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".sql") {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("listing %s: %w", dir, err)
	}
	sort.Strings(files)
	var b bytes.Buffer
	for _, f := range files {
		data, err := os.ReadFile(f) //nolint:gosec // G304: paths come from walking schema/
		if err != nil {
			return fmt.Errorf("reading %s: %w", f, err)
		}
		b.Write(data)
		b.WriteString("\n")
	}
	if err := os.WriteFile(out, b.Bytes(), 0o600); err != nil {
		return fmt.Errorf("writing the desired schema: %w", err)
	}
	return nil
}

// statements runs mysqldef and keeps only the DDL: no dry-run banner, no
// BEGIN/COMMIT wrapper (MySQL DDL commits implicitly; goose wraps each file).
func statements(args ...string) (string, error) {
	out, code, err := mysqldef(args...)
	if err != nil {
		return "", err
	}
	if code != 0 {
		return "", fmt.Errorf("mysqldef exited %d\n%s", code, out)
	}
	var b strings.Builder
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		line := sc.Text()
		switch strings.TrimSpace(line) {
		case "-- dry run --", "BEGIN;", "COMMIT;", "-- Nothing is modified --", "":
			continue
		}
		if strings.HasPrefix(line, "-- Skipped:") {
			return "", fmt.Errorf("mysqldef skipped a statement: %s", line)
		}
		b.WriteString(line + "\n")
	}
	return b.String(), sc.Err()
}

// mysqldef runs the pinned mysqldef (tools.mod) with the scratch password.
func mysqldef(args ...string) (string, int, error) {
	cmd := exec.CommandContext(context.Background(), "go", append([]string{"tool", "-modfile=tools.mod", "mysqldef", "--config-inline", "skip_tables: goose_db_version"}, args...)...) //nolint:gosec // G204: fixed tool, arguments built here
	cmd.Env = append(os.Environ(), "MYSQL_PWD=dev")
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return string(out), exit.ExitCode(), nil
	}
	if err != nil {
		return string(out), 0, fmt.Errorf("running mysqldef: %w", err)
	}
	return string(out), 0, nil
}
