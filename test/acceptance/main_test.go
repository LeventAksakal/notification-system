//go:build integration

// Package acceptance holds the black-box acceptance tests. They build
// ./cmd/app once, start one MySQL and one Valkey per package run, and run
// `app serve` and `app worker` as child processes configured only through
// the settings in docs/briefs/CONTRACT.md. Tests speak HTTP to the API, read
// the provider double's counters and query the database.
//
// Valkey is shared by every test, so no test calls t.Parallel.
package acceptance

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"

	"github.com/LeventAksakal/notification-system/test/support"
)

const mysqlRootPassword = "test"

// prebuilt runs the tests against an existing binary instead of building
// ./cmd/app: go test -tags integration ./test/acceptance -args -app-bin=PATH
var prebuilt = flag.String("app-bin", "", "use this app binary instead of building ./cmd/app")

var (
	// appBin is the path of the binary built from ./cmd/app by TestMain.
	appBin string
	// repoRoot is the module root; `app` children run there.
	repoRoot string

	infraOnce sync.Once
	infra     sharedInfra
)

// sharedInfra is the MySQL and Valkey every test of this package uses.
type sharedInfra struct {
	mysql *tcmysql.MySQLContainer
	vk    *tcvalkey.ValkeyContainer
	// mysqlAddr is host:port of MySQL; the root user can CREATE DATABASE.
	mysqlAddr  string
	valkeyAddr string
	err        error
}

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		fmt.Fprintln(os.Stderr, "acceptance: repo root:", err)
		return 2
	}
	repoRoot = root
	dir, err := os.MkdirTemp("", "acceptance-app-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "acceptance: temp dir:", err)
		return 2
	}
	defer func() { _ = os.RemoveAll(dir) }()

	flag.Parse()
	if *prebuilt != "" {
		appBin, err = filepath.Abs(*prebuilt)
		if err != nil {
			fmt.Fprintln(os.Stderr, "acceptance: -app-bin:", err)
			return 2
		}
	} else {
		name := "app"
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		appBin = filepath.Join(dir, name)
		build := exec.Command("go", "build", "-o", appBin, "./cmd/app")
		build.Dir = repoRoot
		if out, err := build.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "acceptance: go build ./cmd/app failed: %v\n%s\n", err, out)
			return 2
		}
	}

	code := m.Run()

	if infra.mysql != nil || infra.vk != nil {
		for _, c := range []testcontainers.Container{infra.mysql, infra.vk} {
			if c != nil {
				_ = testcontainers.TerminateContainer(c)
			}
		}
	}
	return code
}

// shared starts MySQL and Valkey on first use and returns them. A failure to
// start fails the calling test (and every later one) with the same error.
func shared(t testing.TB) *sharedInfra {
	t.Helper()
	infraOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		var wg sync.WaitGroup
		var myErr, vkErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			c, err := tcmysql.Run(ctx, support.MySQLImage,
				tcmysql.WithDatabase("app"), tcmysql.WithUsername("root"), tcmysql.WithPassword(mysqlRootPassword))
			infra.mysql = c
			if err != nil {
				myErr = fmt.Errorf("start mysql: %w", err)
				return
			}
			addr, err := c.PortEndpoint(ctx, "3306/tcp", "")
			if err != nil {
				myErr = fmt.Errorf("mysql address: %w", err)
				return
			}
			infra.mysqlAddr = addr
		}()
		go func() {
			defer wg.Done()
			c, err := tcvalkey.Run(ctx, support.ValkeyImage)
			infra.vk = c
			if err != nil {
				vkErr = fmt.Errorf("start valkey: %w", err)
				return
			}
			addr, err := c.PortEndpoint(ctx, "6379/tcp", "")
			if err != nil {
				vkErr = fmt.Errorf("valkey address: %w", err)
				return
			}
			infra.valkeyAddr = addr
		}()
		wg.Wait()
		if myErr != nil {
			infra.err = myErr
		} else if vkErr != nil {
			infra.err = vkErr
		}
	})
	if infra.err != nil {
		t.Fatalf("shared infrastructure: %v", infra.err)
	}
	return &infra
}
