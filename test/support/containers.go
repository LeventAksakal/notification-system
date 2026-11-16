//go:build integration

package support

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"

	"github.com/LeventAksakal/notification-system/internal/store"
)

// mysqlPassword is the password of both the app user and root in the test server.
const mysqlPassword = "test"

// mysqlServer is one MySQL server per test process, started on first use. A
// server start took 25 to 70 s on the loaded Docker Desktop harness, and the
// store package alone started ten, past go test's 10 minute limit; a fresh
// database per test keeps the isolation. Ryuk removes the container when the
// process exits.
var mysqlServer struct {
	once sync.Once
	addr string // host:port
	err  error
}

var mysqlSeq atomic.Int64

func sharedMySQL() (string, error) {
	mysqlServer.once.Do(func() {
		ctx := context.Background() // outlives the test that starts it
		c, err := tcmysql.Run(ctx, MySQLImage,
			tcmysql.WithDatabase("app"), tcmysql.WithUsername("app"), tcmysql.WithPassword(mysqlPassword),
			// As in docker-compose.yml: a non-SUPER user may create the schema's
			// triggers while binary logging is on (decision 0005, "As built").
			testcontainers.WithCmdArgs("--log-bin-trust-function-creators=ON"))
		if err != nil {
			mysqlServer.err = fmt.Errorf("start mysql: %w", err)
			return
		}
		mysqlServer.addr, mysqlServer.err = c.PortEndpoint(ctx, "3306/tcp", "")
	})
	return mysqlServer.addr, mysqlServer.err
}

// MySQL returns a fresh database on the process's MySQL server (the compose
// image), migrated the way `app migrate` does it as the app user, with an open
// pool and its DSN. The database is dropped when the test ends.
func MySQL(t testing.TB) (*sql.DB, string) {
	t.Helper()
	addr, err := sharedMySQL()
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("t_%d_%d", os.Getpid(), mysqlSeq.Add(1))
	root, err := sql.Open("mysql", fmt.Sprintf("root:%s@tcp(%s)/", mysqlPassword, addr))
	if err != nil {
		t.Fatalf("open mysql as root: %v", err)
	}
	defer func() { _ = root.Close() }()
	for _, stmt := range []string{
		"CREATE DATABASE " + name + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci",
		"GRANT ALL PRIVILEGES ON " + name + ".* TO 'app'@'%'",
	} {
		if _, err := root.ExecContext(t.Context(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	dsn := fmt.Sprintf("app:%s@tcp(%s)/%s", mysqlPassword, addr, name)
	db, err := store.Open(t.Context(), dsn, 5)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		r, err := sql.Open("mysql", fmt.Sprintf("root:%s@tcp(%s)/", mysqlPassword, addr))
		if err != nil {
			return
		}
		defer func() { _ = r.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = r.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name)
	})
	if err := store.Migrate(t.Context(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db, dsn
}

// Valkey starts Valkey (the compose image) and returns its host:port.
func Valkey(t testing.TB) string {
	t.Helper()
	ctx := t.Context()
	c, err := tcvalkey.Run(ctx, ValkeyImage)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start valkey: %v", err)
	}
	addr, err := c.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		t.Fatalf("valkey address: %v", err)
	}
	return addr
}
