//go:build integration

package support

import (
	"database/sql"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"

	"github.com/LeventAksakal/notification-system/internal/store"
)

// MySQL starts MySQL (the compose image), applies the embedded migrations the
// way `app migrate` does, and returns an open pool and its DSN.
func MySQL(t testing.TB) (*sql.DB, string) {
	t.Helper()
	ctx := t.Context()
	c, err := tcmysql.Run(ctx, MySQLImage,
		tcmysql.WithDatabase("app"), tcmysql.WithUsername("app"), tcmysql.WithPassword("test"))
	testcontainers.CleanupContainer(t, c) // before the error check: removes a half-started container too
	if err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	dsn, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("mysql dsn: %v", err)
	}
	db, err := store.Open(ctx, dsn, 5)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := store.Migrate(ctx, db); err != nil {
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
