//go:build integration

package support_test

import (
	"testing"

	"github.com/valkey-io/valkey-go"

	"github.com/LeventAksakal/notification-system/test/support"
)

func TestMySQLHelperReturnsAMigratedDatabase(t *testing.T) {
	db, dsn := support.MySQL(t)
	if dsn == "" {
		t.Fatal("empty dsn")
	}
	var n int
	// goose records every applied migration; at least the init one must be there.
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM goose_db_version WHERE version_id > 0").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Errorf("%d migrations applied, want at least 1", n)
	}
}

func TestValkeyHelperReturnsAReachableServer(t *testing.T) {
	addr := support.Valkey(t)
	c, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Do(t.Context(), c.B().Ping().Build()).Error(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}
