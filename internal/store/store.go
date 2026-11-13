package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pressly/goose/v3"

	"github.com/LeventAksakal/notification-system/migrations"
)

// KeyMaxLen is the app_meta key limit; the same number is in
// api/openapi.yaml (maxLength) and schema/app_meta.sql (VARCHAR).
// TestKeyLimitAgreesAcrossSpecServiceAndSchema in internal/api/handlers_test.go
// asserts they agree.
const KeyMaxLen = 64

// Open connects to MySQL. parseTime is forced on so DATETIME scans into
// time.Time, whatever the injected DSN says.
func Open(ctx context.Context, dsn string, maxOpen int) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err) // the error never echoes the password
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql connector: %w", err)
	}
	db := sql.OpenDB(conn)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return db, nil
}

// Migrate applies every pending migration embedded in the binary.
func Migrate(ctx context.Context, db *sql.DB) error {
	p, err := goose.NewProvider(goose.DialectMySQL, db, migrations.FS)
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	results, err := p.Up(ctx)
	if err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	for _, r := range results {
		slog.InfoContext(ctx, "migration applied", "version", r.Source.Version, "path", r.Source.Path, "took", r.Duration)
	}
	return nil
}
