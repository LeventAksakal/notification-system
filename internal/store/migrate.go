package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/pressly/goose/v3"

	"github.com/LeventAksakal/notification-system/migrations"
)

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
