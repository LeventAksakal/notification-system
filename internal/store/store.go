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

// Driver timeouts used when the DSN sets none. Without them a call on a
// dead connection (a MySQL that hangs rather than refuses) waits for the
// kernel's TCP timeout, minutes; a worker's claim has no request deadline.
const (
	defaultDialTimeout = 5 * time.Second
	defaultIOTimeout   = 30 * time.Second // per read or write, not per query
)

// Open connects to MySQL. parseTime is forced on so DATETIME scans into
// time.Time, whatever the injected DSN says.
func Open(ctx context.Context, dsn string, maxOpen int) (*sql.DB, error) {
	cfg, err := driverConfig(dsn)
	if err != nil {
		return nil, err
	}
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

// driverConfig parses dsn and applies what Open forces or defaults.
func driverConfig(dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err) // the error never echoes the password
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultDialTimeout
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = defaultIOTimeout
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = defaultIOTimeout
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	// Arguments are interpolated client-side: one text round trip per
	// statement instead of prepare, execute and close. The close gets no
	// reply, so with a prepared statement every query is write-write-read,
	// which stalls ~40 ms on delayed ACKs behind some proxies (measured
	// behind Docker Desktop: an empty claim took 48 ms at 1.7 ms RTT).
	cfg.InterpolateParams = true
	// NOW(3) and the DATETIME columns are read and written as UTC, the same
	// zone the driver formats time.Time arguments in.
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["time_zone"] = "'+00:00'"
	return cfg, nil
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

// Warm opens n pooled connections up front, so the first queries of a
// worker do not each pay a TCP and auth handshake at once (claims that wait
// on dials finish together and arrive at the provider as a burst).
func Warm(ctx context.Context, db *sql.DB, n int) error {
	conns := make([]*sql.Conn, 0, n)
	defer func() {
		for _, c := range conns {
			_ = c.Close() // back to the idle pool
		}
	}()
	for range n {
		c, err := db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("warm pool: %w", err)
		}
		conns = append(conns, c)
		if err := c.PingContext(ctx); err != nil {
			return fmt.Errorf("warm pool: %w", err)
		}
	}
	return nil
}
