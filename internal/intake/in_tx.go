package intake

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

func (s *Service) inTx(ctx context.Context, fn func(q *sqlc.Queries, tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(s.q.WithTx(tx), tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
