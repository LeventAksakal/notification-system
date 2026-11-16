package store

import (
	"database/sql"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// Queue is the MySQL notification.Queue (decisions 0003, 0018).
type Queue struct {
	db *sql.DB
	q  *sqlc.Queries
}

var _ notification.Queue = (*Queue)(nil)

// NewQueue returns the queue over db (opened with Open).
func NewQueue(db *sql.DB) *Queue { return &Queue{db: db, q: sqlc.New(db)} }
