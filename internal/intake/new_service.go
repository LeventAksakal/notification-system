package intake

import (
	"database/sql"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// Service is the intake side of the system. It is safe for concurrent use.
type Service struct {
	db        *sql.DB
	q         *sqlc.Queries
	pub       BatchPublisher
	waker     notification.Waker // nil: no wake hints
	retention time.Duration
	now       func() time.Time
}

// New returns a Service storing in db and announcing every committed status
// change through pub. An Idempotency-Key is remembered for retention. A pub
// without PublishAll is wrapped so a request still waits at most PublishBudget.
func New(db *sql.DB, pub notification.Publisher, retention time.Duration) *Service {
	bp, ok := pub.(BatchPublisher)
	if !ok {
		bp = sequential{pub}
	}
	return &Service{db: db, q: sqlc.New(db), pub: bp, retention: retention, now: time.Now}
}
