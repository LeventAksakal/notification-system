package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"regexp"
	"syscall"

	"github.com/go-sql-driver/mysql"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// MySQL server error numbers the services branch on.
const (
	erConCount             = 1040 // too many connections
	erServerShutdown       = 1053
	erDupEntry             = 1062
	erTooManyUserConns     = 1203
	erLockWaitTimeout      = 1205
	erLockDeadlock         = 1213
	erClientInteractionEnd = 4031 // the server closed an idle connection
	erCheckViolated        = 3819 // a CHECK constraint refused the row
)

// statusTransitionCheck is the CHECK that holds the state machine in the
// database (schema/notifications.sql); the triggers feed it status_from.
const statusTransitionCheck = "status_transition"

// checkNameRE reads the constraint name out of error 3819's message.
var checkNameRE = regexp.MustCompile(`Check constraint '([^']+)' is violated`)

// CheckViolated returns the name of the CHECK constraint that refused a write.
func CheckViolated(err error) (string, bool) {
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != erCheckViolated {
		return "", false
	}
	m := checkNameRE.FindStringSubmatch(me.Message)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// RuleViolation maps a status change the database refused to
// notification.ErrInvalidTransition, so a refused transition is the same error
// whichever layer caught it; the MySQL error stays in the chain. Any other
// error is returned as it is.
func RuleViolation(err error) error {
	if name, ok := CheckViolated(err); ok && name == statusTransitionCheck {
		return fmt.Errorf("%w (refused by the database): %w", notification.ErrInvalidTransition, err)
	}
	return err
}

// IsDuplicateKey reports whether err is a primary or unique key violation.
func IsDuplicateKey(err error) bool { return isMySQLError(err, erDupEntry) }

// IsLockConflict reports whether err is a deadlock or a lock wait timeout:
// the transaction was rolled back and may be retried.
func IsLockConflict(err error) bool {
	return isMySQLError(err, erLockDeadlock) || isMySQLError(err, erLockWaitTimeout)
}

// IsUnavailable reports whether err means the database cannot be reached
// (or cannot take this request now): no connection could be made or kept, the
// wait for one ran out, the server is shutting down or has no connection to
// spare. Nothing was stored and the same request may succeed later. An error
// the server answered with about the query itself is not unavailability.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, driver.ErrBadConn),
		errors.Is(err, mysql.ErrInvalidConn),
		errors.Is(err, sql.ErrConnDone),
		// The deadline of the request ran out while connecting, waiting for a
		// pooled connection or for the server's answer: the database did not
		// answer in time.
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, syscall.ECONNREFUSED),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.ECONNABORTED):
		return true
	case isMySQLError(err, erConCount), isMySQLError(err, erTooManyUserConns),
		isMySQLError(err, erServerShutdown), isMySQLError(err, erClientInteractionEnd):
		return true
	}
	// A dial that failed (refused, unreachable, timed out, name not found).
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return true
	}
	var dns *net.DNSError
	return errors.As(err, &dns)
}

func isMySQLError(err error, number uint16) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == number
}
