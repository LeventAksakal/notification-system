package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"syscall"

	"github.com/go-sql-driver/mysql"
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
)

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
