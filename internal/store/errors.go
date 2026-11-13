package store

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// MySQL server error numbers the services branch on.
const (
	erDupEntry        = 1062
	erLockWaitTimeout = 1205
	erLockDeadlock    = 1213
)

// IsDuplicateKey reports whether err is a primary or unique key violation.
func IsDuplicateKey(err error) bool { return isMySQLError(err, erDupEntry) }

// IsLockConflict reports whether err is a deadlock or a lock wait timeout:
// the transaction was rolled back and may be retried.
func IsLockConflict(err error) bool {
	return isMySQLError(err, erLockDeadlock) || isMySQLError(err, erLockWaitTimeout)
}

func isMySQLError(err error, number uint16) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == number
}
