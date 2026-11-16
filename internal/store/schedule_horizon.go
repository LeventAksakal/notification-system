package store

import (
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// erCheckConstraintViolated is MySQL's error number when a CHECK refuses a row.
const erCheckConstraintViolated = 3819

// scheduleHorizonCheck is the CHECK in schema/notifications.sql that holds
// scheduled_at within notification.ScheduleHorizon of the database clock.
const scheduleHorizonCheck = "scheduled_within_horizon"

// IsBeyondScheduleHorizon reports whether err is the database refusing a
// scheduled_at further ahead than the horizon (decision 0016). The service
// checks the horizon first on the same clock, so this is the last line: it
// fires only if the clock read before the write disagrees with the write's
// own NOW(3), e.g. after the server's clock stepped back.
func IsBeyondScheduleHorizon(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == erCheckConstraintViolated &&
		strings.Contains(me.Message, "'"+scheduleHorizonCheck+"'")
}
