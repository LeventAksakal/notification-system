package store

import (
	"database/sql"
	"time"
	"unicode/utf8"
)

// delayUs is how far dueAt lies ahead of the local clock, in microseconds; the
// due time is then computed on the database clock (NOW(3) + delay).
func delayUs(dueAt time.Time) int64 {
	return max(time.Until(dueAt).Microseconds(), 0)
}

func nullString(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

// cut shortens s to at most n characters, on a rune boundary.
func cut(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n])
}

func nullInt16(status int) sql.NullInt16 {
	if status <= 0 {
		return sql.NullInt16{}
	}
	return sql.NullInt16{Int16: int16(min(status, 999)), Valid: true} //nolint:gosec // bounded
}

func nullClaim(v uint64) sql.NullInt64 {
	return sql.NullInt64{Int64: int64(v), Valid: true} //nolint:gosec // claim versions stay far below 2^63
}
