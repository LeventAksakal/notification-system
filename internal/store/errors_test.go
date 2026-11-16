package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

func TestIsUnavailable(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("create notification: %w", err) }
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"a refused dial", wrap(refused), true},
		{"a dial that timed out", wrap(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("i/o timeout")}), true},
		{"an unknown host", wrap(&net.DNSError{Err: "no such host", Name: "mysql"}), true},
		{"a connection reset mid-read", wrap(&net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}), true},
		{"a bad pooled connection", wrap(driver.ErrBadConn), true},
		{"the driver's invalid connection", wrap(mysql.ErrInvalidConn), true},
		{"a connection already closed", wrap(sql.ErrConnDone), true},
		{"a deadline while waiting for a connection", wrap(context.DeadlineExceeded), true},
		{"too many connections", wrap(&mysql.MySQLError{Number: 1040, Message: "Too many connections"}), true},
		{"the server shutting down", wrap(&mysql.MySQLError{Number: 1053, Message: "Server shutdown in progress"}), true},
		{"a duplicate key from a reachable server", wrap(&mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}), false},
		{"a syntax error from a reachable server", wrap(&mysql.MySQLError{Number: 1064, Message: "syntax"}), false},
		{"a deadlock", wrap(&mysql.MySQLError{Number: 1213, Message: "Deadlock"}), false},
		{"no rows", wrap(sql.ErrNoRows), false},
		{"a cancelled request", wrap(context.Canceled), false},
		{"a write error on an established connection", wrap(&net.OpError{Op: "write", Net: "tcp", Err: errors.New("broken")}), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsUnavailable(tc.err); got != tc.want {
				t.Fatalf("IsUnavailable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRefusedTransitionIsTheStateMachinesError(t *testing.T) {
	check := func(name string) error {
		return fmt.Errorf("finish x: %w", &mysql.MySQLError{Number: 3819, Message: "Check constraint '" + name + "' is violated."})
	}
	cases := []struct {
		name           string
		err            error
		wantTransition bool
	}{
		{"the state machine's CHECK", check("status_transition"), true},
		{"another CHECK", check("lease_only_while_processing"), false},
		{"a duplicate key", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RuleViolation(tc.err)
			if errors.Is(got, notification.ErrInvalidTransition) != tc.wantTransition {
				t.Fatalf("RuleViolation(%v) = %v; ErrInvalidTransition wanted: %v", tc.err, got, tc.wantTransition)
			}
			if tc.err != nil && !errors.Is(got, tc.err) {
				t.Fatalf("RuleViolation dropped the original error: %v", got)
			}
		})
	}
}
