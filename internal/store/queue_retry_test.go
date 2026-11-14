package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/LeventAksakal/notification-system/internal/core"
)

// lockDB is a database/sql driver that answers every statement with one
// affected row, except that the statements starting with failOn get a MySQL
// lock conflict while fails > 0 (fails < 0: always). It counts what the queue
// did, so a test sees whole transactions retried.
type lockDB struct {
	mu        sync.Mutex
	failOn    string
	fails     int
	errNo     uint16
	execs     map[string]int // by first two words of the statement
	begins    int
	commits   int
	rollbacks int
}

func (d *lockDB) Connect(context.Context) (driver.Conn, error) { return &lockConn{d: d}, nil }
func (d *lockDB) Driver() driver.Driver                        { return nil }

func (d *lockDB) count(f func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	f()
}

type lockConn struct{ d *lockDB }

func (c *lockConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("lockDB: no prepare") }
func (c *lockConn) Close() error                        { return nil }
func (c *lockConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *lockConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.d.count(func() { c.d.begins++ })
	return lockTx{d: c.d}, nil
}

func (c *lockConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	d := c.d
	d.mu.Lock()
	defer d.mu.Unlock()
	f := strings.Fields(strings.TrimSpace(query[strings.Index(query, "\n")+1:]))
	key := strings.Join(f[:min(2, len(f))], " ")
	d.execs[key]++
	if strings.HasPrefix(key, d.failOn) && d.fails != 0 {
		if d.fails > 0 {
			d.fails--
		}
		return nil, &mysql.MySQLError{Number: d.errNo, Message: "Deadlock found when trying to get lock; try restarting transaction"}
	}
	return driver.RowsAffected(1), nil
}

type lockTx struct{ d *lockDB }

func (t lockTx) Commit() error   { t.d.count(func() { t.d.commits++ }); return nil }
func (t lockTx) Rollback() error { t.d.count(func() { t.d.rollbacks++ }); return nil }

func newLockQueue(t *testing.T, failOn string, fails int) (*Queue, *lockDB) {
	t.Helper()
	d := &lockDB{failOn: failOn, fails: fails, errNo: erLockDeadlock, execs: map[string]int{}}
	db := sql.OpenDB(d)
	t.Cleanup(func() { _ = db.Close() })
	return NewQueue(db), d
}

func lockJob() (core.Job, core.Attempt) {
	return core.Job{ID: "01J00000000000000000000000", Channel: core.ChannelSMS, ClaimVersion: 3, Attempts: 0},
		core.Attempt{Outcome: core.OutcomeAccepted, StartedAt: time.Now(), ProviderMessageID: "p-1"}
}

// TestQueueWritesRetryALockConflict: every result write and Release runs its
// whole transaction again after a deadlock and then succeeds.
func TestQueueWritesRetryALockConflict(t *testing.T) {
	writes := map[string]func(*Queue, core.Job, core.Attempt) error{
		"MarkSent": func(q *Queue, j core.Job, a core.Attempt) error { return q.MarkSent(context.Background(), j, a) },
		"Reschedule": func(q *Queue, j core.Job, a core.Attempt) error {
			return q.Reschedule(context.Background(), j, a, time.Now().Add(time.Second))
		},
		"MarkFailed": func(q *Queue, j core.Job, a core.Attempt) error {
			return q.MarkFailed(context.Background(), j, a, core.FailurePermanent)
		},
	}
	for name, write := range writes {
		for _, failOn := range []string{"UPDATE", "INSERT"} {
			t.Run(name+"/deadlock on "+failOn, func(t *testing.T) {
				q, d := newLockQueue(t, failOn, 1)
				j, a := lockJob()
				if err := write(q, j, a); err != nil {
					t.Fatalf("%s after one deadlock: %v, want nil", name, err)
				}
				// The whole transaction ran twice: update and insert each
				// again, the first transaction rolled back, one commit.
				if d.begins != 2 || d.commits != 1 {
					t.Errorf("begins %d commits %d, want 2 and 1", d.begins, d.commits)
				}
				if d.execs["UPDATE notifications"] != 2 {
					t.Errorf("update ran %d times, want 2 (the whole transaction again)", d.execs["UPDATE notifications"])
				}
				wantInserts := 1
				if failOn == "INSERT" {
					wantInserts = 2
				}
				if d.execs["INSERT INTO"] != wantInserts {
					t.Errorf("insert ran %d times, want %d", d.execs["INSERT INTO"], wantInserts)
				}
			})
		}
	}
	t.Run("Release", func(t *testing.T) {
		q, d := newLockQueue(t, "UPDATE", 1)
		j, _ := lockJob()
		if err := q.Release(context.Background(), j, time.Now()); err != nil {
			t.Fatalf("Release after one deadlock: %v, want nil", err)
		}
		if d.execs["UPDATE notifications"] != 2 {
			t.Errorf("release ran %d times, want 2", d.execs["UPDATE notifications"])
		}
	})
	t.Run("Release after a lock wait timeout", func(t *testing.T) {
		q, d := newLockQueue(t, "UPDATE", 1)
		d.errNo = erLockWaitTimeout
		j, _ := lockJob()
		if err := q.Release(context.Background(), j, time.Now()); err != nil {
			t.Fatalf("Release after one lock wait timeout: %v, want nil", err)
		}
	})
}

// TestQueueWritesGiveUpAfterBoundedRetries: a conflict that does not clear is
// tried lockRetries times in all and then returned as the lock conflict it is,
// never as a lost lease.
func TestQueueWritesGiveUpAfterBoundedRetries(t *testing.T) {
	start := time.Now()
	q, d := newLockQueue(t, "UPDATE", -1)
	j, a := lockJob()
	err := q.MarkSent(context.Background(), j, a)
	if !IsLockConflict(err) || errors.Is(err, core.ErrLeaseLost) {
		t.Fatalf("MarkSent under a lasting deadlock: %v, want the lock conflict", err)
	}
	if d.execs["UPDATE notifications"] != lockRetries || d.commits != 0 || d.rollbacks != lockRetries {
		t.Errorf("updates %d commits %d rollbacks %d, want %d, 0, %d", d.execs["UPDATE notifications"], d.commits, d.rollbacks, lockRetries, lockRetries)
	}
	// Waits: (1+2+3) * lockBackoff * at most 1.5, plus scheduling slack.
	if el := time.Since(start); el > time.Second {
		t.Errorf("gave up after %v, want a short bounded wait", el)
	}

	q, d = newLockQueue(t, "UPDATE", -1)
	if err := q.Release(context.Background(), j, time.Now()); !IsLockConflict(err) {
		t.Fatalf("Release under a lasting deadlock: %v, want the lock conflict", err)
	}
	if d.execs["UPDATE notifications"] != lockRetries {
		t.Errorf("release ran %d times, want %d", d.execs["UPDATE notifications"], lockRetries)
	}
}

// TestQueueWriteRetryHonoursContext: once ctx is done no further attempt is made.
func TestQueueWriteRetryHonoursContext(t *testing.T) {
	q, d := newLockQueue(t, "UPDATE", -1)
	ctx, cancel := context.WithCancel(context.Background())
	j, _ := lockJob()
	// The driver gets ctx too; cancel after the first failure by pinning the
	// jitter hook, which runs between attempts.
	old := lockJitter
	t.Cleanup(func() { lockJitter = old })
	lockJitter = func(n int64) int64 { cancel(); return n - 1 }
	if err := q.Release(ctx, j, time.Now()); !IsLockConflict(err) {
		t.Fatalf("Release with ctx cancelled between attempts: %v, want the lock conflict", err)
	}
	if d.execs["UPDATE notifications"] != 1 {
		t.Errorf("release ran %d times after ctx was done, want 1", d.execs["UPDATE notifications"])
	}
}

// TestQueueWriteDoesNotRetryOtherErrors: only a lock conflict is retried.
func TestQueueWriteDoesNotRetryOtherErrors(t *testing.T) {
	q, d := newLockQueue(t, "UPDATE", -1)
	d.errNo = erDupEntry
	j, a := lockJob()
	if err := q.MarkSent(context.Background(), j, a); err == nil || IsLockConflict(err) {
		t.Fatalf("MarkSent: %v, want the duplicate key error", err)
	}
	if d.execs["UPDATE notifications"] != 1 {
		t.Errorf("update ran %d times, want 1", d.execs["UPDATE notifications"])
	}
}
