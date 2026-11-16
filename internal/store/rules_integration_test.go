//go:build integration

package store_test

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/test/support"
)

// Every business rule the database holds (decision 0005, "As built") is
// checked here against real MySQL, with hand-written SQL that bypasses the
// Go state machine: the rules must hold for any writer.

// asState is the SET clause that makes a row consistent with status s in
// every column except status_from, so only the state machine can refuse it.
var asState = map[notification.Status]string{
	notification.StatusPending:    `status = 'pending', lease_until = NULL, finished_at = NULL, failure_kind = NULL, provider_message_id = NULL`,
	notification.StatusProcessing: `status = 'processing', lease_until = NOW(3) + INTERVAL 1 MINUTE, finished_at = NULL, failure_kind = NULL, provider_message_id = NULL`,
	notification.StatusSent:       `status = 'sent', lease_until = NULL, finished_at = NOW(3), failure_kind = NULL, provider_message_id = 'm-1', attempts = GREATEST(attempts, 1)`,
	notification.StatusFailed:     `status = 'failed', lease_until = NULL, finished_at = NOW(3), failure_kind = 'exhausted', provider_message_id = NULL, attempts = GREATEST(attempts, 1)`,
	notification.StatusCancelled:  `status = 'cancelled', lease_until = NULL, finished_at = NOW(3), failure_kind = NULL, provider_message_id = NULL`,
}

// pathTo is a sequence of allowed changes from pending to each state.
var pathTo = map[notification.Status][]notification.Status{
	notification.StatusPending:    nil,
	notification.StatusProcessing: {notification.StatusProcessing},
	notification.StatusSent:       {notification.StatusProcessing, notification.StatusSent},
	notification.StatusFailed:     {notification.StatusProcessing, notification.StatusFailed},
	notification.StatusCancelled:  {notification.StatusCancelled},
}

// insertPending inserts one pending sms notification and returns its id.
func insertPending(t *testing.T, db *sql.DB) string {
	t.Helper()
	id := uuid.Must(uuid.NewV7()).String()
	if _, err := db.ExecContext(t.Context(), `INSERT INTO notifications (id, recipient, channel, content, due_at, correlation_id)
		VALUES (?, '+905551234567', 'sms', 'hello', NOW(3), 'corr')`, id); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return id
}

// moveTo writes status s (and the columns it needs) to row id in plain SQL.
func moveTo(t *testing.T, db *sql.DB, id string, s notification.Status) error {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `UPDATE notifications SET `+asState[s]+` WHERE id = ?`, id)
	return err
}

// rowIn inserts a row and walks it to state s through allowed changes.
func rowIn(t *testing.T, db *sql.DB, s notification.Status) string {
	t.Helper()
	id := insertPending(t, db)
	for _, step := range pathTo[s] {
		if err := moveTo(t, db, id, step); err != nil {
			t.Fatalf("walking to %s, step %s: %v", s, step, err)
		}
	}
	return id
}

// wantCheck fails unless err is a refusal by the named CHECK.
func wantCheck(t *testing.T, err error, name string) {
	t.Helper()
	got, ok := store.CheckViolated(err)
	if !ok || got != name {
		t.Fatalf("want a refusal by CHECK %s, got %v", name, err)
	}
}

func TestDatabaseRules(t *testing.T) {
	db, _ := support.MySQL(t)

	t.Run("the database allows exactly the status changes the Go state machine allows", func(t *testing.T) {
		for _, from := range notification.Statuses {
			for _, to := range notification.Statuses {
				if from == to {
					// Writing the same status is not a change: the Go side never
					// issues it and the triggers leave status_from alone.
					continue
				}
				t.Run(fmt.Sprintf("%s to %s", from, to), func(t *testing.T) {
					id := rowIn(t, db, from)
					err := moveTo(t, db, id, to)
					goAllows := notification.Transition(from, to) == nil
					switch {
					case goAllows && err != nil:
						t.Fatalf("Go allows %s -> %s, the database refused it: %v", from, to, err)
					case !goAllows && err == nil:
						t.Fatalf("Go refuses %s -> %s, the database let it through", from, to)
					case !goAllows && !errors.Is(store.RuleViolation(err), notification.ErrInvalidTransition):
						t.Fatalf("%s -> %s refused, but not as ErrInvalidTransition: %v", from, to, err)
					}
				})
			}
		}
	})

	t.Run("a notification can only be inserted as pending", func(t *testing.T) {
		for _, s := range notification.Statuses {
			t.Run(string(s), func(t *testing.T) {
				id := uuid.Must(uuid.NewV7()).String()
				_, err := db.ExecContext(t.Context(), `INSERT INTO notifications SET id = ?, recipient = '+905551234567', channel = 'sms',
					content = 'hello', due_at = NOW(3), correlation_id = 'corr', `+asState[s], id)
				if s == notification.StatusPending {
					if err != nil {
						t.Fatalf("pending insert refused: %v", err)
					}
					return
				}
				wantCheck(t, err, "status_transition")
			})
		}
	})

	t.Run("status_from cannot be forged by a writer", func(t *testing.T) {
		id := uuid.Must(uuid.NewV7()).String()
		_, err := db.ExecContext(t.Context(), `INSERT INTO notifications SET id = ?, recipient = '+905551234567', channel = 'sms',
			content = 'hello', due_at = NOW(3), correlation_id = 'corr', status_from = 'pending', `+asState[notification.StatusProcessing], id)
		wantCheck(t, err, "status_transition")

		id = rowIn(t, db, notification.StatusCancelled)
		_, err = db.ExecContext(t.Context(), `UPDATE notifications SET status_from = 'processing', `+asState[notification.StatusSent]+` WHERE id = ?`, id)
		wantCheck(t, err, "status_transition")
	})

	statusFrom := func(t *testing.T, id string) string {
		t.Helper()
		var from sql.NullString
		if err := db.QueryRowContext(t.Context(), `SELECT status_from FROM notifications WHERE id = ?`, id).Scan(&from); err != nil {
			t.Fatalf("read status_from: %v", err)
		}
		return from.String
	}

	t.Run("an update that leaves status alone passes and keeps the pair it had", func(t *testing.T) {
		pending := rowIn(t, db, notification.StatusPending)
		if _, err := db.ExecContext(t.Context(), `UPDATE notifications SET due_at = NOW(3) + INTERVAL 1 MINUTE, last_error = 'x' WHERE id = ?`, pending); err != nil {
			t.Fatalf("update of a pending row without a status change: %v", err)
		}
		if got := statusFrom(t, pending); got != "created" {
			t.Fatalf("status_from = %q after an update without a status change, want created", got)
		}
		processing := rowIn(t, db, notification.StatusProcessing)
		if _, err := db.ExecContext(t.Context(), `UPDATE notifications SET status = 'processing', lease_until = lease_until + INTERVAL 1 MINUTE WHERE id = ?`, processing); err != nil {
			t.Fatalf("rewriting the same status: %v", err)
		}
		if got := statusFrom(t, processing); got != "pending" {
			t.Fatalf("status_from = %q after rewriting the same status, want pending", got)
		}
	})

	t.Run("an update that leaves status alone cannot forge status_from", func(t *testing.T) {
		id := rowIn(t, db, notification.StatusCancelled)
		if _, err := db.ExecContext(t.Context(), `UPDATE notifications SET status_from = 'failed' WHERE id = ?`, id); err != nil {
			t.Fatalf("update of status_from alone: %v", err)
		}
		if got := statusFrom(t, id); got != "pending" {
			t.Fatalf("status_from = %q after a forging update, want pending (it came from pending)", got)
		}
		// The forged origin would have made cancelled -> pending look like a replay.
		wantCheck(t, moveTo(t, db, id, notification.StatusPending), "status_transition")
	})

	t.Run("each business rule CHECK refuses a row that breaks it", func(t *testing.T) {
		cases := []struct {
			name  string
			from  notification.Status
			set   string
			check string
		}{
			{"finished_at missing on a terminal row", notification.StatusCancelled, `finished_at = NULL`, "finished_exactly_when_terminal"},
			{"finished_at on a pending row", notification.StatusPending, `finished_at = NOW(3)`, "finished_exactly_when_terminal"},
			{"no lease while processing", notification.StatusProcessing, `lease_until = NULL`, "lease_only_while_processing"},
			{"a lease while pending", notification.StatusPending, `lease_until = NOW(3)`, "lease_only_while_processing"},
			{"a provider message id on a failed row", notification.StatusFailed, `provider_message_id = 'm-2'`, "provider_id_only_when_sent"},
			{"a failure kind on a sent row", notification.StatusSent, `failure_kind = 'permanent'`, "failure_kind_only_when_failed"},
			{"sent without an attempt", notification.StatusSent, `attempts = 0`, "attempted_before_sent_or_failed"},
			{"a batch position without a batch", notification.StatusPending, `batch_index = 3`, "batch_position_with_batch"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				id := rowIn(t, db, tc.from)
				_, err := db.ExecContext(t.Context(), `UPDATE notifications SET `+tc.set+` WHERE id = ?`, id)
				wantCheck(t, err, tc.check)
			})
		}
	})

	t.Run("two notifications cannot hold the same batch position", func(t *testing.T) {
		batch := uuid.Must(uuid.NewV7()).String()
		if _, err := db.ExecContext(t.Context(), `INSERT INTO batches (id, total, correlation_id) VALUES (?, 2, 'corr')`, batch); err != nil {
			t.Fatalf("insert batch: %v", err)
		}
		for i, id := range []string{insertPending(t, db), insertPending(t, db)} {
			_, err := db.ExecContext(t.Context(), `UPDATE notifications SET batch_id = ?, batch_index = 0 WHERE id = ?`, batch, id)
			if i == 0 && err != nil {
				t.Fatalf("first row at position 0: %v", err)
			}
			if i == 1 && !store.IsDuplicateKey(err) {
				t.Fatalf("second row at position 0: want a duplicate key, got %v", err)
			}
		}
	})

	t.Run("the database allows exactly the attempt outcome changes the Go state machine allows", func(t *testing.T) {
		id := rowIn(t, db, notification.StatusProcessing)
		claim := 0
		for _, from := range notification.AttemptOutcomes {
			for _, to := range notification.AttemptOutcomes {
				if from == to {
					continue // not a change; the triggers leave outcome_from alone
				}
				t.Run(fmt.Sprintf("%s to %s", from, to), func(t *testing.T) {
					claim++
					attemptIn(t, db, id, claim, from)
					err := setAttempt(t, db, id, claim, to)
					goAllows := notification.AttemptTransition(from, to) == nil
					switch {
					case goAllows && err != nil:
						t.Fatalf("Go allows %s -> %s, the database refused it: %v", from, to, err)
					case !goAllows && err == nil:
						t.Fatalf("Go refuses %s -> %s, the database let it through", from, to)
					case !goAllows:
						wantCheck(t, err, "attempt_outcome_transition")
					}
				})
			}
		}
	})

	t.Run("an attempt can only be inserted in flight, with its claim", func(t *testing.T) {
		id := rowIn(t, db, notification.StatusProcessing)
		for i, o := range notification.AttemptOutcomes {
			t.Run(string(o), func(t *testing.T) {
				_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_attempts SET notification_id = ?, attempt_no = 1,
					channel = 'sms', started_at = NOW(3), claim_version = ?, `+attemptResult[o], id, 100+i)
				if o == notification.OutcomeInFlight {
					if err != nil {
						t.Fatalf("in_flight insert refused: %v", err)
					}
					return
				}
				wantCheck(t, err, "attempt_outcome_transition")
			})
		}
		_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_attempts SET notification_id = ?, attempt_no = 1,
			channel = 'sms', started_at = NOW(3), outcome_from = 'in_flight', claim_version = 200, `+attemptResult[notification.OutcomeAccepted], id)
		wantCheck(t, err, "attempt_outcome_transition")
		_, err = db.ExecContext(t.Context(), `INSERT INTO delivery_attempts SET notification_id = ?, attempt_no = 1,
			channel = 'sms', started_at = NOW(3), claim_version = NULL, `+attemptResult[notification.OutcomeInFlight], id)
		wantCheck(t, err, "attempt_claim_known")
	})

	t.Run("an update that leaves the outcome alone cannot forge outcome_from", func(t *testing.T) {
		id := rowIn(t, db, notification.StatusProcessing)
		attemptIn(t, db, id, 1, notification.OutcomeAccepted)
		if _, err := db.ExecContext(t.Context(), `UPDATE delivery_attempts SET outcome_from = 'unknown'
			WHERE notification_id = ? AND claim_version = 1`, id); err != nil {
			t.Fatalf("update of outcome_from alone: %v", err)
		}
		// Had the forged origin stuck, accepted -> transient would look like a late result.
		wantCheck(t, setAttempt(t, db, id, 1, notification.OutcomeTransient), "attempt_outcome_transition")
	})

	t.Run("one claim makes at most one provider call", func(t *testing.T) {
		id := rowIn(t, db, notification.StatusProcessing)
		attemptIn(t, db, id, 1, notification.OutcomeUnknown)
		_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_attempts SET notification_id = ?, attempt_no = 2,
			channel = 'sms', started_at = NOW(3), claim_version = 1, `+attemptResult[notification.OutcomeInFlight], id)
		var me *mysql.MySQLError
		if !errors.As(err, &me) || me.Number != 1062 {
			t.Fatalf("second attempt on claim 1: want a duplicate key (1062), got %v", err)
		}
	})

	t.Run("each delivery attempt CHECK refuses a row that breaks it", func(t *testing.T) {
		id := rowIn(t, db, notification.StatusProcessing)
		cases := []struct {
			name, result, check string
		}{
			{"a provider message id on a transient attempt", `outcome = 'transient', duration_ms = 5, http_status = 503, error = 'HTTP 503', provider_message_id = 'm-3'`, "attempt_provider_id_only_when_accepted"},
			{"accepted with a 500", `outcome = 'accepted', duration_ms = 5, http_status = 500, provider_message_id = 'm-4'`, "attempt_accepted_is_2xx"},
			{"accepted with no response", `outcome = 'accepted', duration_ms = 5, http_status = NULL, provider_message_id = 'm-5'`, "attempt_accepted_is_2xx"},
			{"an HTTP status below 100", `outcome = 'permanent', duration_ms = 5, http_status = 42, error = 'HTTP 42'`, "attempt_http_status_range"},
			{"a failed attempt without its reason", `outcome = 'permanent', duration_ms = 5, http_status = 400, error = NULL`, "attempt_error_exactly_when_failed_or_unknown"},
			{"an accepted attempt with an error", `outcome = 'accepted', duration_ms = 5, http_status = 202, error = 'oops', provider_message_id = 'm-6'`, "attempt_error_exactly_when_failed_or_unknown"},
			{"an unknown attempt without its reason", `outcome = 'unknown', error = NULL`, "attempt_error_exactly_when_failed_or_unknown"},
			{"an unknown attempt with a response", `outcome = 'unknown', http_status = 503, error = 'lease ran out'`, "attempt_no_result_yet"},
			{"an unknown attempt with a duration", `outcome = 'unknown', duration_ms = 5, error = 'lease ran out'`, "attempt_no_result_yet"},
		}
		for i, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				attemptIn(t, db, id, i+1, notification.OutcomeInFlight)
				_, err := db.ExecContext(t.Context(), `UPDATE delivery_attempts SET `+tc.result+`
					WHERE notification_id = ? AND claim_version = ?`, id, i+1)
				wantCheck(t, err, tc.check)
			})
		}
		inserts := []struct {
			name, values, check string
		}{
			{"attempt number zero", `attempt_no = 0`, "attempt_no_from_one"},
			{"in flight with a response", `attempt_no = 1, http_status = 200`, "attempt_no_result_yet"},
			{"in flight with a duration", `attempt_no = 1, duration_ms = 5`, "attempt_no_result_yet"},
			{"in flight with an error", `attempt_no = 1, error = 'x'`, "attempt_error_exactly_when_failed_or_unknown"},
		}
		for i, tc := range inserts {
			t.Run(tc.name, func(t *testing.T) {
				_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_attempts SET notification_id = ?, channel = 'sms',
					started_at = NOW(3), outcome = 'in_flight', claim_version = ?, `+tc.values, id, 100+i)
				wantCheck(t, err, tc.check)
			})
		}
		// Valid results pass every CHECK: accepted without a message id, a
		// transient without a response (timeout), a late result after unknown.
		for i, result := range []string{
			`outcome = 'accepted', duration_ms = 5, http_status = 202`,
			`outcome = 'transient', duration_ms = 5, http_status = NULL, error = 'timeout'`,
		} {
			attemptIn(t, db, id, 200+i, notification.OutcomeInFlight)
			if _, err := db.ExecContext(t.Context(), `UPDATE delivery_attempts SET `+result+`
				WHERE notification_id = ? AND claim_version = ?`, id, 200+i); err != nil {
				t.Fatalf("valid result (%s) refused: %v", result, err)
			}
		}
		attemptIn(t, db, id, 300, notification.OutcomeUnknown)
		if err := setAttempt(t, db, id, 300, notification.OutcomeAccepted); err != nil {
			t.Fatalf("a late result after unknown refused: %v", err)
		}
	})
}

// attemptResult is the SET clause that makes an attempt consistent with
// outcome o in every column except outcome_from.
var attemptResult = map[notification.AttemptOutcome]string{
	notification.OutcomeInFlight:  `outcome = 'in_flight', duration_ms = 0, http_status = NULL, error = NULL, provider_message_id = NULL`,
	notification.OutcomeAccepted:  `outcome = 'accepted', duration_ms = 5, http_status = 202, error = NULL, provider_message_id = 'm-1'`,
	notification.OutcomeTransient: `outcome = 'transient', duration_ms = 5, http_status = 503, error = 'HTTP 503', provider_message_id = NULL`,
	notification.OutcomePermanent: `outcome = 'permanent', duration_ms = 5, http_status = 400, error = 'HTTP 400', provider_message_id = NULL`,
	notification.OutcomeUnknown:   `outcome = 'unknown', duration_ms = 0, http_status = NULL, error = 'the lease ran out', provider_message_id = NULL`,
}

// setAttempt writes outcome o to the attempt of notification id made by claim.
func setAttempt(t *testing.T, db *sql.DB, id string, claim int, o notification.AttemptOutcome) error {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `UPDATE delivery_attempts SET `+attemptResult[o]+`
		WHERE notification_id = ? AND claim_version = ?`, id, claim)
	return err
}

// attemptIn inserts the attempt of notification id made by claim in flight,
// as the worker does before the call, and walks it to outcome o.
func attemptIn(t *testing.T, db *sql.DB, id string, claim int, o notification.AttemptOutcome) {
	t.Helper()
	res, err := db.ExecContext(t.Context(), `INSERT INTO delivery_attempts (notification_id, attempt_no, channel, started_at, outcome, claim_version)
		SELECT id, ?, channel, NOW(3), 'in_flight', ? FROM notifications WHERE id = ?`, claim, claim, id)
	if err != nil {
		t.Fatalf("attempt in flight: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("attempt in flight: no notification %s", id)
	}
	if o == notification.OutcomeInFlight {
		return
	}
	if err := setAttempt(t, db, id, claim, o); err != nil {
		t.Fatalf("attempt to %s: %v", o, err)
	}
}
