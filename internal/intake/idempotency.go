package intake

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// errKeyTaken marks the duplicate-key error of the idempotency row itself.
var errKeyTaken = errors.New("idempotency key taken")

// maxTxAttempts bounds the retries of a create whose transaction lost a lock
// race (deadlock, lock wait timeout) or found an expired key.
const maxTxAttempts = 5

// store runs write in a transaction that, with an Idempotency-Key, first
// inserts the key row (decision 0012). The primary key decides a race: a
// second request with the same key blocks on the first one's row lock and
// gets a duplicate-key error once it commits. It returns the id of the
// resource the request resolved to: resourceID when written now, the earlier
// resource's id when the key replays.
func (s *Service) store(ctx context.Context, key string, hash []byte, kind sqlc.IdempotencyKeysResourceKind, resourceID string,
	write func(q *sqlc.Queries, tx *sql.Tx) error,
) (string, error) {
	var last error
	for range maxTxAttempts {
		err := s.inTx(ctx, func(q *sqlc.Queries, tx *sql.Tx) error {
			if key != "" {
				if err := q.InsertIdempotencyKey(ctx, sqlc.InsertIdempotencyKeyParams{
					IdemKey: key, RequestHash: hash, ResourceKind: kind, ResourceID: resourceID,
				}); err != nil {
					if store.IsDuplicateKey(err) {
						return errKeyTaken
					}
					return fmt.Errorf("insert idempotency key: %w", err)
				}
			}
			return write(q, tx)
		})
		switch {
		case err == nil:
			return resourceID, nil
		case errors.Is(err, errKeyTaken):
			id, retry, err := s.replay(ctx, key, hash, kind)
			if err != nil || !retry {
				return id, err
			}
			last = errKeyTaken
		case store.IsLockConflict(err):
			last = err
		case store.IsBeyondScheduleHorizon(err):
			// The database's own check (see scheduleClock); which item of a
			// batch it refused is not reported, so neither is an index.
			return "", invalid("scheduledAt", horizonReason())
		default:
			return "", err
		}
	}
	return "", fmt.Errorf("store after %d attempts: %w", maxTxAttempts, last)
}

// earlier looks key up before a create does any work. ok reports a stored
// resource for the same request, whose id is returned; a different request
// under the key is ErrIdempotencyKeyReused. An absent or expired key (or no
// key) lets the create go on; the insert's primary key still decides a race.
func (s *Service) earlier(ctx context.Context, key string, hash []byte, kind sqlc.IdempotencyKeysResourceKind) (id string, ok bool, err error) {
	if key == "" {
		return "", false, nil
	}
	id, retry, err := s.replay(ctx, key, hash, kind)
	if err != nil || retry {
		return "", false, err
	}
	return id, true, nil
}

// checkIdempotencyKey repeats the spec's rule for the header (1 to
// MaxIdempotencyKeyLen visible ASCII characters), so a key the ASCII column
// cannot hold is a 400, never a database error.
func checkIdempotencyKey(key string) error {
	if key == "" {
		return nil
	}
	if len(key) > notification.MaxIdempotencyKeyLen {
		return invalid(idempotencyKeyField, fmt.Sprintf("at most %d characters", notification.MaxIdempotencyKeyLen))
	}
	for i := range len(key) {
		if key[i] < '!' || key[i] > '~' {
			return invalid(idempotencyKeyField, "visible ASCII characters only, no spaces")
		}
	}
	return nil
}

// idempotencyKeyField names the header in a refusal's details.
const idempotencyKeyField = "Idempotency-Key"

// replay resolves a taken key: the earlier resource when the payload is the
// same, ErrIdempotencyKeyReused when not. retry asks the caller to try the
// insert again: the key expired (and was just forgotten) or vanished.
func (s *Service) replay(ctx context.Context, key string, hash []byte, kind sqlc.IdempotencyKeysResourceKind) (id string, retry bool, err error) {
	for range maxTxAttempts {
		row, err := s.q.GetIdempotencyKey(ctx, key)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return "", true, nil
		case store.IsLockConflict(err):
			continue
		case err != nil:
			return "", false, fmt.Errorf("read idempotency key: %w", err)
		}
		if row.CreatedAt.Before(s.now().Add(-s.retention)) {
			if _, err := s.q.DeleteExpiredIdempotencyKey(ctx, sqlc.DeleteExpiredIdempotencyKeyParams{
				IdemKey: key, CreatedAt: s.now().Add(-s.retention).UTC(),
			}); err != nil && !store.IsLockConflict(err) {
				return "", false, fmt.Errorf("forget expired idempotency key: %w", err)
			}
			return "", true, nil
		}
		if row.ResourceKind != kind || !bytes.Equal(row.RequestHash, hash) {
			return "", false, ErrIdempotencyKeyReused
		}
		return row.ResourceID, false, nil
	}
	return "", true, nil
}

// requestHash is the SHA-256 of the canonical payload: the kind and the
// request as decoded, re-encoded with a fixed field order and sorted map keys,
// the priority defaulted and times in UTC.
func requestHash(kind string, v any) []byte {
	canon := func(r Request) Request {
		if r.Priority == "" {
			r.Priority = notification.PriorityNormal
		}
		if r.ScheduledAt != nil {
			t := r.ScheduledAt.UTC()
			r.ScheduledAt = &t
		}
		return r
	}
	switch x := v.(type) {
	case Request:
		v = canon(x)
	case []Request:
		c := make([]Request, len(x))
		for i, r := range x {
			c[i] = canon(r)
		}
		v = c
	}
	b, err := json.Marshal(struct {
		Kind    string `json:"kind"`
		Payload any    `json:"payload"`
	}{kind, v})
	if err != nil {
		panic(fmt.Sprintf("encode request for hashing: %v", err)) // plain strings, maps and times always encode
	}
	sum := sha256.Sum256(b)
	return sum[:]
}
