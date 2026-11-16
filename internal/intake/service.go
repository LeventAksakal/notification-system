package intake

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
	"github.com/LeventAksakal/notification-system/internal/telemetry"
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

// BatchPublisher is a notification.Publisher that also sends many events as one call
// with one bounded wait (realtime.Publisher), so a batch's announcement never
// costs one wait per item.
type BatchPublisher interface {
	notification.Publisher
	PublishAll(ctx context.Context, es []notification.Event) error
}

// PublishBudget bounds the time a request spends announcing its changes when
// pub has no batch form: every event of the request shares this one deadline.
const PublishBudget = time.Second

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

// WithWaker makes s send a wake hint after every commit that makes a
// notification pending (create, batch, replay), so an idle worker claims it
// at once instead of on its safety poll. Call it before s is used.
func (s *Service) WithWaker(w notification.Waker) *Service {
	s.waker = w
	return s
}

// wake sends hints, best effort: an error is logged, never returned. It runs
// after the commit, so a worker it wakes finds the rows.
func (s *Service) wake(ctx context.Context, hints ...notification.Wake) {
	if s.waker == nil || len(hints) == 0 {
		return
	}
	if err := s.waker.Wake(ctx, hints...); err != nil {
		slog.WarnContext(ctx, "wake hint", "count", len(hints), "err", err)
	}
}

// hintFor is the wake hint for a stored pending notification.
func hintFor(n sqlc.Notification) notification.Wake {
	return notification.Wake{Channel: notification.Channel(n.Channel), Priority: notification.Priority(n.Priority), Due: n.DueAt}
}

// sequential gives a plain notification.Publisher a batch form: one event after the
// other under one shared deadline, stopping at the first error.
type sequential struct{ notification.Publisher }

func (p sequential) PublishAll(ctx context.Context, es []notification.Event) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), PublishBudget)
	defer cancel()
	for i, e := range es {
		if err := p.Publish(ctx, e); err != nil {
			return fmt.Errorf("publish event %d of %d: %w", i+1, len(es), err)
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("publish: %d of %d events left: %w", len(es)-i-1, len(es), err)
		}
	}
	return nil
}

// Meta is what a create request carries besides its body.
type Meta struct {
	IdempotencyKey string // "" when absent
	CorrelationID  string
	Traceparent    string // "" when the request has no span context
}

// Notification is a stored notification as the API shows it.
type Notification struct {
	sqlc.Notification
	// Scheduled: stored pending, never tried, due in the future (database clock).
	Scheduled bool
	// History is the attempt history, oldest first; only filled by Get.
	History []sqlc.DeliveryAttempt
}

// Status is the API status: the stored one, or "scheduled".
func (n Notification) Status() string {
	if n.Scheduled {
		return "scheduled"
	}
	return string(n.Notification.Status)
}

// BatchCreated is the answer to an accepted batch.
type BatchCreated struct {
	ID              string
	Total           int
	NotificationIDs []string // in request order
	CreatedAt       time.Time
}

// Batch is one page of a batch with its counts per API status.
type Batch struct {
	sqlc.Batch
	Counts     map[string]int
	Items      []Notification
	NextCursor string
}

// Filter narrows a notification list. Status is an API status.
type Filter struct {
	Status      string
	Channel     notification.Channel
	BatchID     string
	CreatedFrom *time.Time
	CreatedTo   *time.Time
	Limit       int
	Cursor      string
}

// Page is one page of a list, newest first.
type Page struct {
	Items      []Notification
	NextCursor string // "" on the last page
}

// errKeyTaken marks the duplicate-key error of the idempotency row itself.
var errKeyTaken = errors.New("idempotency key taken")

// maxTxAttempts bounds the retries of a create whose transaction lost a lock
// race (deadlock, lock wait timeout) or found an expired key.
const maxTxAttempts = 5

// Create validates, renders and stores one notification. replayed reports
// that the Idempotency-Key matched an earlier identical request, whose
// notification is returned.
func (s *Service) Create(ctx context.Context, r Request, m Meta) (n Notification, replayed bool, err error) {
	if err := checkIdempotencyKey(m.IdempotencyKey); err != nil {
		return Notification{}, false, err
	}
	// A replay answers from the stored resource before anything that reads
	// current state (templates, the clock): the first response is the answer.
	hash := requestHash("notification", r)
	if id, ok, err := s.earlier(ctx, m.IdempotencyKey, hash, sqlc.IdempotencyKeysResourceKindNotification); err != nil || ok {
		if err != nil {
			return Notification{}, false, err
		}
		return s.replayed(ctx, id)
	}
	templates, err := s.loadTemplates(ctx, []Request{r})
	if err != nil {
		return Notification{}, false, err
	}
	now, err := s.scheduleClock(ctx, []Request{r})
	if err != nil {
		return Notification{}, false, err
	}
	a, errs := Validate(r, templates, now)
	if len(errs) > 0 {
		return Notification{}, false, &ValidationError{Details: errs}
	}
	id := uuid.Must(uuid.NewV7())
	createdAt := idTime(id)
	params := sqlc.InsertNotificationParams{
		ID:            id.String(),
		Recipient:     a.To,
		Channel:       sqlc.NotificationsChannel(a.Channel),
		Priority:      sqlc.NotificationsPriority(a.Priority),
		Content:       a.Content,
		ScheduledAt:   nullTime(a.ScheduledAt),
		CorrelationID: m.CorrelationID,
		Traceparent:   sql.NullString{String: m.Traceparent, Valid: m.Traceparent != ""},
		CreatedAt:     createdAt,
	}
	stored, err := s.store(ctx, m.IdempotencyKey, hash, sqlc.IdempotencyKeysResourceKindNotification, params.ID,
		func(q *sqlc.Queries, _ *sql.Tx) error { return q.InsertNotification(ctx, params) })
	if err != nil {
		return Notification{}, false, err
	}
	if stored != params.ID {
		return s.replayed(ctx, stored)
	}
	n, err = s.get(ctx, stored)
	if err != nil {
		return Notification{}, false, err
	}
	lctx := telemetry.WithNotification(ctx, n.ID, n.CorrelationID)
	slog.InfoContext(lctx, "notification accepted", "channel", a.Channel, "priority", a.Priority, "scheduled", n.Scheduled)
	s.publishCreated(lctx, n.Notification)
	s.wake(lctx, hintFor(n.Notification))
	return n, false, nil
}

// CreateBatch validates every item, then stores the batch and all of its
// notifications in one transaction, or nothing (decision 0011).
func (s *Service) CreateBatch(ctx context.Context, rs []Request, m Meta) (b BatchCreated, replayed bool, err error) {
	if len(rs) == 0 || len(rs) > notification.MaxBatchSize {
		return BatchCreated{}, false, invalid("notifications", fmt.Sprintf("must hold 1 to %d notifications", notification.MaxBatchSize))
	}
	if err := checkIdempotencyKey(m.IdempotencyKey); err != nil {
		return BatchCreated{}, false, err
	}
	hash := requestHash("batch", rs)
	if id, ok, err := s.earlier(ctx, m.IdempotencyKey, hash, sqlc.IdempotencyKeysResourceKindBatch); err != nil || ok {
		if err != nil {
			return BatchCreated{}, false, err
		}
		b, err = s.batchCreated(ctx, id)
		return b, err == nil, err
	}
	templates, err := s.loadTemplates(ctx, rs)
	if err != nil {
		return BatchCreated{}, false, err
	}
	now, err := s.scheduleClock(ctx, rs)
	if err != nil {
		return BatchCreated{}, false, err
	}
	items := make([]store.BatchItem, len(rs))
	var all []FieldError
	for i, r := range rs {
		a, errs := Validate(r, templates, now)
		for _, e := range errs {
			e.Index = &i
			all = append(all, e)
		}
		items[i] = store.BatchItem{BatchIndex: i, Recipient: a.To, Channel: string(a.Channel),
			Priority: string(a.Priority), Content: a.Content, ScheduledAt: a.ScheduledAt}
	}
	if len(all) > 0 {
		return BatchCreated{}, false, &ValidationError{Details: all}
	}

	batchID := uuid.Must(uuid.NewV7())
	b = BatchCreated{ID: batchID.String(), Total: len(rs), CreatedAt: idTime(batchID), NotificationIDs: make([]string, len(rs))}
	for i := range items {
		items[i].ID = uuid.Must(uuid.NewV7()).String()
		b.NotificationIDs[i] = items[i].ID
	}
	meta := store.BatchMeta{BatchID: b.ID, CorrelationID: m.CorrelationID, Traceparent: m.Traceparent, CreatedAt: b.CreatedAt}
	stored, err := s.store(ctx, m.IdempotencyKey, hash, sqlc.IdempotencyKeysResourceKindBatch, b.ID,
		func(q *sqlc.Queries, tx *sql.Tx) error {
			if err := q.InsertBatch(ctx, sqlc.InsertBatchParams{
				ID: b.ID, Total: uint32(len(rs)), CorrelationID: m.CorrelationID, CreatedAt: b.CreatedAt, //nolint:gosec // at most MaxBatchSize
			}); err != nil {
				return fmt.Errorf("insert batch: %w", err)
			}
			return store.InsertBatchItems(ctx, tx, meta, items)
		})
	if err != nil {
		return BatchCreated{}, false, err
	}
	if stored != b.ID {
		b, err = s.batchCreated(ctx, stored)
		return b, true, err
	}

	slog.InfoContext(ctx, "batch accepted", "batch_id", b.ID, "total", b.Total)
	events := make([]notification.Event, len(items))
	for i, it := range items {
		events[i] = notification.Event{
			NotificationID: it.ID, BatchID: b.ID, Status: notification.StatusPending, At: b.CreatedAt, CorrelationID: m.CorrelationID,
			Seq: 1,
		}
	}
	// One pipelined publish with one deadline, whatever the batch size.
	if err := s.pub.PublishAll(ctx, events); err != nil {
		slog.WarnContext(ctx, "publish batch status changes", "batch_id", b.ID, "count", len(events), "err", err)
	}
	// One pipelined hint per channel and priority (the waker keeps the
	// earliest due time of each).
	hints := make([]notification.Wake, len(items))
	for i, it := range items {
		due := b.CreatedAt // due now: any time not after the commit
		if it.ScheduledAt != nil {
			due = *it.ScheduledAt
		}
		hints[i] = notification.Wake{Channel: notification.Channel(it.Channel), Priority: notification.Priority(it.Priority), Due: due}
	}
	s.wake(ctx, hints...)
	return b, false, nil
}

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

// scheduleClock reads the database clock when any of rs carries a schedule,
// once per request whatever the batch size, so the horizon is measured on the
// clock that sets due_at and runs the claim (decision 0016). A request
// without a schedule reads nothing and gets the zero time, which Validate
// does not look at then. The scheduled_within_horizon CHECK repeats the rule
// in the INSERT with the statement's own NOW(3), which is never earlier than
// this read, so it only refuses what this check accepted if the server clock
// stepped back in between.
func (s *Service) scheduleClock(ctx context.Context, rs []Request) (time.Time, error) {
	for _, r := range rs {
		if r.ScheduledAt != nil {
			now, err := s.q.DatabaseNow(ctx)
			if err != nil {
				return time.Time{}, fmt.Errorf("read the database clock: %w", err)
			}
			return now, nil
		}
	}
	return time.Time{}, nil
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

// replayed answers a create replay with the stored notification.
func (s *Service) replayed(ctx context.Context, id string) (Notification, bool, error) {
	n, err := s.get(ctx, id)
	if err != nil {
		return Notification{}, false, err
	}
	slog.InfoContext(telemetry.WithNotification(ctx, n.ID, n.CorrelationID), "notification create replayed",
		"request_correlation_id", telemetry.CorrelationID(ctx))
	return n, true, nil
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

func (s *Service) batchCreated(ctx context.Context, id string) (BatchCreated, error) {
	b, err := s.q.GetBatch(ctx, id)
	if err != nil {
		return BatchCreated{}, fmt.Errorf("read batch %s: %w", id, err)
	}
	ids, err := s.q.ListBatchNotificationIDs(ctx, sql.NullString{String: id, Valid: true})
	if err != nil {
		return BatchCreated{}, fmt.Errorf("read batch %s ids: %w", id, err)
	}
	return BatchCreated{ID: b.ID, Total: int(b.Total), NotificationIDs: ids, CreatedAt: b.CreatedAt}, nil
}

// Get returns one notification with its attempt history.
func (s *Service) Get(ctx context.Context, id string) (Notification, error) {
	n, err := s.get(ctx, id)
	if err != nil {
		return Notification{}, err
	}
	n.History, err = s.q.ListDeliveryAttempts(ctx, id)
	if err != nil {
		return Notification{}, fmt.Errorf("read attempts of %s: %w", id, err)
	}
	if n.History == nil {
		n.History = []sqlc.DeliveryAttempt{}
	}
	return n, nil
}

func (s *Service) get(ctx context.Context, id string) (Notification, error) {
	row, err := s.q.GetNotification(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("read notification %s: %w", id, err)
	}
	return Notification{Notification: row.Notification, Scheduled: row.IsScheduled != 0}, nil
}

// List returns one page of notifications, newest first.
func (s *Service) List(ctx context.Context, f Filter) (Page, error) {
	limit := pageSize(f.Limit)
	lf := store.ListFilter{
		Channel: string(f.Channel), BatchID: f.BatchID, CreatedFrom: f.CreatedFrom, CreatedTo: f.CreatedTo, Limit: limit + 1,
	}
	switch f.Status {
	case "":
	case "scheduled", "pending":
		lf.Status = string(notification.StatusPending)
		only := f.Status == "scheduled"
		lf.Scheduled = &only
	default:
		if !notification.Status(f.Status).Valid() {
			return Page{}, invalid("status", "unknown status")
		}
		lf.Status = f.Status
	}
	if f.Cursor != "" {
		id, err := DecodeListCursor(f.Cursor)
		if err != nil {
			return Page{}, err
		}
		lf.BeforeID = id
	}
	rows, err := store.ListNotifications(ctx, s.db, lf)
	if err != nil {
		return Page{}, err
	}
	page := Page{Items: make([]Notification, 0, min(len(rows), limit))}
	for i, r := range rows {
		if i == limit {
			page.NextCursor = EncodeListCursor(page.Items[limit-1].ID)
			break
		}
		page.Items = append(page.Items, Notification{Notification: r.Notification, Scheduled: r.IsScheduled})
	}
	return page, nil
}

// Cancel moves a pending notification to cancelled (decision 0005).
func (s *Service) Cancel(ctx context.Context, id string) (Notification, error) {
	return s.transition(ctx, id, notification.StatusPending, notification.StatusCancelled, s.q.CancelNotification, ErrNotCancellable, "notification cancelled")
}

// Retry replays a failed notification: pending again, fresh attempt budget,
// due now; its attempt history is kept.
func (s *Service) Retry(ctx context.Context, id string) (Notification, error) {
	return s.transition(ctx, id, notification.StatusFailed, notification.StatusPending, s.q.ReplayNotification, ErrNotRetryable, "notification replayed")
}

func (s *Service) transition(ctx context.Context, id string, from, to notification.Status,
	update func(context.Context, string) (sql.Result, error), refused error, msg string,
) (Notification, error) {
	if err := notification.Transition(from, to); err != nil {
		return Notification{}, err
	}
	res, err := update(ctx, id)
	if err != nil {
		return Notification{}, fmt.Errorf("%s %s to %s: %w", id, from, to, err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return Notification{}, fmt.Errorf("%s %s to %s: rows affected: %w", id, from, to, err)
	}
	if changed == 0 {
		if _, err := s.get(ctx, id); err != nil {
			return Notification{}, err
		}
		return Notification{}, refused
	}
	n, err := s.get(ctx, id)
	if err != nil {
		return Notification{}, err
	}
	// Lines about a notification carry its stored correlation id; the
	// request's own id stays on the access line and is added here.
	lctx := telemetry.WithNotification(ctx, id, n.CorrelationID)
	slog.InfoContext(lctx, msg, "from", from, "to", to, "request_correlation_id", telemetry.CorrelationID(ctx))
	// The statement set status_seq = LAST_INSERT_ID(status_seq + 1): the
	// sequence of this change, whatever happens to the row after it. The
	// change is committed either way; without it the read's value is the
	// best left (exact unless a later change already landed).
	seq := n.StatusSeq
	if v, err := res.LastInsertId(); err == nil && v > 0 {
		seq = uint64(v) //nolint:gosec // v > 0
	} else {
		slog.WarnContext(lctx, "no status_seq from the update; using the read", "err", err, "status_seq", seq)
	}
	// The read may already show a later change (a worker claims a replayed
	// row at once); the event describes this change. A cancel is final and a
	// replay resets attempts, so neither depends on that read.
	e := notification.Event{
		NotificationID: n.ID, BatchID: n.BatchID.String, Status: to, Attempts: int(n.Attempts),
		Error: n.LastError.String, At: n.UpdatedAt, CorrelationID: n.CorrelationID, Seq: seq,
	}
	if to == notification.StatusPending { // a replay: fresh budget, claimable again
		e.Attempts = 0
	}
	s.publishEvent(lctx, e)
	if to == notification.StatusPending {
		s.wake(lctx, hintFor(n.Notification))
	}
	return n, nil
}

// BatchStatusSeqs returns the status_seq of every notification of a batch,
// keyed by id: what a batch subscription already has once it is read.
func (s *Service) BatchStatusSeqs(ctx context.Context, id string) (map[string]uint64, error) {
	rows, err := s.q.ListBatchStatusSeqs(ctx, sql.NullString{String: id, Valid: true})
	if err != nil {
		return nil, fmt.Errorf("batch %s status sequences: %w", id, err)
	}
	out := make(map[string]uint64, len(rows))
	for _, r := range rows {
		out[r.ID] = r.StatusSeq
	}
	return out, nil
}

// GetBatch returns a batch's counts per API status and one page of its items
// in request order.
func (s *Service) GetBatch(ctx context.Context, id string, limit int, cursor string) (Batch, error) {
	after := -1
	if cursor != "" {
		i, err := DecodeBatchCursor(cursor)
		if err != nil {
			return Batch{}, err
		}
		after = i
	}
	b, err := s.q.GetBatch(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Batch{}, ErrNotFound
	}
	if err != nil {
		return Batch{}, fmt.Errorf("read batch %s: %w", id, err)
	}
	out := Batch{Batch: b, Counts: map[string]int{}}
	bid := sql.NullString{String: id, Valid: true}
	counts, err := s.q.CountBatchByStatus(ctx, bid)
	if err != nil {
		return Batch{}, fmt.Errorf("count batch %s: %w", id, err)
	}
	for _, c := range counts {
		out.Counts[asString(c.ApiStatus)] = int(c.N)
	}
	n := pageSize(limit)
	rows, err := s.q.ListBatchItems(ctx, sqlc.ListBatchItemsParams{
		BatchID: bid, AfterIndex: sql.NullInt32{Int32: int32(after), Valid: true}, Limit: int32(n + 1), //nolint:gosec // bounded
	})
	if err != nil {
		return Batch{}, fmt.Errorf("list batch %s: %w", id, err)
	}
	out.Items = make([]Notification, 0, min(len(rows), n))
	for i, r := range rows {
		if i == n {
			out.NextCursor = EncodeBatchCursor(int(out.Items[n-1].BatchIndex.Int32))
			break
		}
		out.Items = append(out.Items, Notification{Notification: r.Notification, Scheduled: r.IsScheduled != 0})
	}
	return out, nil
}

func (s *Service) inTx(ctx context.Context, fn func(q *sqlc.Queries, tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(s.q.WithTx(tx), tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// loadTemplates reads the bodies of the templates the requests name; an
// unknown name is simply absent from the map.
func (s *Service) loadTemplates(ctx context.Context, rs []Request) (map[string]string, error) {
	out := map[string]string{}
	tried := map[string]bool{}
	for _, r := range rs {
		if r.Template == nil || tried[r.Template.Name] {
			continue
		}
		tried[r.Template.Name] = true
		t, err := s.q.GetTemplate(ctx, r.Template.Name)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read template %q: %w", r.Template.Name, err)
		}
		out[t.Name] = t.Body
	}
	return out, nil
}

// publishCreated announces a creation: pending, sequence 1. It is built from
// the stored row but not from its current state, which a worker may already
// have moved on (that change is announced by the worker).
func (s *Service) publishCreated(ctx context.Context, n sqlc.Notification) {
	s.publishEvent(ctx, notification.Event{
		NotificationID: n.ID, BatchID: n.BatchID.String, Status: notification.StatusPending,
		At: n.CreatedAt, CorrelationID: n.CorrelationID, Seq: 1,
	})
}

// publishEvent is best effort: an error is logged, never returned.
func (s *Service) publishEvent(ctx context.Context, e notification.Event) {
	if err := s.pub.Publish(ctx, e); err != nil {
		slog.WarnContext(ctx, "publish status change", "status", e.Status, "err", err)
	}
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

// idTime is the creation time a UUIDv7 carries, to the millisecond; it is
// stored as created_at, so id order and created_at order agree.
func idTime(id uuid.UUID) time.Time {
	sec, nsec := id.Time().UnixTime()
	return time.Unix(sec, nsec).UTC().Truncate(time.Millisecond)
}

func nullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: t.UTC(), Valid: true}
}

func pageSize(limit int) int {
	switch {
	case limit <= 0:
		return notification.DefaultPageSize
	case limit > notification.MaxPageSize:
		return notification.MaxPageSize
	}
	return limit
}

func asString(v any) string {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case string:
		return x
	}
	return fmt.Sprint(v)
}
