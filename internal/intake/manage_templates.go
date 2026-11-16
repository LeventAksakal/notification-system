package intake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"

	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

// Template is a stored template with the variable names its body uses.
type Template struct {
	sqlc.Template
	Variables []string
}

func newTemplate(t sqlc.Template) Template {
	return Template{Template: t, Variables: Variables(t.Body)}
}

// Why a template write was refused (decision 0015, optimistic concurrency).
// The API maps them to 428 and 412.
var (
	ErrVersionRequired = errors.New("the template exists: replacing it needs If-Match with its current version")
	ErrVersionStale    = errors.New("If-Match is not the template's current version")
)

// VersionError is a template write refused by its version check. Err is
// ErrVersionRequired or ErrVersionStale; Current is the template's version at
// the time of the refusal, 0 when it does not exist.
type VersionError struct {
	Err     error
	Current uint32
}

func (e *VersionError) Error() string {
	if e.Current == 0 {
		return e.Err.Error() + " (the template does not exist)"
	}
	return fmt.Sprintf("%v (current version %d)", e.Err, e.Current)
}

func (e *VersionError) Unwrap() error { return e.Err }

// errNoRowMatched is an UpdateTemplate that changed nothing: the version is
// stale or the template is gone.
var errNoRowMatched = errors.New("no template row matched")

// PutTemplate creates a template when ifMatch is nil, or replaces it when
// ifMatch is its current version. created reports a new template. Every
// write is one statement whose condition the database checks (a plain INSERT,
// or UPDATE ... WHERE version = ?), never read-then-write; the triggers on
// templates make the stored version 1 on insert and the old one plus 1 on
// every update. Notifications already accepted keep their rendered content.
func (s *Service) PutTemplate(ctx context.Context, name, body string, ifMatch *uint64) (t Template, created bool, err error) {
	if errs := CheckTemplate(name, body); len(errs) > 0 {
		return Template{}, false, &ValidationError{Details: errs}
	}
	if ifMatch == nil {
		t, err = s.createTemplate(ctx, name, body)
		return t, err == nil, err
	}
	t, err = s.replaceTemplate(ctx, name, body, *ifMatch)
	return t, false, err
}

// createAttempts bounds the inserts of one create: a second try only when
// the template was deleted between the duplicate key and the read of its
// version (a 428 must name the current version, and there is none left). Two
// such losses in a row are answered 428 without a version.
const createAttempts = 2

func (s *Service) createTemplate(ctx context.Context, name, body string) (Template, error) {
	var row sqlc.Template
	var err error
	for attempt := 1; ; attempt++ {
		err = s.inTx(ctx, func(q *sqlc.Queries, _ *sql.Tx) error {
			if err := q.InsertTemplate(ctx, sqlc.InsertTemplateParams{Name: name, Body: body}); err != nil {
				return err
			}
			var err error
			row, err = q.GetTemplate(ctx, name)
			return err
		})
		if !store.IsDuplicateKey(err) {
			break
		}
		verr := s.versionError(ctx, name, ErrVersionRequired)
		var ve *VersionError
		if attempt < createAttempts && errors.As(verr, &ve) && ve.Current == 0 {
			continue
		}
		return Template{}, verr
	}
	if err != nil {
		return Template{}, fmt.Errorf("create template %q: %w", name, err)
	}
	slog.InfoContext(ctx, "template created", "template", name, "version", row.Version)
	return newTemplate(row), nil
}

func (s *Service) replaceTemplate(ctx context.Context, name, body string, ifMatch uint64) (Template, error) {
	// A version past the column's range can never be current.
	if ifMatch > math.MaxUint32 {
		return Template{}, s.versionError(ctx, name, ErrVersionStale)
	}
	var row sqlc.Template
	// The read shares the update's transaction, so the answer is the row this
	// update wrote (its X lock holds until commit), not a later writer's.
	err := s.inTx(ctx, func(q *sqlc.Queries, _ *sql.Tx) error {
		n, err := q.UpdateTemplate(ctx, sqlc.UpdateTemplateParams{Name: name, Body: body, Version: uint32(ifMatch)}) //nolint:gosec // G115: at most MaxUint32, checked above
		if err != nil {
			return err
		}
		if n == 0 {
			return errNoRowMatched
		}
		row, err = q.GetTemplate(ctx, name)
		return err
	})
	if errors.Is(err, errNoRowMatched) {
		return Template{}, s.versionError(ctx, name, ErrVersionStale)
	}
	if err != nil {
		return Template{}, fmt.Errorf("replace template %q: %w", name, err)
	}
	slog.InfoContext(ctx, "template replaced", "template", name, "version", row.Version)
	return newTemplate(row), nil
}

// versionError reads the template's current version for a refusal. The read
// only informs the caller; it decides nothing.
func (s *Service) versionError(ctx context.Context, name string, why error) error {
	t, err := s.q.GetTemplate(ctx, name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return &VersionError{Err: why}
	case err != nil:
		return fmt.Errorf("read template %q after a refused write: %w", name, err)
	}
	return &VersionError{Err: why, Current: t.Version}
}

// GetTemplate reads one template.
func (s *Service) GetTemplate(ctx context.Context, name string) (Template, error) {
	t, err := s.q.GetTemplate(ctx, name)
	if errors.Is(err, sql.ErrNoRows) {
		return Template{}, ErrNotFound
	}
	if err != nil {
		return Template{}, fmt.Errorf("read template %q: %w", name, err)
	}
	return newTemplate(t), nil
}

// ListTemplates returns every template by name.
func (s *Service) ListTemplates(ctx context.Context) ([]Template, error) {
	ts, err := s.q.ListTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	out := make([]Template, len(ts))
	for i, t := range ts {
		out[i] = newTemplate(t)
	}
	return out, nil
}

// DeleteTemplate removes a template.
func (s *Service) DeleteTemplate(ctx context.Context, name string) error {
	n, err := s.q.DeleteTemplate(ctx, name)
	if err != nil {
		return fmt.Errorf("delete template %q: %w", name, err)
	}
	if n == 0 {
		return ErrNotFound
	}
	slog.InfoContext(ctx, "template deleted", "template", name)
	return nil
}
