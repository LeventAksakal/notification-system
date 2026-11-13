package intake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

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

// PutTemplate creates or replaces a template. Notifications already accepted
// keep their rendered content (decision 0015).
func (s *Service) PutTemplate(ctx context.Context, name, body string) (Template, error) {
	if errs := CheckTemplate(name, body); len(errs) > 0 {
		return Template{}, &ValidationError{Details: errs}
	}
	if err := s.q.UpsertTemplate(ctx, sqlc.UpsertTemplateParams{Name: name, Body: body}); err != nil {
		return Template{}, fmt.Errorf("store template %q: %w", name, err)
	}
	slog.InfoContext(ctx, "template stored", "template", name)
	return s.GetTemplate(ctx, name)
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
