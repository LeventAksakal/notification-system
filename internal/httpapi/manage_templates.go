package httpapi

import (
	"context"
	"errors"

	"github.com/LeventAksakal/notification-system/internal/intake"
)

type templatesOutput struct{ Body TemplateList }

func (s *Server) ListTemplates(ctx context.Context, _ *struct{}) (*templatesOutput, error) {
	ts, err := s.intake.ListTemplates(ctx)
	if err != nil {
		return nil, err
	}
	out := &templatesOutput{TemplateList{Items: make([]Template, len(ts))}}
	for i, t := range ts {
		out.Body.Items[i] = toTemplate(t)
	}
	return out, nil
}

type TemplateNameInput struct {
	Name TemplateName `path:"name"`
}

// templateOutput is one template with its version as the ETag.
type templateOutput struct {
	ETag string `header:"ETag" doc:"The template's version as a strong ETag, such as \"3\"; send it back in If-Match"`
	Body Template
}

func (s *Server) GetTemplate(ctx context.Context, in *TemplateNameInput) (*templateOutput, error) {
	t, err := s.intake.GetTemplate(ctx, string(in.Name))
	if errors.Is(err, intake.ErrNotFound) {
		return nil, notFound("template")
	}
	if err != nil {
		return nil, err
	}
	return &templateOutput{ETag: etagOf(t.Version), Body: toTemplate(t)}, nil
}

func (s *Server) DeleteTemplate(ctx context.Context, in *TemplateNameInput) (*struct{}, error) {
	err := s.intake.DeleteTemplate(ctx, string(in.Name))
	if errors.Is(err, intake.ErrNotFound) {
		return nil, notFound("template")
	}
	return nil, err
}

func toTemplate(t intake.Template) Template {
	return Template{Name: TemplateName(t.Name), Body: t.Body, Version: int(t.Version), Variables: t.Variables, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt}
}
