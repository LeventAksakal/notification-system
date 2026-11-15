package httpapi

import (
	"context"
	"errors"

	"github.com/danielgtaylor/huma/v2"

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

type putTemplateInput struct {
	TemplateNameInput
	Body TemplateWrite
}

// Resolve: every operation with a body runs singleValues (see refuseNull).
func (*putTemplateInput) Resolve(ctx huma.Context) []error { return singleValues(ctx) }

type templateOutput struct{ Body Template }

func (s *Server) PutTemplate(ctx context.Context, in *putTemplateInput) (*templateOutput, error) {
	t, err := s.intake.PutTemplate(ctx, string(in.Name), in.Body.Body)
	if err := refusal(err); err != nil {
		return nil, err
	}
	return &templateOutput{toTemplate(t)}, nil
}

func (s *Server) GetTemplate(ctx context.Context, in *TemplateNameInput) (*templateOutput, error) {
	t, err := s.intake.GetTemplate(ctx, string(in.Name))
	if errors.Is(err, intake.ErrNotFound) {
		return nil, notFound("template")
	}
	if err != nil {
		return nil, err
	}
	return &templateOutput{toTemplate(t)}, nil
}

func (s *Server) DeleteTemplate(ctx context.Context, in *TemplateNameInput) (*struct{}, error) {
	err := s.intake.DeleteTemplate(ctx, string(in.Name))
	if errors.Is(err, intake.ErrNotFound) {
		return nil, notFound("template")
	}
	return nil, err
}

func toTemplate(t intake.Template) Template {
	return Template{Name: TemplateName(t.Name), Body: t.Body, Variables: t.Variables, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt}
}
