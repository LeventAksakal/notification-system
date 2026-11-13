package api

import (
	"context"
	"errors"

	"github.com/LeventAksakal/notification-system/internal/intake"
)

func (s *Server) ListTemplates(ctx context.Context, _ ListTemplatesRequestObject) (ListTemplatesResponseObject, error) {
	ts, err := s.intake.ListTemplates(ctx)
	if err != nil {
		return nil, err
	}
	out := ListTemplates200JSONResponse{Items: make([]Template, len(ts))}
	for i, t := range ts {
		out.Items[i] = toTemplate(t)
	}
	return out, nil
}

func (s *Server) PutTemplate(ctx context.Context, req PutTemplateRequestObject) (PutTemplateResponseObject, error) {
	if req.Body == nil {
		return PutTemplate400JSONResponse{InvalidRequestJSONResponse(missingBody)}, nil
	}
	t, err := s.intake.PutTemplate(ctx, req.Name, req.Body.Body)
	if body, ok := invalidBody(err); ok {
		return PutTemplate400JSONResponse{InvalidRequestJSONResponse(body)}, nil
	}
	if err != nil {
		return nil, err
	}
	return PutTemplate200JSONResponse(toTemplate(t)), nil
}

func (s *Server) GetTemplate(ctx context.Context, req GetTemplateRequestObject) (GetTemplateResponseObject, error) {
	t, err := s.intake.GetTemplate(ctx, req.Name)
	if errors.Is(err, intake.ErrNotFound) {
		return GetTemplate404JSONResponse{NotFoundJSONResponse(notFound("template"))}, nil
	}
	if err != nil {
		return nil, err
	}
	return GetTemplate200JSONResponse(toTemplate(t)), nil
}

func (s *Server) DeleteTemplate(ctx context.Context, req DeleteTemplateRequestObject) (DeleteTemplateResponseObject, error) {
	err := s.intake.DeleteTemplate(ctx, req.Name)
	if errors.Is(err, intake.ErrNotFound) {
		return DeleteTemplate404JSONResponse{NotFoundJSONResponse(notFound("template"))}, nil
	}
	if err != nil {
		return nil, err
	}
	return DeleteTemplate204Response{}, nil
}

func toTemplate(t intake.Template) Template {
	return Template{Name: t.Name, Body: t.Body, Variables: t.Variables, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt}
}
