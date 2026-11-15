package httpapi

import (
	"time"
	"uuid"

	"github.com/danielgtaylor/huma/v2"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// The types in this file are the HTTP contract (decision 0024): Huma derives
// api/openapi.yaml from them, and the request validator from the same
// schemas. Limits are read from internal/notification (decision 0017), never
// written here as numbers; the TransformSchema methods put them in.

// component registers s under name in the spec's components (once) and
// returns a $ref to it, so an enum or a shared string shape is one named
// schema that the request validator and generated clients share.
func component(r huma.Registry, name string, s *huma.Schema) *huma.Schema {
	if _, ok := r.Map()[name]; !ok {
		s.PrecomputeMessages()
		r.Map()[name] = s
	}
	return &huma.Schema{Ref: "#/components/schemas/" + name}
}

func enum[T ~string](vs ...T) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = string(v)
	}
	return out
}

func ptr[T any](v T) *T { return &v }

// Channel is a delivery channel.
type Channel string

func (Channel) Schema(r huma.Registry) *huma.Schema {
	return component(r, "Channel", &huma.Schema{Type: huma.TypeString, Enum: enum(notification.Channels...)})
}

// Priority orders claiming within a channel (decision 0010).
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

func (Priority) Schema(r huma.Registry) *huma.Schema {
	return component(r, "Priority", &huma.Schema{Type: huma.TypeString, Enum: enum(PriorityHigh, PriorityNormal, PriorityLow)})
}

// Status is the API view of a notification's state.
type Status string

const (
	StatusScheduled  Status = "scheduled"
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

func (Status) Schema(r huma.Registry) *huma.Schema {
	return component(r, "Status", &huma.Schema{
		Type: huma.TypeString,
		Description: "Stored states are pending, processing, sent, failed and cancelled. `scheduled` is shown\n" +
			"for a pending notification whose due time is in the future and that was never tried.\n" +
			"`sent` means the provider accepted it; the provider never confirms delivery.\n",
		Enum: enum(StatusScheduled, StatusPending, StatusProcessing, StatusSent, StatusFailed, StatusCancelled),
	})
}

// FailureKind says why a notification ended failed.
type FailureKind string

func (FailureKind) Schema(r huma.Registry) *huma.Schema {
	return component(r, "FailureKind", &huma.Schema{
		Type:        huma.TypeString,
		Description: "`permanent`: the provider refused it. `exhausted`: every attempt failed.",
		Enum:        []any{"permanent", "exhausted"},
	})
}

// TemplateName names a template, in a path and in a body.
type TemplateName string

func (TemplateName) Schema(r huma.Registry) *huma.Schema {
	return component(r, "TemplateName", &huma.Schema{
		Type:      huma.TypeString,
		MinLength: ptr(1),
		MaxLength: ptr(notification.MaxTemplateNameLen),
		Pattern:   notification.TemplateNamePattern,
	})
}

// TemplateRef asks intake to render a stored template (decision 0015).
type TemplateRef struct {
	Name      TemplateName      `json:"name"`
	Variables map[string]string `json:"variables,omitempty"`
}

func (TemplateRef) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	v := s.Properties["variables"]
	v.MaxProperties = ptr(notification.MaxTemplateVariables)
	if vs, ok := v.AdditionalProperties.(*huma.Schema); ok {
		vs.MaxLength = ptr(notification.MaxTemplateVariableLen)
		vs.PrecomputeMessages() // the parent's pass does not reach additionalProperties
	}
	return s
}

// NotificationCreate is one notification to queue.
type NotificationCreate struct {
	To          string       `json:"to" minLength:"1"`
	Channel     Channel      `json:"channel"`
	Content     string       `json:"content,omitempty" minLength:"1"`
	Template    *TemplateRef `json:"template,omitempty"`
	Priority    Priority     `json:"priority,omitempty"`
	ScheduledAt *time.Time   `json:"scheduledAt,omitempty" doc:"Deliver no earlier than this. A time in the past means now; more than 30 days ahead is refused."`
}

func (NotificationCreate) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Description = "Exactly one of `content` and `template` is required."
	s.Properties["to"].MaxLength = ptr(notification.MaxRecipientLen)
	s.Properties["content"].MaxLength = ptr(notification.MaxContentLen)
	return s
}

// BatchCreate is up to MaxBatchSize notifications stored atomically.
type BatchCreate struct {
	Notifications []NotificationCreate `json:"notifications" minItems:"1"`
}

func (BatchCreate) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Properties["notifications"].MaxItems = ptr(notification.MaxBatchSize)
	return s
}

// TemplateWrite is the body of PUT /v1/templates/{name}.
type TemplateWrite struct {
	Body string `json:"body" minLength:"1"`
}

func (TemplateWrite) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Properties["body"].MaxLength = ptr(notification.MaxTemplateBodyLen)
	return s
}

// Notification is one stored notification as the API shows it.
type Notification struct {
	ID                uuid.UUID         `json:"id"`
	BatchID           *uuid.UUID        `json:"batchId,omitempty"`
	To                string            `json:"to"`
	Channel           Channel           `json:"channel"`
	Content           string            `json:"content"`
	Priority          Priority          `json:"priority"`
	Status            Status            `json:"status"`
	FailureKind       *FailureKind      `json:"failureKind,omitempty"`
	Attempts          int               `json:"attempts" doc:"Provider calls made in the current attempt budget"`
	LastError         *string           `json:"lastError,omitempty"`
	ProviderMessageID *string           `json:"providerMessageId,omitempty"`
	ScheduledAt       *time.Time        `json:"scheduledAt,omitempty"`
	DueAt             time.Time         `json:"dueAt" doc:"Earliest time a worker may pick it up (the schedule, or the next retry)"`
	CorrelationID     string            `json:"correlationId"`
	CreatedAt         time.Time         `json:"createdAt"`
	UpdatedAt         time.Time         `json:"updatedAt"`
	FinishedAt        *time.Time        `json:"finishedAt,omitempty" doc:"When it reached sent, failed or cancelled"`
	DeliveryAttempts  []DeliveryAttempt `json:"deliveryAttempts,omitzero" doc:"Only on GET by id; oldest first"`
}

// DeliveryAttempt is one provider call.
type DeliveryAttempt struct {
	AttemptNo         int       `json:"attemptNo"`
	StartedAt         time.Time `json:"startedAt"`
	DurationMs        int       `json:"durationMs"`
	Outcome           string    "json:\"outcome\" enum:\"accepted,transient,permanent\" doc:\"`accepted`: 2xx. `transient`: will be retried (network error, timeout, 408, 425, 429, 5xx). `permanent`: any other 4xx.\""
	HTTPStatus        *int      `json:"httpStatus,omitempty" doc:"Absent when no response arrived"`
	Error             *string   `json:"error,omitempty"`
	ProviderMessageID *string   `json:"providerMessageId,omitempty"`
}

// NotificationPage is one page of a keyset-paginated list.
type NotificationPage struct {
	Items      []Notification `json:"items"`
	NextCursor string         `json:"nextCursor,omitempty" doc:"Absent on the last page"`
}

// StatusCounts counts notifications per API status.
type StatusCounts struct {
	Scheduled  int `json:"scheduled"`
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Sent       int `json:"sent"`
	Failed     int `json:"failed"`
	Cancelled  int `json:"cancelled"`
}

// BatchCreated answers a stored batch.
type BatchCreated struct {
	ID              uuid.UUID   `json:"id"`
	Total           int         `json:"total"`
	NotificationIDs []uuid.UUID `json:"notificationIds" doc:"In the order of the request"`
	CreatedAt       time.Time   `json:"createdAt"`
}

// BatchStatus is a batch's counts and one page of its items.
type BatchStatus struct {
	ID         uuid.UUID      `json:"id"`
	Total      int            `json:"total"`
	Counts     StatusCounts   `json:"counts"`
	Items      []Notification `json:"items" doc:"One page of the batch's notifications, in the order of the request"`
	NextCursor string         `json:"nextCursor,omitempty"`
	CreatedAt  time.Time      `json:"createdAt"`
}

// Template is a stored template.
type Template struct {
	Name      TemplateName `json:"name"`
	Body      string       `json:"body"`
	Variables []string     `json:"variables" doc:"Variable names found in the body"`
	CreatedAt time.Time    `json:"createdAt"`
	UpdatedAt time.Time    `json:"updatedAt"`
}

// TemplateList is every template, by name.
type TemplateList struct {
	Items []Template `json:"items"`
}

// LatencyMs holds delivery latency percentiles.
type LatencyMs struct {
	P50 int `json:"p50"`
	P95 int `json:"p95"`
	P99 int `json:"p99"`
}

func (LatencyMs) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Description = "From the first due time (creation, or the schedule if later) to provider acceptance, " +
		"retries included, over notifications sent in the window; zeros when none. A replayed dead letter " +
		"counts from its original due time, so replays of old failures raise the upper percentiles"
	return s
}

// QueueDepth is one channel's queue.
type QueueDepth struct {
	Channel            Channel `json:"channel"`
	Pending            int     `json:"pending" doc:"Waiting and due now"`
	Scheduled          int     `json:"scheduled" doc:"Waiting for a due time in the future (scheduled, or backing off before a retry)"`
	Processing         int     `json:"processing"`
	OldestPendingAgeMs int     `json:"oldestPendingAgeMs" doc:"How long the oldest due notification has waited; 0 when none"`
}

// DeliveryStats is one channel's delivery numbers in the window.
type DeliveryStats struct {
	Channel        Channel   `json:"channel"`
	Sent           int       "json:\"sent\" doc:\"Notifications that reached `sent` in the window\""
	Failed         int       "json:\"failed\" doc:\"Notifications that reached `failed` in the window\""
	SuccessRate    float64   `json:"successRate" doc:"sent / (sent + failed); 0 when both are 0"`
	FailureRate    float64   `json:"failureRate" doc:"failed / (sent + failed); 0 when both are 0"`
	Attempts       int       `json:"attempts" doc:"Provider calls started in the window"`
	FailedAttempts int       `json:"failedAttempts" doc:"Provider calls in the window that were not accepted"`
	LatencyMs      LatencyMs `json:"latencyMs"`
}

// DeliveryTotals is every channel's delivery numbers in the window.
type DeliveryTotals struct {
	Sent           int       `json:"sent"`
	Failed         int       `json:"failed"`
	SuccessRate    float64   `json:"successRate"`
	FailureRate    float64   `json:"failureRate"`
	Attempts       int       `json:"attempts"`
	FailedAttempts int       `json:"failedAttempts"`
	LatencyMs      LatencyMs `json:"latencyMs"`
}

// Metrics is the summary of GET /v1/metrics (decision 0014).
type Metrics struct {
	GeneratedAt       time.Time       `json:"generatedAt"`
	WindowSeconds     int             `json:"windowSeconds"`
	QueueDepth        int             `json:"queueDepth" doc:"Every notification not yet finished - pending (due or not) plus processing"`
	Queue             []QueueDepth    `json:"queue" doc:"One entry per channel, always all three"`
	Totals            StatusCounts    `json:"totals"`
	Delivery          DeliveryTotals  `json:"delivery"`
	DeliveryByChannel []DeliveryStats `json:"deliveryByChannel" doc:"One entry per channel, always all three"`
}

// StatusEvent is one message on the WebSocket stream (GET /v1/ws). Huma does
// not route the stream; the type is registered in the spec by hand (api.go).
type StatusEvent struct {
	NotificationID uuid.UUID  `json:"notificationId"`
	BatchID        *uuid.UUID `json:"batchId,omitempty"`
	Status         Status     `json:"status"`
	Attempts       int        `json:"attempts"`
	Error          *string    `json:"error,omitempty"`
	At             time.Time  `json:"at"`
	CorrelationID  string     `json:"correlationId"`
	Snapshot       *bool      "json:\"snapshot,omitempty\" doc:\"True on the first message of a `notificationId` subscription\""
}

func (StatusEvent) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Description = "One message on the WebSocket stream"
	return s
}

// Health answers the liveness and readiness probes.
type Health struct {
	Status string            `json:"status" enum:"ok,unavailable"`
	Checks map[string]string `json:"checks"`
}

const (
	healthOK          = "ok"
	healthUnavailable = "unavailable"
)

// ErrorDetail names one broken rule. Field order is the wire order.
type ErrorDetail struct {
	Field  string `json:"field"`
	Index  *int   `json:"index,omitempty" doc:"Position of the item in a batch request"`
	Reason string `json:"reason"`
}

// Error is every refused request's body; errors.go makes Huma write it in
// place of RFC 9457 problem details. Field order is the wire order.
type Error struct {
	status  int
	headers map[string]string

	Code    string        `json:"code" doc:"invalid_request, not_found, not_cancellable, not_retryable, idempotency_key_reused, request_too_large, internal"`
	Details []ErrorDetail `json:"details,omitempty"`
	Message string        `json:"message"`
}
