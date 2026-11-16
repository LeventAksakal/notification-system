package httpapi

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"uuid"

	"github.com/coder/websocket"
	"github.com/danielgtaylor/huma/v2"

	"github.com/LeventAksakal/notification-system/internal/intake"
	"github.com/LeventAksakal/notification-system/internal/notification"
)

// The WebSocket stream's contract (GET /v1/ws), in one place: the message
// type, how a status change becomes one, the refusal while there is no event
// feed, and the close codes that end a stream. The spec (api.go,
// documentStream) and the AsyncAPI document are generated from these.

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
	Seq            uint64     "json:\"seq,omitempty\" doc:\"The notification's status sequence: 1 at creation, one more with every status change. A stream sends a notification's events in this order without repeats; a gap means a change was lost, read the notification. Absent when unknown\""
	Snapshot       *bool      "json:\"snapshot,omitempty\" doc:\"True on the first message of a `notificationId` subscription\""
}

func (StatusEvent) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Description = "One message on the WebSocket stream"
	return s
}

// toStatusEvent maps a status change to the contract's StatusEvent. An id
// that is not a UUID fails; the stream skips that event rather than sending
// a zero id.
func toStatusEvent(e notification.Event) (StatusEvent, error) {
	id, err := uuid.Parse(e.NotificationID)
	if err != nil {
		return StatusEvent{}, fmt.Errorf("notification id %q: %w", e.NotificationID, err)
	}
	v := StatusEvent{NotificationID: id, Status: Status(e.Status), Attempts: e.Attempts,
		At: e.At.UTC(), CorrelationID: e.CorrelationID, Seq: e.Seq}
	if e.BatchID != "" {
		bid, err := uuid.Parse(e.BatchID)
		if err != nil {
			return StatusEvent{}, fmt.Errorf("batch id %q: %w", e.BatchID, err)
		}
		v.BatchID = &bid
	}
	if e.Error != "" {
		v.Error = &e.Error
	}
	return v, nil
}

// snapshotEvent is the stored state as an event (stored status; the API view
// is applied when it is written).
func snapshotEvent(n intake.Notification) notification.Event {
	return notification.Event{
		NotificationID: n.ID, BatchID: n.BatchID.String, Status: notification.Status(n.Notification.Status),
		Attempts: int(n.Attempts), Error: n.LastError.String, At: n.UpdatedAt, CorrelationID: n.CorrelationID,
		Seq: n.StatusSeq,
	}
}

// liveUpdatesUnavailable describes /v1/ws's 503 (LiveUpdatesUnavailable in
// the spec). It is not the API-wide ServiceUnavailable: the database is
// usually fine, so creating, reading and cancelling still work.
const liveUpdatesUnavailable = "Live updates are unavailable (`unavailable`): this instance has lost its event feed, " +
	"or is shutting down. The REST API still answers; reconnect shortly. When the database is what cannot be reached, " +
	"the body says so and `Retry-After` is set"

// liveUpdatesUnavailable503 is the LiveUpdatesUnavailable response object.
// Retry-After is listed but optional: only the database case sets it.
func liveUpdatesUnavailable503() *huma.Response {
	r := errorBody(liveUpdatesUnavailable)
	r.Headers = map[string]*huma.Param{"Retry-After": {
		Description: "Seconds to wait before trying again; set only when the database cannot be reached",
		Schema:      &huma.Schema{Type: huma.TypeInteger},
	}}
	return r
}

// errLiveUpdatesUnavailable is the 503 body while this instance has no event
// feed (Valkey down).
func errLiveUpdatesUnavailable() *Error {
	return newError(http.StatusServiceUnavailable, "unavailable",
		"live updates are unavailable: the event feed is down; retry shortly, the REST API still answers")
}

// streamClose is one way the server ends a stream: the close code, the
// reason sent in the close frame, and when it happens (for the contract).
type streamClose struct {
	code   websocket.StatusCode
	reason string
	when   string
}

// The close codes are RFC 6455 section 7.4.1's (1001, 1008) and the IANA
// registry's 1012 "service restart".
var (
	closeTooSlow = streamClose{websocket.StatusPolicyViolation, "too slow: events were dropped; resubscribe",
		"the subscriber fell behind and events were dropped (the send buffer, APP_WS_SEND_BUFFER, filled); resubscribe"}
	closeFeedLost = streamClose{websocket.StatusServiceRestart, "event feed lost; reconnect and read the snapshot",
		"the server lost its event feed, so changes may be missing; reconnect (503 until the feed is back) and read the snapshot"}
	closeShutdown = streamClose{websocket.StatusGoingAway, "server shutting down",
		"the server is shutting down; reconnect"}
)

// streamCloses lists every close the server sends, in the contract's order.
var streamCloses = []streamClose{closeTooSlow, closeFeedLost, closeShutdown}

// closeCodesDoc is the close codes as a Markdown list for the spec.
func closeCodesDoc() string {
	var b strings.Builder
	b.WriteString("The server ends a stream with one of these close codes:\n")
	for _, c := range streamCloses {
		b.WriteString("- `" + strconv.Itoa(int(c.code)) + "`: " + c.when + ".\n")
	}
	return b.String()
}
