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
	NotificationID uuid.UUID  "json:\"notificationId\" doc:\"The notification this message is about: its `id` in the REST API (GET /v1/notifications/{id}). Lower case, with hyphens\""
	BatchID        *uuid.UUID "json:\"batchId,omitempty\" doc:\"The batch the notification was created in; absent for a notification created on its own\""
	Status         Status     "json:\"status\" doc:\"The notification's status after this change. A snapshot shows it as GET /v1/notifications/{id} does, so it may be `scheduled`; a change shows the stored status, never `scheduled` (a notification created with a future due time arrives as `pending`)\""
	Attempts       int        "json:\"attempts\" doc:\"Delivery attempts that have ended so far (the provider answered, the call failed, or the worker's lease ran out); 0 before the first one ends and again after a replay\""
	Error          *string    "json:\"error,omitempty\" doc:\"The last delivery error recorded for the notification, as text for a person; absent when none was recorded. Branch on `status`, not on this text\""
	At             time.Time  "json:\"at\" doc:\"When the change was made, by the server's clock, in UTC (RFC 3339). On a snapshot, when the notification last changed\""
	CorrelationID  string     "json:\"correlationId\" doc:\"The `X-Correlation-ID` stored with the notification when it was created; every log line and trace about it carries the same id\""
	Seq            uint64     "json:\"seq,omitempty\" doc:\"The notification's status sequence: 1 at creation, one more with every status change. A stream sends a notification's events in this order without repeats; a gap means a change was lost, read the notification. Absent when unknown\""
	Snapshot       *bool      "json:\"snapshot,omitempty\" doc:\"True on the first message of a `notificationId` subscription\""
}

func (StatusEvent) TransformSchema(_ huma.Registry, s *huma.Schema) *huma.Schema {
	s.Description = "One message on the WebSocket stream"
	return s
}

// The query parameters streamEvents reads (ws.go), each with what a client
// author needs to know. Both are optional; the value is read with
// url.Values.Get, so of a repeated parameter only the first counts, and an
// empty value is the same as none. Other parameters are ignored.
const (
	queryNotificationID = "notificationId"
	queryBatchID        = "batchId"
)

// uuidForms is how streamEvents parses an id (parseUUID): uuid.Parse plus a
// `urn:uuid:` prefix in any case.
const uuidForms = "A UUID: `f81d4fae-7dec-11d0-a765-00a0c91e6bf6`; upper case, a `urn:uuid:` prefix (any case), " +
	"braces and the 32 hex digits without hyphens are accepted too. Anything else is refused with `400 invalid_request` " +
	"naming this parameter in `details[].field`"

var streamQuery = []struct{ name, doc string }{
	{queryNotificationID, "Subscribe to one notification. The first message is its snapshot (`snapshot: true`, its current " +
		"status as GET /v1/notifications/{id} reads it), then its changes follow in `seq` order. A notification that does not " +
		"exist (or has expired) is refused with `404 not_found`. " + uuidForms},
	{queryBatchID, "Subscribe to the notifications of one batch: their changes follow, each notification's in `seq` order; " +
		"there is no snapshot, read GET /v1/batches/{id} for the current state. A batch that does not exist is refused with " +
		"`404 not_found`. With `notificationId` as well, that notification's snapshot comes first and its changes follow only " +
		"if it is in this batch (if not, none ever do). With neither parameter, every notification's changes follow, with no " +
		"snapshot. " + uuidForms},
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
// reason sent in the close frame (empty when it varies), when it happens and
// the standard that defines the code (for the contract).
type streamClose struct {
	code     websocket.StatusCode
	reason   string
	when     string
	standard string
}

// The standards close codes come from: RFC 6455 defines 1000 to 1011 in
// section 7.4.1; the codes registered since (1012 "Service Restart" among
// them) are in the IANA registry RFC 6455 section 11.7 set up.
const (
	rfc6455CloseCodes = "RFC 6455 section 7.4.1, https://www.rfc-editor.org/rfc/rfc6455#section-7.4.1"
	ianaCloseCodes    = "IANA WebSocket Close Code Number Registry (RFC 6455 section 11.7), " +
		"https://www.iana.org/assignments/websocket/websocket.xhtml#close-code-number"
)

// The closes ws.go sends.
var (
	closeTooSlow = streamClose{websocket.StatusPolicyViolation, "too slow: events were dropped; resubscribe",
		"the subscriber fell behind and events were dropped (the send buffer, APP_WS_SEND_BUFFER, filled); resubscribe",
		rfc6455CloseCodes}
	closeFeedLost = streamClose{websocket.StatusServiceRestart, "event feed lost; reconnect and read the snapshot",
		"the server lost its event feed, so changes may be missing; reconnect (503 until the feed is back) and read the snapshot",
		ianaCloseCodes}
	closeShutdown = streamClose{websocket.StatusGoingAway, "server shutting down",
		"the server is shutting down; reconnect", rfc6455CloseCodes}
)

// The closes github.com/coder/websocket sends for the server while it reads
// the client's frames (Conn.CloseRead in pump): the reason text of the first
// is the library's (read.go, CloseRead), so a test pins it to the library.
var (
	closeUnexpectedData = streamClose{websocket.StatusPolicyViolation, "unexpected data message",
		"the client sent a data message; clients send none on this stream", rfc6455CloseCodes}
	closeProtocolError = streamClose{websocket.StatusProtocolError, "",
		"the client broke the WebSocket framing (an unmasked or fragmented control frame, reserved bits or an unknown opcode); " +
			"the reason is the library's description of the fault", rfc6455CloseCodes}
)

// streamCloses lists every close the server sends, in the contract's order.
var streamCloses = []streamClose{closeTooSlow, closeFeedLost, closeShutdown, closeUnexpectedData, closeProtocolError}

// closeCodesDoc is the close codes as a Markdown list for the spec.
func closeCodesDoc() string {
	var b strings.Builder
	b.WriteString("The server ends a stream with one of these close codes:\n")
	for _, c := range streamCloses {
		b.WriteString("- `" + strconv.Itoa(int(c.code)) + "`: " + c.when + ".\n")
	}
	return b.String()
}
