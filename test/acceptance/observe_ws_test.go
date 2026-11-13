//go:build integration

package acceptance

import (
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"
)

const wsWait = 60 * time.Second

// wantLifecycle checks that id's events read pending, processing, sent in that
// order, with non-decreasing `at` and the StatusEvent required fields.
func wantLifecycle(t *testing.T, evs []statusEvent, n notification) {
	t.Helper()
	mine := eventsFor(evs, n.ID)
	if got, want := statuses(mine), []string{"pending", "processing", "sent"}; !slices.Equal(got, want) {
		t.Fatalf("events for %s: want statuses %v in order, got %v", n.ID, want, mine)
	}
	requireEventFields(t, mine, n.CorrelationID)
	for i := 1; i < len(mine); i++ {
		if mine[i].At != nil && mine[i-1].At != nil && mine[i].At.Before(*mine[i-1].At) {
			t.Errorf("event `at` goes backwards: %v then %v", mine[i-1].At, mine[i].At)
		}
	}
	if last := mine[len(mine)-1]; last.Attempts == nil || *last.Attempts != 1 {
		t.Errorf("sent event attempts: want 1, got %v", last)
	}
}

// B4: Bonus: WebSocket Updates - "A subscriber sees each status change in order
// without polling" (decision 0013): unfiltered subscription.
func TestB4WSStreamsStatusChanges(t *testing.T) {
	s := startStack(t, stackOptions{Workers: 1})
	c := s.subscribe(t)
	n := s.create(t, msg("sms", "b4 stream"))
	evs := collectUntil(t, c, wsWait, reached(n.ID, "sent"), "sent of "+n.ID)
	wantLifecycle(t, evs, n)
}

// B4: Bonus: WebSocket Updates - a late subscriber gets the final status as the
// snapshot (decision 0013, Confirmation).
func TestB4WSSnapshotAfterSent(t *testing.T) {
	s := startStack(t, stackOptions{Workers: 1})
	n := s.create(t, msg("email", "b4 late"))
	s.waitStatus(t, n.ID, "sent", wsWait)

	c := s.subscribe(t, "notificationId", n.ID)
	e, err := readEvent(c, time.Now().Add(10*time.Second))
	if err != nil {
		t.Fatalf("first message of a notificationId subscription: %v", err)
	}
	if !e.Snapshot || e.Status != "sent" || e.NotificationID != n.ID {
		t.Errorf("first message: want snapshot of %s with status sent, got %v", n.ID, e)
	}
	requireEventFields(t, []statusEvent{e}, n.CorrelationID)
}

// B4: Bonus: WebSocket Updates - subscribe by id before delivery: snapshot
// first, then each change in order.
func TestB4WSSnapshotThenChanges(t *testing.T) {
	s := startStack(t, stackOptions{})
	n := s.create(t, msg("push", "b4 early"))
	c := s.subscribe(t, "notificationId", n.ID)

	first, err := readEvent(c, time.Now().Add(10*time.Second))
	if err != nil {
		t.Fatalf("first message of a notificationId subscription: %v", err)
	}
	if !first.Snapshot || first.Status != "pending" || first.NotificationID != n.ID {
		t.Fatalf("first message: want snapshot of %s with status pending, got %v", n.ID, first)
	}

	s.StartWorker()
	evs := collectUntil(t, c, wsWait, reached(n.ID, "sent"), "sent of "+n.ID)
	for _, e := range evs {
		if e.NotificationID != n.ID {
			t.Errorf("subscription to %s delivered an event of another notification: %v", n.ID, e)
		}
		if e.Snapshot {
			t.Errorf("only the first message may be a snapshot: %v", e)
		}
	}
	if got := statuses(eventsFor(evs, n.ID)); !slices.Equal(got, []string{"processing", "sent"}) {
		t.Errorf("after the snapshot: want [processing sent], got %v", evs)
	}
	requireEventFields(t, evs, n.CorrelationID)
}

// B4: Bonus: WebSocket Updates - a batchId subscription carries only that
// batch's notifications.
func TestB4WSBatchFilter(t *testing.T) {
	s := startStack(t, stackOptions{})
	b := s.createBatch(t, []map[string]any{msg("sms", "b4 batch 1"), msg("email", "b4 batch 2"), msg("push", "b4 batch 3")})
	c := s.subscribe(t, "batchId", b.ID)
	other := s.create(t, msg("sms", "b4 not in the batch"))
	s.StartWorker()

	allSent := func(evs []statusEvent) bool {
		for _, id := range b.NotificationIDs {
			if !reached(id, "sent")(evs) {
				return false
			}
		}
		return true
	}
	evs := collectUntil(t, c, wsWait, allSent, "sent of every batch notification")
	s.waitStatus(t, other.ID, "sent", wsWait)
	// Drain briefly: an event of the unrelated notification would show here.
	for {
		e, err := readEvent(c, time.Now().Add(time.Second))
		if errors.Is(err, errWSTimeout) {
			break
		}
		if err != nil {
			t.Fatalf("draining: %v", err)
		}
		evs = append(evs, e)
	}
	for _, e := range evs {
		if !slices.Contains(b.NotificationIDs, e.NotificationID) {
			t.Errorf("batch subscription delivered an event outside the batch: %v", e)
		}
		if e.BatchID != b.ID {
			t.Errorf("event batchId: want %s, got %v", b.ID, e)
		}
	}
}

// B4: Bonus: WebSocket Updates - cancelling is a status change too.
func TestB4WSCancelIsAnEvent(t *testing.T) {
	s := startStack(t, stackOptions{})
	c := s.subscribe(t)
	n := s.create(t, msg("email", "b4 cancel me"))
	s.cancel(t, n.ID)
	evs := collectUntil(t, c, 30*time.Second, reached(n.ID, "cancelled"), "cancelled of "+n.ID)
	if got := statuses(eventsFor(evs, n.ID)); !slices.Equal(got, []string{"pending", "cancelled"}) {
		t.Errorf("events for %s: want [pending cancelled], got %v", n.ID, got)
	}
	requireEventFields(t, eventsFor(evs, n.ID), n.CorrelationID)
}

// B4: Bonus: WebSocket Updates - subscribing to an unknown notification is
// refused at the handshake (api/openapi.yaml /v1/ws 404).
func TestB4WSUnknownNotification404(t *testing.T) {
	s := startStack(t, stackOptions{})
	// Baseline: a known id upgrades, so a 404 below is not just a missing route.
	n := s.create(t, msg("sms", "b4 known"))
	s.subscribe(t, "notificationId", n.ID)

	c, status, err := s.dialWS("notificationId", newUUID())
	if err == nil {
		_ = c.CloseNow()
		t.Fatalf("unknown notificationId: want the handshake refused with 404, it upgraded")
	}
	if status != http.StatusNotFound {
		t.Errorf("unknown notificationId: want HTTP 404, got %d (%v)", status, err)
	}
}

// B4: Bonus: WebSocket Updates - events cross API instances (decision 0013:
// fan-out through Valkey): subscribe on a second instance, create on the first.
func TestB4WSAcrossInstances(t *testing.T) {
	s := startStack(t, stackOptions{Workers: 1})
	peer := s.startServe(t, nil)
	c := peer.subscribe(t)
	n := s.create(t, msg("push", "b4 across"))
	evs := collectUntil(t, c, wsWait, reached(n.ID, "sent"), "sent of "+n.ID+" on the second instance")
	wantLifecycle(t, evs, n)
}
