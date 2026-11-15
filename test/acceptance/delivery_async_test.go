//go:build integration

package acceptance

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
	"github.com/LeventAksakal/notification-system/test/apiclient"
)

// R7: Process notifications asynchronously via queue workers
// (POST returns before delivery; workers deliver; killing one worker loses nothing).
func TestDeliveryR7AsyncWorkers(t *testing.T) {
	t.Run("with no worker POST returns and the notification waits; a worker then sends it", func(t *testing.T) {
		s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(nil)})
		n := s.create(t, msg("sms", "R7 queued before any worker"))
		if n.Status != "pending" {
			t.Errorf("status in the 201: want pending, got %q", n.Status)
		}
		consistently(t, time.Second, func() bool {
			return s.Provider.Stats().Channels["sms"].Received == 0 && s.storedStatus(t, n.Id) == "pending"
		}, "with 0 workers the notification stays pending and nothing reaches the provider")

		s.StartWorker()
		dlEventually(t, 15*time.Second, "the notification is sent once a worker runs", func() (bool, string) {
			st := s.storedStatus(t, n.Id)
			return st == "sent", "stored status " + st
		})
	})

	// Dials: APP_LEASE_DURATION=2s and APP_REAP_INTERVAL=500ms, so the killed
	// worker's claims return to pending within ~2.5 s; a 200 ms delay fault
	// keeps sends in flight so the kill lands on claimed rows.
	t.Run("killing one of three workers mid-drain loses nothing", func(t *testing.T) {
		const total = 600
		s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(nil)})
		chans := []string{"sms", "email", "push"}
		ids := s.dlCreateMany(t, total, func(i int) apiclient.NotificationCreate {
			return msg(chans[i%3], fmt.Sprintf("R7 backlog %d", i))
		})
		s.dlFaults(t, fakeprovider.Fault{Mode: fakeprovider.ModeDelay, DelayMS: 200})
		for range 3 {
			s.StartWorker()
		}
		dlEventually(t, 20*time.Second, "the drain is under way (150 deliveries)", func() (bool, string) {
			st := s.Provider.Stats()
			d := st.Channels["sms"].Deliveries + st.Channels["email"].Deliveries + st.Channels["push"].Deliveries
			return d >= 150, fmt.Sprintf("%d deliveries", d)
		})
		s.KillWorker(2)
		t.Logf("killed worker-2 with stored counts %v", s.dlCounts(t, "1=1"))

		s.dlAllSent(t, 45*time.Second, total, "1=1")
		if p := s.count(t, "SELECT COUNT(*) FROM notifications WHERE status = 'processing'"); p != 0 {
			t.Errorf("%d notifications still processing", p)
		}
		delivered := s.dlDeliveredKeys()
		missing := 0
		for _, id := range ids {
			if !delivered[id.String()] {
				missing++
			}
		}
		if missing > 0 {
			t.Errorf("%d of %d notifications never delivered at the provider", missing, total)
		}
	})
}

// R16: Provider: POST { "to", "channel", "content" }, expected 202 { "messageId", "status", "timestamp" }
// (the provider double receives exactly that body; the returned messageId is stored).
func TestDeliveryR16ProviderCall(t *testing.T) {
	rec := dlNewRecorder(t)
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(map[string]string{
		"APP_PROVIDER_URL_SMS": rec.URL + "/send",
	})})
	rec.SetUpstream(s.ProviderURL)

	const content = "R16 exact body: Your code is 4821 ✓"
	n := s.create(t, apiclient.NotificationCreate{To: smsTo, Channel: apiclient.Sms, Content: ptr(content), Priority: ptr(apiclient.High)})
	s.StartWorker()
	dlEventually(t, 15*time.Second, "the notification is sent", func() (bool, string) {
		st := s.storedStatus(t, n.Id)
		return st == "sent", "stored status " + st
	})

	calls := rec.Calls()
	if len(calls) != 1 {
		t.Fatalf("provider calls: want 1, got %d", len(calls))
	}
	c := calls[0]
	body, keys := dlJSONKeys(t, c.Body)
	if fmt.Sprint(keys) != "[channel content to]" {
		t.Errorf("body keys: want exactly [channel content to], got %v (%s)", keys, c.Body)
	}
	want := map[string]string{"to": smsTo, "channel": "sms", "content": content}
	for k, v := range want {
		if body[k] != v {
			t.Errorf("body %q: want %q, got %v", k, v, body[k])
		}
	}
	if got := c.Header.Get("Idempotency-Key"); got != n.Id.String() {
		t.Errorf("Idempotency-Key: want the notification id %q, got %q", n.Id, got)
	}
	if c.Status != 202 {
		t.Fatalf("double answered %d: %s", c.Status, c.ResponseBody)
	}
	var answer struct {
		MessageID string `json:"messageId"`
	}
	if err := json.Unmarshal(c.ResponseBody, &answer); err != nil || answer.MessageID == "" {
		t.Fatalf("double's 202 has no messageId: %s (%v)", c.ResponseBody, err)
	}
	if rs := s.Provider.Requests(fakeprovider.Filter{Key: n.Id.String()}); len(rs) != 1 || !rs[0].Delivery {
		t.Errorf("double's record for the key: want 1 delivered request, got %+v", rs)
	}

	got := s.get(t, n.Id)
	if got.Status != "sent" {
		t.Errorf("GET status: want sent, got %q", got.Status)
	}
	if deref(got.ProviderMessageId) != answer.MessageID {
		t.Errorf("providerMessageId: want the double's messageId %q, got %q", answer.MessageID, deref(got.ProviderMessageId))
	}
	if got.Attempts != 1 {
		t.Errorf("attempts: want 1, got %d", got.Attempts)
	}
	if len(history(got)) != 1 {
		t.Fatalf("deliveryAttempts: want 1 entry, got %s", attemptsString(history(got)))
	}
	a := history(got)[0]
	if a.Outcome != "accepted" || a.HttpStatus == nil || *a.HttpStatus != 202 {
		t.Errorf("delivery attempt: want accepted with httpStatus 202, got %s", attemptString(a))
	}
}
