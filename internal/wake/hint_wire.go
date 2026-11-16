// Package wake carries hints that work became pending from the processes that
// commit it (the API, and workers releasing or rescheduling) to the idle
// delivery workers, over Valkey pub/sub. MySQL stays the queue (decision
// 0003): a hint says only "channel C, priority P, due at T"; the worker still
// claims the row from the database. Hints are best effort. A lost one delays
// a send by at most SafetyPoll and never loses it, because an idle worker
// still asks the database that often.
package wake

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// SafetyPoll is the longest a missed hint may delay a send: an idle worker
// asks the database for the channel's next due time at least this often.
// Hints go missing only while a subscription reconnects, the listener claims
// at once on every (re)subscribe, and the rate limiter needs the same Valkey,
// so nothing is sent during that window anyway. One second keeps the cost of
// that guarantee at one indexed query per channel per second per worker.
// It is not a dial: it bounds a fault case and offers an operator no
// trade-off worth tuning.
const SafetyPoll = time.Second

// Prefix is the start of every wake channel's name; the channel's name
// follows it ("notifier:wake:sms").
const Prefix = "notifier:wake:"

// channelName is the pub/sub channel hints for ch travel on. One per delivery
// channel, so a worker serving some channels hears only theirs.
func channelName(ch notification.Channel) string { return Prefix + string(ch) }

type wireHint struct {
	Priority notification.Priority `json:"priority"`
	Due      time.Time             `json:"due"`
}

func encode(h notification.Wake) ([]byte, error) {
	return json.Marshal(wireHint{Priority: h.Priority, Due: h.Due})
}

func decode(ch notification.Channel, b []byte) (notification.Wake, error) {
	var w wireHint
	if err := json.Unmarshal(b, &w); err != nil {
		return notification.Wake{}, fmt.Errorf("decode wake hint: %w", err)
	}
	if !w.Priority.Valid() {
		return notification.Wake{}, fmt.Errorf("decode wake hint: priority %q", w.Priority)
	}
	return notification.Wake{Channel: ch, Priority: w.Priority, Due: w.Due}, nil
}
