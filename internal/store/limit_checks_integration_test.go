//go:build integration

package store_test

import (
	"fmt"
	"strings"
	"testing"
	"unicode"

	"github.com/google/uuid"

	"github.com/LeventAksakal/notification-system/internal/notification"
	"github.com/LeventAksakal/notification-system/test/support"
)

// atAndPast returns a string exactly at l, in l's unit, and one unit past it.
// Multi-byte characters make bytes and characters disagree, so a CHECK that
// counts the wrong unit fails one of the two rows.
func atAndPast(l notification.Limit) (at, past string) {
	if l.Unit == notification.Bytes {
		at = strings.Repeat("é", l.Max/2) + strings.Repeat("a", l.Max%2)
		return at, at + "a"
	}
	at = strings.Repeat("é", l.Max)
	return at, at + "é"
}

// whiteSpaceRunes lists Unicode's White_Space property, the set
// notification.IsBlank treats as blank.
func whiteSpaceRunes() []rune {
	var rs []rune
	for _, r16 := range unicode.White_Space.R16 {
		for r := rune(r16.Lo); r <= rune(r16.Hi); r += rune(r16.Stride) {
			rs = append(rs, r)
		}
	}
	for _, r32 := range unicode.White_Space.R32 {
		for r := rune(r32.Lo); r <= rune(r32.Hi); r += rune(r32.Stride) { //nolint:gosec // Unicode tables stop at U+10FFFF, inside rune's range
			rs = append(rs, r)
		}
	}
	return rs
}

// The database CHECKs refuse what the validator refuses, written straight
// into MySQL (decision 0017): each limit at its value, one unit past it, and
// blank values on every White_Space code point.
func TestDatabaseChecksAgreeWithTheLimits(t *testing.T) {
	db, _ := support.MySQL(t)
	recipient := map[notification.Channel]string{
		notification.ChannelSMS: "+905551234567", notification.ChannelEmail: "a@example.com", notification.ChannelPush: "device-1"}

	type row struct {
		name             string
		channel, to, txt string
		accept           bool
	}
	var rows []row
	for _, ch := range notification.Channels {
		l := notification.ContentLimit(ch)
		at, past := atAndPast(l)
		c, to := string(ch), recipient[ch]
		rows = append(rows,
			row{fmt.Sprintf("%s content at %d %s", c, l.Max, l.Unit), c, to, at, true},
			row{fmt.Sprintf("%s content one %s past", c, strings.TrimSuffix(string(l.Unit), "s")), c, to, past, false},
			row{c + " content empty", c, to, "", false},
			row{c + " content whitespace only", c, to, " \t\n\u00a0\u3000", false},
		)
	}
	for _, r := range whiteSpaceRunes() {
		if !notification.IsBlank(string(r)) {
			t.Errorf("IsBlank(%U) = false for a White_Space code point", r)
		}
		rows = append(rows,
			row{fmt.Sprintf("content of %U alone", r), "push", "device-1", string(r), false},
			row{fmt.Sprintf("recipient of %U alone", r), "push", string(r), "x", false},
		)
	}
	// Look like whitespace but are not White_Space: accepted by both.
	for _, r := range []rune{'\u200b', '\ufeff', '\u180e'} {
		if notification.IsBlank(string(r)) {
			t.Errorf("IsBlank(%U) = true outside White_Space", r)
		}
		rows = append(rows, row{fmt.Sprintf("content of %U alone", r), "push", "device-1", string(r), true})
	}
	rows = append(rows,
		row{"email recipient at 254 bytes", "email", strings.Repeat("a", 249) + "@b.cd", "x", true},
		row{"email recipient at 255 bytes", "email", strings.Repeat("a", 250) + "@b.cd", "x", false},
		row{"email recipient of 254 characters, 503 bytes", "email", strings.Repeat("é", 249) + "@b.cd", "x", false},
		row{"push recipient at 255 characters", "push", strings.Repeat("é", notification.MaxPushRecipientLen), "x", true},
		row{"push recipient empty", "push", "", "x", false},
		row{"sms recipient of 15 digits", "sms", "+123456789012345", "x", true},
		row{"sms recipient of a country code and one digit", "sms", "+12", "x", true},
		row{"sms recipient of 16 digits", "sms", "+1234567890123456", "x", false},
		row{"sms recipient with a country code starting 0", "sms", "+0123456", "x", false},
		row{"sms recipient without +", "sms", "1234567", "x", false},
	)
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			_, err := db.ExecContext(t.Context(),
				`INSERT INTO notifications (id, recipient, channel, content, due_at, correlation_id)
				 VALUES (?, ?, ?, ?, NOW(3), 'limit-checks')`, uuid.Must(uuid.NewV7()).String(), r.to, r.channel, r.txt)
			if r.accept && err != nil {
				t.Errorf("refused a row the validator accepts: %v", err)
			}
			if !r.accept && err == nil {
				t.Error("accepted a row the validator refuses")
			}
		})
	}

	bodyAt, bodyPast := atAndPast(notification.Limit{Max: notification.MaxTemplateBodyBytes, Unit: notification.Bytes})
	for _, tc := range []struct {
		name, tname, body string
		accept            bool
	}{
		{"template name at 63 and body at its byte limit", strings.Repeat("n", notification.MaxTemplateNameLen), bodyAt, true},
		{"template name starting with a digit", "1st", "x", true},
		{"template name of 64", strings.Repeat("m", notification.MaxTemplateNameLen+1), "x", false},
		{"template name with an underscore", "welcome_1", "x", false},
		{"template name with a capital", "Welcome", "x", false},
		{"template name ending with a hyphen", "welcome-", "x", false},
		{"template body one byte past its limit", "too-long-body", bodyPast, false},
		{"template body whitespace only", "blank-body", " \n\u3000", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(t.Context(), `INSERT INTO templates (name, body) VALUES (?, ?)`, tc.tname, tc.body)
			if tc.accept && err != nil {
				t.Errorf("refused a template the validator accepts: %v", err)
			}
			if !tc.accept && err == nil {
				t.Error("accepted a template the validator refuses")
			}
		})
	}
}
