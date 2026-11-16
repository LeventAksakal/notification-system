package notification

import (
	"encoding/base64"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"
)

func TestChannelLimitsWithinTheSpecBounds(t *testing.T) {
	for _, c := range Channels {
		if l := ContentLimit(c); l.Max <= 0 || l.Max > MaxContentLen || l.Unit == "" {
			t.Errorf("ContentLimit(%q) = %+v, want a unit and Max within (0, %d]", c, l, MaxContentLen)
		}
		if l := RecipientLimit(c); l.Max <= 0 || l.Max > MaxRecipientLen || l.Unit == "" {
			t.Errorf("RecipientLimit(%q) = %+v, want a unit and Max within (0, %d]", c, l, MaxRecipientLen)
		}
	}
	if ContentLimit("fax") != (Limit{}) || RecipientLimit("fax") != (Limit{}) {
		t.Error("unknown channel must have no limit")
	}
	// MaxContentLen is the largest channel limit, not merely a bound on them.
	largest := 0
	for _, c := range Channels {
		largest = max(largest, ContentLimit(c).Max)
	}
	if largest != MaxContentLen {
		t.Errorf("MaxContentLen = %d, largest channel content limit = %d", MaxContentLen, largest)
	}
}

func TestUnitLen(t *testing.T) {
	s := "é\U0001f600a" // 2 + 4 + 1 bytes, 3 code points
	if n := Bytes.Len(s); n != 7 {
		t.Errorf("Bytes.Len = %d, want 7", n)
	}
	if n := Characters.Len(s); n != 3 {
		t.Errorf("Characters.Len = %d, want 3", n)
	}
}

// The pattern the spec carries and the validator's IsBlank agree on every
// code point: a one-character string is blank exactly when it is whitespace.
func TestNotBlankPatternAgreesWithIsBlank(t *testing.T) {
	re := regexp.MustCompile(NotBlankPattern)
	spaces := 0
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if !utf8.ValidRune(r) {
			continue
		}
		s := string(r)
		if re.MatchString(s) == IsBlank(s) {
			t.Errorf("U+%04X: pattern matches %v, IsBlank %v", r, re.MatchString(s), IsBlank(s))
		}
		if unicode.Is(unicode.White_Space, r) {
			spaces++
		}
	}
	if spaces != 25 {
		t.Errorf("Unicode White_Space has %d code points here, the doc comment says 25", spaces)
	}
	if re.MatchString("") || !IsBlank("") {
		t.Error("the empty string must be blank")
	}
	if !re.MatchString(" \u3000x\t") || IsBlank(" \u3000x\t") {
		t.Error("one visible character among whitespace is not blank")
	}
}

// RFC 5321 §4.5.3.1.7 guarantees 64K octets of message content; the email
// body limit, base64-encoded in 76-character lines (RFC 2045 §6.8), leaves room
// for headers, and twice it does not fit.
func TestEmailContentLimitFitsTheSMTPGuarantee(t *testing.T) {
	encoded := func(n int) int {
		chars := base64.StdEncoding.EncodedLen(n)
		lines := (chars + 75) / 76
		return chars + 2*lines
	}
	const guarantee = 64 << 10
	if got := encoded(MaxEmailContentBytes); got != 44_842 || got >= guarantee {
		t.Errorf("encoded email limit = %d octets, doc says 44,842 (< %d)", got, guarantee)
	}
	if got := encoded(2 * MaxEmailContentBytes); got != 89_684 {
		t.Errorf("encoded twice the email limit = %d octets, doc says 89,684", got)
	}
}

func TestPushContentLimitLeavesTheEnvelope(t *testing.T) {
	var env struct {
		APS struct {
			Alert struct {
				Body string `json:"body"`
			} `json:"alert"`
		} `json:"aps"`
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != apnsAlertEnvelope {
		t.Fatalf("envelope %s, constant %s", b, apnsAlertEnvelope)
	}
	env.APS.Alert.Body = strings.Repeat("é", MaxPushContentBytes/2) + "a" // 4067 bytes
	if b, _ = json.Marshal(env); len(b) != 4096 {
		t.Errorf("payload at the push limit is %d bytes, want 4096", len(b))
	}
}

// MaxRequestBodyBytes is the smallest power of two that holds a full batch of
// SMS at the content limit in worst-case UTF-8.
func TestRequestBodyHoldsAFullBatchOfLongestSMS(t *testing.T) {
	type item struct {
		To          string    `json:"to"`
		Channel     Channel   `json:"channel"`
		Content     string    `json:"content"`
		Priority    Priority  `json:"priority"`
		ScheduledAt time.Time `json:"scheduledAt"`
	}
	it := item{To: "+123456789012345", Channel: ChannelSMS, Content: strings.Repeat("\U0001f600", MaxSMSContentLen),
		Priority: PriorityNormal, ScheduledAt: time.Date(2026, 11, 16, 12, 0, 0, 123e6, time.UTC)}
	items := make([]item, MaxBatchSize)
	for i := range items {
		items[i] = it
	}
	b, err := json.Marshal(map[string]any{"notifications": items})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(b); n > MaxRequestBodyBytes || n <= MaxRequestBodyBytes/2 {
		t.Errorf("full SMS batch is %d bytes; want within (%d, %d]", n, MaxRequestBodyBytes/2, MaxRequestBodyBytes)
	}
	if MaxRequestBodyBytes&(MaxRequestBodyBytes-1) != 0 {
		t.Error("MaxRequestBodyBytes is not a power of two")
	}
}

func TestDerivedLimits(t *testing.T) {
	cases := []struct {
		name      string
		got, want int
	}{
		{"push content", MaxPushContentBytes, 4067},
		{"correlation id is a traceparent", MaxCorrelationIDLen, len("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
		{"template variables", MaxTemplateVariables, 6553},
		{"cursor", MaxCursorLen, base64.RawURLEncoding.EncodedLen(len("n1:") + 36)},
		{"page ceiling", MaxPageSize, min(MaxPageSize, MaxRequestBodyBytes/MaxEmailContentBytes-1)},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: %d, want %d", c.name, c.got, c.want)
		}
	}
}
