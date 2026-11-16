package fakeprovider_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// clock is a settable time source; tests move it instead of sleeping.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

func (c *clock) set(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC).Add(d)
}

func start(t *testing.T, opts ...fakeprovider.Option) (*fakeprovider.Server, string, *clock) {
	t.Helper()
	c := &clock{}
	c.set(0)
	s := fakeprovider.New(append([]fakeprovider.Option{fakeprovider.WithClock(c.now)}, opts...)...)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts.URL, c
}

type sent struct {
	status int
	header http.Header
	body   map[string]any
	err    error
}

func send(t *testing.T, client *http.Client, method, url, body string, headers ...string) sent {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return sent{err: err}
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	out := sent{status: res.StatusCode, header: res.Header}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out.body); err != nil {
			t.Fatalf("response is not a JSON object: %s", raw)
		}
	}
	return out
}

const sms = `{"to":"+905551234567","channel":"sms","content":"Your message"}`

func msg(channel string) string {
	return fmt.Sprintf(`{"to":"+905551234567","channel":%q,"content":"Your message"}`, channel)
}

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if err := json.NewDecoder(res.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func post(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(sms))
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	return res.Body.Close()
}

// waitFor polls cond for up to 2 s: a bounded wait on an observable outcome,
// not a fixed sleep before an assertion.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
