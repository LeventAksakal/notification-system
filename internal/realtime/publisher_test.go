package realtime

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"go.uber.org/goleak"

	"github.com/LeventAksakal/notification-system/internal/notification"
)

// How the fake server answers PUBLISH.
const (
	replyOK      = "ok"       // at once
	replyErrOnce = "err-once" // one error reply, then ok
	replyHang    = "hang"     // not until the test calls unhang
	replyGated   = "gated"    // one reply per token sent on gate
)

// fakeValkey speaks just enough RESP2 for a valkey-go client to publish.
type fakeValkey struct {
	addr string
	ln   net.Listener
	wg   sync.WaitGroup

	mu    sync.Mutex
	mode  string
	conns []net.Conn

	hung     chan struct{} // closed by unhang
	hungOnce sync.Once
	stop     chan struct{}
	gate     chan struct{}
	seen     chan struct{} // one per PUBLISH received in gated mode
}

func newFakeValkey(t *testing.T) *fakeValkey {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeValkey{addr: ln.Addr().String(), ln: ln, mode: replyOK,
		hung: make(chan struct{}), stop: make(chan struct{}), gate: make(chan struct{}), seen: make(chan struct{}, 16)}
	f.wg.Add(1)
	go func() { // owned by close through wg
		defer f.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns = append(f.conns, c)
			f.mu.Unlock()
			f.wg.Add(1)
			go func() { // owned by close through wg
				defer f.wg.Done()
				f.serve(c)
			}()
		}
	}()
	t.Cleanup(f.close)
	return f
}

func (f *fakeValkey) setMode(m string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = m
}

func (f *fakeValkey) unhang() { f.hungOnce.Do(func() { close(f.hung) }) }

func (f *fakeValkey) close() {
	close(f.stop)
	_ = f.ln.Close()
	f.mu.Lock()
	for _, c := range f.conns {
		_ = c.Close()
	}
	f.mu.Unlock()
	f.wg.Wait()
}

func (f *fakeValkey) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		reply := "+OK\r\n"
		switch strings.ToUpper(args[0]) {
		case "PING":
			reply = "+PONG\r\n"
		case "PUBLISH":
			reply = ":1\r\n"
			f.mu.Lock()
			mode := f.mode
			if mode == replyErrOnce {
				f.mode = replyOK
			}
			f.mu.Unlock()
			switch mode {
			case replyErrOnce:
				reply = "-ERR injected\r\n"
			case replyHang:
				select {
				case <-f.hung:
				case <-f.stop:
					return
				}
			case replyGated:
				f.seen <- struct{}{}
				select {
				case <-f.gate:
				case <-f.stop:
					return
				}
			}
		}
		if _, err := io.WriteString(c, reply); err != nil {
			return
		}
	}
}

func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("not an array: %q", line)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || n < 1 {
		return nil, fmt.Errorf("array length %q", line)
	}
	args := make([]string, n)
	for i := range args {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(hdr, "$")))
		if err != nil {
			return nil, fmt.Errorf("bulk length %q", hdr)
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:size])
	}
	return args, nil
}

// newTestPublisher publishes to f with one connection and no client retries.
func newTestPublisher(t *testing.T, f *fakeValkey) *Publisher {
	t.Helper()
	c, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{f.addr}, DisableCache: true,
		AlwaysRESP2: true, ForceSingleClient: true, DisableRetry: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return NewPublisher(c)
}

func event(i int) notification.Event {
	return notification.Event{NotificationID: fmt.Sprintf("n%d", i), Status: notification.StatusProcessing, At: time.Now()}
}

func TestPublisherErrorReplyDoesNotMarkValkeyDown(t *testing.T) {
	noLeaks(t)
	f := newFakeValkey(t)
	p := newTestPublisher(t, f)
	f.setMode(replyErrOnce)
	err := p.Publish(t.Context(), event(1))
	if err == nil || errors.Is(err, ErrUnavailable) {
		t.Fatalf("first publish: want the injected error reply, got %v", err)
	}
	if err := p.Publish(t.Context(), event(2)); err != nil {
		t.Fatalf("next publish after an error reply: want it sent, got %v", err)
	}
}

func TestPublisherTimeoutFailsOthersAtOnceUntilAProbeSucceeds(t *testing.T) {
	noLeaks(t)
	f := newFakeValkey(t)
	p := newTestPublisher(t, f)
	f.setMode(replyHang)
	if err := p.Publish(t.Context(), event(1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("publish to a hung server: want a timeout, got %v", err)
	}

	// Valkey is taken as down. One caller probes; the server holds its reply
	// until the test lets it go.
	f.setMode(replyGated)
	f.unhang()
	probe := make(chan error, 1)
	go func() { probe <- p.Publish(context.Background(), event(2)) }()
	select {
	case <-f.seen:
	case <-time.After(5 * time.Second):
		t.Fatal("the probe publish never reached the server")
	}
	for i := range 3 {
		start := time.Now()
		err := p.Publish(t.Context(), event(10+i))
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("publish while down, with a probe in flight: want ErrUnavailable, got %v", err)
		}
		if d := time.Since(start); d > 50*time.Millisecond {
			t.Errorf("publish while down took %s, want it to fail at once", d)
		}
	}

	f.setMode(replyOK)
	f.gate <- struct{}{}
	if err := <-probe; err != nil {
		t.Fatalf("probe publish: %v", err)
	}
	if err := p.Publish(t.Context(), event(3)); err != nil {
		t.Fatalf("publish after the probe succeeded: want it sent, got %v", err)
	}
}

func TestPublisherLargeBatchToAHungServerReturnsWithinOneDeadline(t *testing.T) {
	noLeaks(t)
	f := newFakeValkey(t)
	p := newTestPublisher(t, f)
	f.setMode(replyHang)
	es := make([]notification.Event, 1000)
	for i := range es {
		es[i] = event(i)
	}
	start := time.Now()
	err := p.PublishAll(t.Context(), es)
	took := time.Since(start)
	if err == nil {
		t.Fatal("publish to a hung server: want an error")
	}
	if took > 1200*time.Millisecond {
		t.Errorf("1000 events to a hung server took %s, want one %s deadline", took, publishTimeout)
	}
}

// noLeaks checks, after every cleanup of the test, that it left no goroutine.
func noLeaks(t *testing.T) {
	t.Helper()
	before := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, before) })
}

func TestPublisherProbeAnsweredWithAnErrorFreesTheNextProbe(t *testing.T) {
	noLeaks(t)
	f := newFakeValkey(t)
	p := newTestPublisher(t, f)
	f.setMode(replyHang)
	if err := p.Publish(t.Context(), event(1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("publish to a hung server: want a timeout, got %v", err)
	}
	// Valkey answers again, first with an error reply (LOADING after a
	// restart): the probe fails, but it proves nothing about being down.
	f.setMode(replyErrOnce)
	f.unhang()
	if err := p.Publish(t.Context(), event(2)); err == nil || errors.Is(err, ErrUnavailable) {
		t.Fatalf("probe answered with an error reply: want that error, got %v", err)
	}
	if err := p.Publish(t.Context(), event(3)); err != nil {
		t.Fatalf("next publish after an error-reply probe: want it tried and sent, got %v", err)
	}
	if err := p.Publish(t.Context(), event(4)); err != nil {
		t.Fatalf("publish after Valkey is up again: %v", err)
	}
}
