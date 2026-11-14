//go:build integration

package acceptance

// R8 under a late rate-limiter reply. The workers reach Valkey through
// jitterProxy, a TCP proxy that holds every chunk Valkey sends back for a
// random time, in byte order; `serve` uses Valkey directly.

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

// dlJitterMax is the largest delay jitterProxy adds to one chunk of Valkey's
// replies. Why 60 ms: on the code of 14 Nov (group of slots mapped to local
// time from the reply's arrival) the busiest second per channel (sms, email,
// push) measured 100/100/100 at 30 ms (passes, at the edge), 102/102/102 at
// 60 ms and 107/106/108 at 120 ms; 60 ms is the smallest of the three that
// breaks the cap, in line with roughly 97 + d/10.3 ms (those runs paced at
// 97/s, 3 % headroom; at today's 95/s the same reasoning gives 95 + d/10.5 ms).
const dlJitterMax = 60 * time.Millisecond

// jitterProxy forwards a local port to target. Data from the client to target
// passes at once; each chunk read from target is released to the client at
// max(previous release, arrival + d), d drawn uniformly from [0, max], so the
// byte order is kept and delays never pile up beyond the drawn value.
type jitterProxy struct {
	Addr   string
	target string
	max    time.Duration
	ln     net.Listener

	chunks  atomic.Int64
	delayed atomic.Int64 // sum of the actual hold times, in microseconds

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

func newJitterProxy(t *testing.T, target string, maxDelay time.Duration) *jitterProxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("jitter proxy listen: %v", err)
	}
	p := &jitterProxy{Addr: ln.Addr().String(), target: target, max: maxDelay, ln: ln, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(p.close)
	return p
}

func (p *jitterProxy) accept() {
	defer p.wg.Done()
	for {
		down, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			_ = down.Close()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = down.Close()
			_ = up.Close()
			return
		}
		p.conns[down], p.conns[up] = struct{}{}, struct{}{}
		p.mu.Unlock()

		held := make(chan heldChunk, 4096)
		p.wg.Add(3)
		go p.forward(up, down)
		go p.readDelayed(up, held)
		go p.writeDelayed(down, up, held)
	}
}

type heldChunk struct {
	b       []byte
	arrived time.Time
	release time.Time
}

// forward copies client -> target without delay.
func (p *jitterProxy) forward(dst, src net.Conn) {
	defer p.wg.Done()
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
}

// readDelayed reads target's replies and stamps each chunk with its release time.
func (p *jitterProxy) readDelayed(src net.Conn, out chan<- heldChunk) {
	defer p.wg.Done()
	defer close(out)
	var last time.Time
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			now := time.Now()
			d := time.Duration(rand.Int64N(int64(p.max) + 1)) //nolint:gosec // G404: test jitter, not security
			rel := now.Add(d)
			if rel.Before(last) {
				rel = last
			}
			last = rel
			out <- heldChunk{b: append([]byte(nil), buf[:n]...), arrived: now, release: rel}
		}
		if err != nil {
			return
		}
	}
}

// writeDelayed sends each chunk to the client at its release time.
func (p *jitterProxy) writeDelayed(dst, src net.Conn, in <-chan heldChunk) {
	defer p.wg.Done()
	defer func() {
		_ = dst.Close()
		_ = src.Close()
		p.mu.Lock()
		delete(p.conns, dst)
		delete(p.conns, src)
		p.mu.Unlock()
	}()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for c := range in {
		if d := time.Until(c.release); d > 0 {
			timer.Reset(d)
			<-timer.C
		}
		p.chunks.Add(1)
		p.delayed.Add(time.Since(c.arrived).Microseconds())
		if _, err := dst.Write(c.b); err != nil {
			_ = src.Close() // the reader stops; drain what it already queued
			for range in {
			}
			return
		}
	}
}

func (p *jitterProxy) close() {
	p.mu.Lock()
	p.closed = true
	_ = p.ln.Close()
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// R8: Implement rate limiting: maximum 100 messages per second per channel -
// when the limiter's replies arrive late. Finding 3 of the 14 Nov fault run:
// the worker maps a group of send slots to local time from the moment the
// limiter's reply arrives, so a reply delayed by d shifts the whole group up
// to d late, on top of the next group; under compose two channels showed 100
// in one second after a Valkey restart (a quiet run at the then 3 % headroom
// showed 98 to 99).
// Dials: dlFastEnv; 700 per channel queued before 3 workers start; the
// workers reach Valkey through jitterProxy (0..dlJitterMax per chunk from
// Valkey to the worker). Only the cap is asserted: with jitter the correct
// behaviour is to send fewer, so there is no throughput floor.
func TestGapDeliveryR8RateUnderLimiterReplyJitter(t *testing.T) {
	const perChannel = 700
	chans := []string{"sms", "email", "push"}
	s := startStack(t, stackOptions{Workers: 0, Env: dlFastEnv(nil)})
	s.dlCreateMany(t, perChannel*3, func(i int) map[string]any {
		return msg(chans[i%3], fmt.Sprintf("gap R8 jitter %d", i))
	})

	jp := newJitterProxy(t, shared(t).valkeyAddr, dlJitterMax)
	s.env = withEnv(s.env, map[string]string{"APP_VALKEY_ADDR": jp.Addr}) // workers only; serve already runs direct
	start := time.Now()
	for range 3 {
		s.StartWorker()
	}
	s.dlAllSent(t, 120*time.Second, perChannel*3, "1=1")
	took := time.Since(start)

	chunks := jp.chunks.Load()
	if chunks == 0 {
		t.Fatalf("no reply passed through the jitter proxy: the workers did not use it")
	}
	t.Logf("jitter 0..%s per chunk: %d chunks from Valkey, mean hold %.1f ms; all %d sent %s after the workers started",
		dlJitterMax, chunks, float64(jp.delayed.Load())/float64(chunks)/1000, perChannel*3, took.Round(time.Millisecond))

	st := s.Provider.Stats()
	for _, ch := range chans {
		cs := st.Channels[ch]
		rs := s.dlRequests(fakeprovider.Filter{Channel: ch})
		n, span, rate := dlRate(rs)
		t.Logf("%s: %d requests, busiest 1 s window %d, average %.2f/s over %s; per 1 s bucket: %v",
			ch, n, cs.MaxRequestsPerSecond, rate, span.Round(time.Millisecond), dlR8Histogram(rs))
		if cs.MaxRequestsPerSecond > 100 {
			t.Errorf("%s: %d requests in one second at the provider with limiter replies up to %s late, limit 100",
				ch, cs.MaxRequestsPerSecond, dlJitterMax)
		}
	}
}
