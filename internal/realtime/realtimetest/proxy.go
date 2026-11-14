// Package realtimetest holds test helpers for code that talks to Valkey: a
// TCP proxy a test can close to make Valkey look down, and open again.
package realtimetest

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// Proxy forwards TCP connections from Addr to a target. Close drops every
// connection and stops listening; Reopen listens on Addr again.
type Proxy struct {
	Addr   string
	target string

	mu    sync.Mutex
	ln    net.Listener
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// NewProxy listens on a free local port and forwards to target until the
// test ends.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &Proxy{Addr: ln.Addr().String(), target: target, conns: map[net.Conn]struct{}{}}
	p.start(ln)
	t.Cleanup(p.Close)
	return p
}

func (p *Proxy) start(ln net.Listener) {
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	p.wg.Add(1)
	go func() { // owned by Close through wg
		defer p.wg.Done()
		p.serve(ln)
	}()
}

func (p *Proxy) serve(ln net.Listener) {
	for {
		down, err := ln.Accept()
		if err != nil {
			return
		}
		up, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			_ = down.Close()
			continue
		}
		p.mu.Lock()
		if p.ln != ln {
			p.mu.Unlock()
			_ = down.Close()
			_ = up.Close()
			return
		}
		p.conns[down], p.conns[up] = struct{}{}, struct{}{}
		p.wg.Add(2)
		p.mu.Unlock()
		pipe := func(dst, src net.Conn) {
			defer p.wg.Done()
			_, _ = io.Copy(dst, src)
			_ = dst.Close()
			_ = src.Close()
		}
		go pipe(up, down)
		go pipe(down, up)
	}
}

// Close stops listening and drops every open connection; safe to repeat.
func (p *Proxy) Close() {
	p.mu.Lock()
	if p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
	p.mu.Unlock()
	p.wg.Wait()
}

// Reopen listens on Addr again after Close, retrying the bind for up to 10 s.
func (p *Proxy) Reopen(t testing.TB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", p.Addr)
		if err == nil {
			p.start(ln)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("proxy reopen %s: %v", p.Addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
