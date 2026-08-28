package panewire_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type herdrFixture struct {
	path     string
	listener net.Listener
	mu       sync.Mutex
	handlers map[string]func() any
	conns    []net.Conn
	requests []map[string]any
}

func newHerdrFixture(t *testing.T, schema string) *herdrFixture {
	t.Helper()
	base, err := os.MkdirTemp("/tmp", "pw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	path := filepath.Join(base, "h.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	f := &herdrFixture{path: path, listener: l, handlers: map[string]func() any{}}
	f.On("api.schema", func() any { var v any; _ = json.Unmarshal([]byte(schema), &v); return v })
	go f.serve(t)
	return f
}
func (f *herdrFixture) Path() string { return f.path }
func (f *herdrFixture) On(method string, handler func() any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers[method] = handler
}
func (f *herdrFixture) Requests(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, req := range f.requests {
		if req["method"] == method {
			n++
		}
	}
	return n
}
func (f *herdrFixture) serve(t *testing.T) {
	for {
		c, err := f.listener.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.conns = append(f.conns, c)
		f.mu.Unlock()
		go f.connection(t, c)
	}
}
func (f *herdrFixture) connection(t *testing.T, c net.Conn) {
	defer c.Close()
	scan := bufio.NewScanner(c)
	for scan.Scan() {
		var req map[string]any
		if json.Unmarshal(scan.Bytes(), &req) != nil {
			continue
		}
		method, _ := req["method"].(string)
		f.mu.Lock()
		f.requests = append(f.requests, req)
		h := f.handlers[method]
		f.mu.Unlock()
		if h == nil {
			h = func() any { return map[string]any{"type": "ok"} }
		}
		response := map[string]any{"id": req["id"], "result": h()}
		b, _ := json.Marshal(response)
		_, _ = fmt.Fprintf(c, "%s\n", b)
	}
}
func (f *herdrFixture) Event(event any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		b, _ := json.Marshal(event)
		_, _ = fmt.Fprintf(c, "%s\n", b)
	}
}
func (f *herdrFixture) Close() {
	_ = f.listener.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		_ = c.Close()
	}
	_ = os.Remove(f.path)
}
