package panewire

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// pipeListener makes a websocket with no socket buffering, so a peer that
// does not read deterministically blocks the server's first frame.
type pipeListener struct {
	mu     sync.Mutex
	conn   net.Conn
	closed chan struct{}
	once   sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	conn := l.conn
	l.conn = nil
	l.mu.Unlock()
	if conn != nil {
		return conn, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}

func hubWriterPipe(t *testing.T) (*hubAgent, *websocket.Conn) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	listener := &pipeListener{conn: serverSide, closed: make(chan struct{})}
	accepted := make(chan *websocket.Conn, 1)
	release := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		accepted <- conn
		<-release
		_ = conn.CloseNow()
	}))
	_ = server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) { return clientSide, nil }}
	peer, _, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.CloseNow() })
	return &hubAgent{conn: <-accepted}, peer
}

// The legacy writer is used on main, where caller cancellation cannot leave
// admission. The contextual writer exists on the changed branch.
func quotaTestWrite(ctx context.Context, agent *hubAgent) error {
	if contextual, ok := any(agent).(interface {
		writeJSONContext(context.Context, any) error
	}); ok {
		return contextual.writeJSONContext(ctx, map[string]string{"op": "quota.refresh"})
	}
	return agent.writeJSON(map[string]string{"op": "quota.refresh"})
}

func TestHubQuotaRefreshWriterAdmissionAndSocketBound(t *testing.T) {
	t.Run("writer admission obeys the request deadline", func(t *testing.T) {
		agent, peer := hubWriterPipe(t)
		agent.writeMu.Lock()
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- quotaTestWrite(ctx, agent) }()
		var result error
		finished := waitUntil(time.Second, func() bool {
			select {
			case result = <-done:
				return true
			default:
				return false
			}
		})
		if !finished {
			_ = peer.CloseNow()
		}
		agent.writeMu.Unlock()
		if !finished {
			<-done
			t.Fatal("held writer did not honor caller cancellation")
		}
		if result == nil {
			t.Fatal("held writer completed instead of honoring caller cancellation")
		}
	})
	t.Run("stalled socket obeys an earlier request deadline", func(t *testing.T) {
		agent, peer := hubWriterPipe(t)
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- quotaTestWrite(ctx, agent) }()
		var result error
		finished := waitUntil(time.Second, func() bool {
			select {
			case result = <-done:
				return true
			default:
				return false
			}
		})
		if !finished {
			_ = peer.CloseNow()
			<-done
			t.Fatal("stalled socket outlived the caller deadline")
		}
		if result == nil {
			t.Fatal("stalled socket succeeded after the caller deadline")
		}
	})
	t.Run("stalled socket releases the writer", func(t *testing.T) {
		agent, _ := hubWriterPipe(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- quotaTestWrite(ctx, agent) }()
		var result error
		finished := waitUntil(8*time.Second, func() bool {
			select {
			case result = <-done:
				return true
			default:
				return false
			}
		})
		if !finished {
			cancel()
			<-done
			t.Fatal("stalled socket held the writer past its five-second write limit")
		}
		if result == nil || ctx.Err() != nil {
			t.Fatalf("socket write must expire before its parent context: err=%v parent=%v", result, ctx.Err())
		}
	})
}
