package panewire

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestR19PreferenceSwitchesDoNotNest walks the node up five preferred endpoints
// in one session. Each switch used to be a recursive serve call, so the
// abandoned frame kept its ping ticker, its preference ticker, and its stack
// alive until the whole chain unwound.
func TestR19PreferenceSwitchesDoNotNest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(request.Context()); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	endpoints := []string{
		"ws://127.0.0.1:9001", "ws://127.0.0.1:9002", "ws://127.0.0.1:9003",
		"ws://127.0.0.1:9004", "ws://127.0.0.1:9005", "ws://127.0.0.1:9006",
	}
	const switchCount = 5
	client, err := NewHubClient(HubClientConfig{
		URLs: endpoints, MachineID: "node-a", Token: "node-token", AllowInsecureForTests: true,
		PingInterval: time.Hour, PreferRetry: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	target := switchCount - 1
	switches := 0
	// Only the endpoint one step above the current one accepts a dial, so each
	// preference tick moves the session up exactly one endpoint.
	client.dial = func(ctx context.Context, endpoint string, _ *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
		mu.Lock()
		wanted := ""
		if target >= 0 {
			wanted = client.endpoints[target]
		}
		mu.Unlock()
		if endpoint != wanted {
			return nil, nil, errors.New("down")
		}
		conn, response, dialErr := websocket.Dial(ctx, r6WSURL(server.URL, ""), nil)
		if dialErr != nil {
			return nil, nil, dialErr
		}
		mu.Lock()
		target--
		switches++
		mu.Unlock()
		return conn, response, nil
	}
	client.endpoint = client.endpoints[switchCount]

	first, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- client.serve(ctx, first) }()

	baseline := 0
	deadline := time.Now().Add(15 * time.Second)
	for {
		mu.Lock()
		done := switches
		mu.Unlock()
		if done == 1 && baseline == 0 {
			// Measured once the session is fully established, so the reader
			// goroutine and the preference probe are already counted.
			baseline = runtime.NumGoroutine()
		}
		if done == switchCount {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d preference switches completed", done, switchCount)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Let the superseded session's reader goroutine observe its closed
	// connection and exit before anything is counted.
	time.Sleep(300 * time.Millisecond)

	if nested := serveConnectionFrames(); nested != 1 {
		t.Fatalf("%d serveConnection frames are live after %d switches, want 1", nested, switchCount)
	}
	if grown := runtime.NumGoroutine() - baseline; grown > 2 {
		t.Fatalf("%d preference switches leaked %d goroutines", switchCount, grown)
	}
	cancel()
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not stop")
	}
}

// serveConnectionFrames counts the live serveConnection frames across all
// goroutines. One session must occupy exactly one frame; a recursive switch
// shows up as a frame per abandoned session, each still holding two tickers.
func serveConnectionFrames() int {
	buffer := make([]byte, 1<<20)
	for {
		length := runtime.Stack(buffer, true)
		if length < len(buffer) {
			return strings.Count(string(buffer[:length]), "(*HubClient).serveConnection(")
		}
		buffer = make([]byte, 2*len(buffer))
	}
}
