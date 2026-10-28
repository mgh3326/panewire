package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// scopefuelEmitting writes a fixture scopefuel that prints exactly body.
func scopefuelEmitting(t *testing.T, body []byte) string {
	t.Helper()
	directory := t.TempDir()
	payload := filepath.Join(directory, "payload")
	if err := os.WriteFile(payload, body, 0644); err != nil {
		t.Fatal(err)
	}
	command := filepath.Join(directory, "scopefuel")
	if err := os.WriteFile(command, []byte("#!/bin/sh\ncat "+payload+"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	return command
}

// TestR19QuotaOutputBoundedByEncodedSize pins the bound on the side that
// matters. A 16 KiB stdout is within the raw cap, but JSON escaping doubles
// quotes and newlines and sextuples control bytes, so the same output can still
// exceed the 32 KiB message the hub is willing to read; such a report must be
// refused at the node rather than silently closing the connection.
func TestR19QuotaOutputBoundedByEncodedSize(t *testing.T) {
	for _, testCase := range []struct {
		name string
		fill byte
	}{
		{"quote", '"'},
		{"newline", '\n'},
		{"control", 0x01},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			body := bytes.Repeat([]byte{testCase.fill}, hubQuotaOutputLimit)
			if encoded := hubQuotaEncodedSize(body); encoded <= hubQuotaEncodedLimit {
				t.Fatalf("fixture does not exercise the encoded bound: encoded=%d", encoded)
			}
			out, err := runHubScopefuel(context.Background(), scopefuelEmitting(t, body))
			if err == nil || err.Error() != "output_too_large" {
				t.Fatalf("raw=%d error=%v, want output_too_large", len(body), err)
			}
			if len(out) != 0 {
				t.Fatalf("rejected report still returned %d bytes", len(out))
			}
		})
	}
	// The encoded bound must not shrink the documented raw allowance for
	// ordinary output: 16 KiB that needs no escaping still passes.
	plain := bytes.Repeat([]byte{'a'}, hubQuotaOutputLimit)
	out, err := runHubScopefuel(context.Background(), scopefuelEmitting(t, plain))
	if err != nil || len(out) != hubQuotaOutputLimit {
		t.Fatalf("unescaped %d bytes rejected: len=%d err=%v", hubQuotaOutputLimit, len(out), err)
	}
	// Whatever passes must fit the hub's message limit once wrapped.
	report, marshalErr := json.Marshal(struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Payload   string `json:"payload"`
	}{"quota.report", strings.Repeat("r", 96), string(out)})
	if marshalErr != nil || len(report) > hubMaxMessageBytes {
		t.Fatalf("accepted report does not fit the protocol limit: len=%d limit=%d err=%v", len(report), hubMaxMessageBytes, marshalErr)
	}
}

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
