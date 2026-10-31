package panewire

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// helloFallbackHub is a fake hub for the rollout contract in
// docs/r26-hub-spawn.md. strict mimics a hub from before spawn_v2: its hello
// parser allows only the original fields, so a hello that carries
// `capabilities` is dropped, no node is registered, and a ping from the
// unregistered node gets no pong — which is exactly how the real parser and
// touch() behave.
type helloFallbackHub struct {
	strict bool

	mu         sync.Mutex
	hellos     []map[string]any
	registered bool
}

func (hub *helloFallbackHub) serve(t *testing.T) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		for {
			var message map[string]any
			if wsjson.Read(request.Context(), connection, &message) != nil {
				return
			}
			switch message["type"] {
			case "hello":
				hub.mu.Lock()
				hub.hellos = append(hub.hellos, message)
				_, hasCapabilities := message["capabilities"]
				if !(hub.strict && hasCapabilities) {
					hub.registered = true
				}
				hub.mu.Unlock()
			case "ping":
				hub.mu.Lock()
				registered := hub.registered
				hub.mu.Unlock()
				if registered {
					_ = wsjson.Write(request.Context(), connection, map[string]any{"type": "pong"})
				}
			}
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func (hub *helloFallbackHub) snapshot() (hellos []map[string]any, registered bool) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return append([]map[string]any(nil), hub.hellos...), hub.registered
}

func runHelloFallbackNode(t *testing.T, server *httptest.Server, ackTimeout time.Duration) {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour, helloAckTimeout: ackTimeout})
	if err != nil {
		t.Fatal(err)
	}
	connection, _, err := websocket.Dial(t.Context(), client.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.serve(ctx, connection)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func helloHasSpawnV2(hello map[string]any) bool {
	capabilities, _ := hello["capabilities"].([]any)
	return len(capabilities) == 1 && capabilities[0] == "spawn_v2"
}

func TestHelloFallbackOldHubRegistersWithoutCapabilities(t *testing.T) {
	hub := &helloFallbackHub{strict: true}
	runHelloFallbackNode(t, hub.serve(t), 100*time.Millisecond)

	eventually(t, 5*time.Second, "old hub registered the node", func() bool {
		_, registered := hub.snapshot()
		return registered
	})
	hellos, _ := hub.snapshot()
	if len(hellos) != 2 {
		t.Fatalf("want one hello with capabilities and one fallback, got %d: %v", len(hellos), hellos)
	}
	if !helloHasSpawnV2(hellos[0]) {
		t.Fatalf("first hello must advertise spawn_v2: %v", hellos[0])
	}
	if _, present := hellos[1]["capabilities"]; present {
		t.Fatalf("fallback hello must omit capabilities: %v", hellos[1])
	}
	for _, field := range []string{"type", "machine_id", "version"} {
		if hellos[1][field] != hellos[0][field] {
			t.Fatalf("fallback hello changed %s: %v vs %v", field, hellos[0][field], hellos[1][field])
		}
	}
}

func TestHelloFallbackNewHubSendsCapabilitiesOnce(t *testing.T) {
	hub := &helloFallbackHub{}
	const ackTimeout = 100 * time.Millisecond
	runHelloFallbackNode(t, hub.serve(t), ackTimeout)

	eventually(t, 5*time.Second, "new hub registered the node", func() bool {
		_, registered := hub.snapshot()
		return registered
	})
	// Several ack windows must pass without a second hello. Waiting on a
	// timer, not polling, because the claim is that nothing more arrives.
	<-time.After(5 * ackTimeout)
	hellos, _ := hub.snapshot()
	if len(hellos) != 1 || !helloHasSpawnV2(hellos[0]) {
		t.Fatalf("an accepting hub must see exactly one hello with spawn_v2, got %v", hellos)
	}
}

// The real hub, not the fake, must keep accepting the capabilities hello and
// record the node as spawn_v2-capable.
func TestHelloFallbackRealHubKeepsCapabilities(t *testing.T) {
	hub, server := t14Hub(t)
	client, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour, helloAckTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	eventually(t, 5*time.Second, "real hub recorded spawn_v2", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes["machine-a"]
		return record != nil && record.agent != nil && record.capabilities["spawn_v2"]
	})
	<-time.After(500 * time.Millisecond)
	hub.mu.Lock()
	record := hub.nodes["machine-a"]
	stillCapable := record != nil && record.capabilities["spawn_v2"]
	hub.mu.Unlock()
	if !stillCapable {
		t.Fatal("a later fallback hello replaced the registered capabilities")
	}
}
