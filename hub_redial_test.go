package panewire

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// Tests for how a node comes back after its hub session ends.
// Contract: docs/hub-drain.md.

func TestHubRedialDelayBounds(t *testing.T) {
	for range 200 {
		if delay := hubRedialDelay(30*time.Second, true); delay < 0 || delay >= hubRedialFirstMax {
			t.Fatalf("first retry %s is outside [0, %s)", delay, hubRedialFirstMax)
		}
		if delay := hubRedialDelay(30*time.Second, false); delay < hubRedialFastDelay/2 || delay >= hubRedialFastDelay*3/2 {
			t.Fatalf("fast retry %s is outside [%s, %s)", delay, hubRedialFastDelay/2, hubRedialFastDelay*3/2)
		}
		// A test or operator that sets a small back-off keeps it.
		if delay := hubRedialDelay(10*time.Millisecond, false); delay >= 15*time.Millisecond {
			t.Fatalf("a 10ms back-off was stretched to %s", delay)
		}
	}
}

// redialNodeClient builds a node client for a fake hub. The long back-off is
// the point: only the fast redial schedule can reconnect inside a few seconds.
func redialNodeClient(t *testing.T, url string, wait func(context.Context, time.Duration) error) *HubClient {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{URL: url, MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour,
		InitialBackoff: 30 * time.Second, MaxBackoff: time.Minute, Wait: wait,
		JobsInboxRoot:       t.TempDir(),
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, nil },
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func runRedialNode(t *testing.T, client *HubClient) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); client.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
}

func TestNodeRedialsAtOnceOnRedialMessageAndOnSocketClose(t *testing.T) {
	var mu sync.Mutex
	var connections int
	var waits []time.Duration
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		mu.Lock()
		connections++
		number := connections
		mu.Unlock()
		switch number {
		case 1:
			// The hub says redial and keeps the socket open: only the message
			// can make the node leave.
			_ = wsjson.Write(request.Context(), connection, map[string]any{"type": "redial"})
		case 2:
			// The hub vanishes without a word.
			return
		}
		for {
			if _, _, err := connection.Read(request.Context()); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	client := redialNodeClient(t, t14WSURL(server.URL, ""), func(ctx context.Context, delay time.Duration) error {
		mu.Lock()
		waits = append(waits, delay)
		mu.Unlock()
		return nil
	})
	runRedialNode(t, client)

	eventually(t, 5*time.Second, "the node to dial three times: first, after redial, after a plain close", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return connections >= 3
	})
	mu.Lock()
	defer mu.Unlock()
	if len(waits) < 2 {
		t.Fatalf("expected a pause before each redial, got %v", waits)
	}
	// Each session that ends starts the fast schedule over, so both pauses are
	// first retries, far under the configured 30s back-off.
	for index, delay := range waits[:2] {
		if delay >= hubRedialFirstMax {
			t.Fatalf("redial %d paused %s; want under %s", index, delay, hubRedialFirstMax)
		}
	}
}

func TestNodeReconnectsWithinSecondsWhenTheHubRestarts(t *testing.T) {
	hub1, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub1.Close() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	firstSignal, signalFirst := context.WithCancel(t.Context())
	firstExit := make(chan int, 1)
	go func() { firstExit <- serveHub(firstSignal, hub1, []net.Listener{listener}, time.Second) }()

	// No Wait override: the real schedule, against a 30s configured back-off.
	client := redialNodeClient(t, "ws://"+address, nil)
	runRedialNode(t, client)
	nodeConnected := func(hub *HubServer) func() bool {
		return func() bool {
			hub.mu.Lock()
			defer hub.mu.Unlock()
			record := hub.nodes["machine-a"]
			return record != nil && record.agent != nil && record.state == "connected"
		}
	}
	eventually(t, 5*time.Second, "the node to connect to the first hub", nodeConnected(hub1))

	signalFirst() // SIGTERM on the old hub
	select {
	case <-firstExit:
	case <-time.After(5 * time.Second):
		t.Fatal("the old hub did not exit")
	}
	// The gap between the old process exiting and the new one listening is the
	// simulated downtime of a restart.
	time.Sleep(500 * time.Millisecond)

	hub2, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub2.Close() })
	listener2, err := net.Listen("tcp", address)
	if err != nil {
		t.Skipf("the old hub's port was taken before the new hub could bind it: %v", err)
	}
	secondSignal, signalSecond := context.WithCancel(t.Context())
	secondExit := make(chan int, 1)
	go func() { secondExit <- serveHub(secondSignal, hub2, []net.Listener{listener2}, time.Second) }()
	t.Cleanup(func() { signalSecond(); <-secondExit })
	started := time.Now()
	eventually(t, 5*time.Second, "the node to reconnect to the restarted hub", nodeConnected(hub2))
	t.Logf("node reconnected %s after the new hub started listening", time.Since(started))
}
