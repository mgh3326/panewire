package panewire

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// unparsedHub registers any node, then sends every queued raw frame and
// records the relay.unconfirmed events the node writes back.
type unparsedHub struct {
	frames [][]byte

	mu          sync.Mutex
	unconfirmed []map[string]any
}

func (hub *unparsedHub) serve(t *testing.T) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		sent := false
		for {
			var message map[string]any
			if wsjson.Read(request.Context(), connection, &message) != nil {
				return
			}
			switch message["type"] {
			case "ping":
				_ = wsjson.Write(request.Context(), connection, map[string]any{"type": "pong"})
				if !sent {
					sent = true
					for _, frame := range hub.frames {
						if connection.Write(request.Context(), websocket.MessageText, frame) != nil {
							return
						}
					}
				}
			case "event":
				if message["kind"] == "relay.unconfirmed" {
					payload, _ := message["payload"].(map[string]any)
					hub.mu.Lock()
					hub.unconfirmed = append(hub.unconfirmed, payload)
					hub.mu.Unlock()
				}
			}
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func (hub *unparsedHub) unconfirmedCount() int {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return len(hub.unconfirmed)
}

// A hub that still joins the inbox summary with a newline (the pre-#2889
// shape) must not vanish silently: the node warns, rate-limited and without
// the text, and reports relay.unconfirmed so the hub can replay or escalate.
func TestUnparsedRelayInjectWarnsAndAnswersUnconfirmed(t *testing.T) {
	bad := `{"type":"relay.inject","kind":"lane.event","job_id":"relay-job-7","pane":"w1:p1","text":"ESC choose SECRET-TEXT\n[inbox] 1 new: ready 1","lane":"operator-desk","event_id":7,"deliver":"now"}`
	if _, ok := parseHubOutbound([]byte(bad)); ok {
		t.Fatal("fixture must be unparseable")
	}
	hub := &unparsedHub{frames: [][]byte{[]byte(bad), []byte(bad), []byte(bad)}}
	server := hub.serve(t)
	var mu sync.Mutex
	var warnings []string
	client, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: 20 * time.Millisecond,
		JobsInboxRoot:       t.TempDir(),
		Warn:                func(message string) { mu.Lock(); warnings = append(warnings, message); mu.Unlock() },
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, nil },
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	pinned := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	client.now = func() time.Time { return pinned }
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

	eventually(t, 5*time.Second, "node answered every dropped directive", func() bool { return hub.unconfirmedCount() >= 3 })
	hub.mu.Lock()
	for _, payload := range hub.unconfirmed {
		if payload["job_id"] != "relay-job-7" || payload["pane"] != "w1:p1" || payload["original_event_id"] != float64(7) || payload["reason"] != relayUnparsedReason {
			t.Fatalf("unconfirmed payload %v", payload)
		}
	}
	hub.mu.Unlock()
	mu.Lock()
	defer mu.Unlock()
	if len(warnings) != 1 || !strings.Contains(warnings[0], "type=relay.inject") || strings.Contains(warnings[0], "SECRET-TEXT") {
		t.Fatalf("want one text-free warning for three drops, got %q", warnings)
	}
}

func TestUnparsedHubMessageWarnRateLimit(t *testing.T) {
	var warnings []string
	client := &HubClient{warn: func(message string) { warnings = append(warnings, message) }}
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	client.now = func() time.Time { return now }
	drop := func(payload string) {
		_, rejection := parseHubOutboundRejection([]byte(payload))
		if rejection.Class == "" {
			t.Fatalf("fixture %q parses", payload)
		}
		client.handleUnparsedHubMessage(t.Context(), nil, []byte(payload), rejection)
	}
	for i := 0; i < 4; i++ {
		drop(`{"type":"nonsense"}`)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "class=unknown_type") {
		t.Fatalf("burst warned %d times: %q", len(warnings), warnings)
	}
	now = now.Add(hubUnparsedWarnEvery)
	drop(`not json`)
	if len(warnings) != 2 || !strings.Contains(warnings[1], "type=unknown") || !strings.Contains(warnings[1], "class=json") || !strings.Contains(warnings[1], "rejected=4") {
		t.Fatalf("after the window: %q", warnings)
	}
}
