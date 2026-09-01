package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestHubR10FailoverEventsFollowWatchedPresenceStateMachine(t *testing.T) {
	clock := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{
			"operator": r6OperatorToken,
			"node-a":   r6NodeAToken,
			"node-b":   r6NodeBToken,
		},
		AlertNodes:        map[string]struct{}{"node-a": {}},
		Now:               func() time.Time { return clock },
		StaleAfter:        time.Second,
		KeepaliveInterval: time.Hour,
		GracePeriod:       2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	presenceOnly := r6DialAgent(t, server.URL, "node-b", r6NodeBToken)
	defer presenceOnly.Close(websocket.StatusNormalClosure, "")
	r6Write(t, presenceOnly, map[string]any{"type": "hello", "machine_id": "node-b", "version": "r10-presence-only"})
	r6Eventually(t, "presence-only node", func() bool { return r6NodeState(r6Nodes(t, server), "node-b") == "connected" })
	quiet := r6DialEvents(t, server.URL)
	clock = clock.Add(time.Second)
	hub.Sweep() // stale
	clock = clock.Add(2 * time.Second)
	hub.Sweep() // first post-grace observation
	clock = clock.Add(time.Second)
	hub.Sweep() // second post-grace observation: a removed class gate emits here
	r10ExpectNoEvent(t, quiet)
	_ = quiet.Close(websocket.StatusNormalClosure, "")

	events := r6DialEvents(t, server.URL)
	defer events.Close(websocket.StatusNormalClosure, "")
	watched := r6DialAgent(t, server.URL, "node-a", r6NodeAToken)
	defer watched.Close(websocket.StatusNormalClosure, "")
	r6Write(t, watched, map[string]any{"type": "hello", "machine_id": "node-a", "version": "r10-watched"})
	r6Eventually(t, "watched node", func() bool { return r6NodeState(r6Nodes(t, server), "node-a") == "connected" })
	clock = clock.Add(time.Second)
	hub.Sweep() // stale
	clock = clock.Add(2 * time.Second)
	hub.Sweep() // first post-grace observation
	clock = clock.Add(time.Second)
	hub.Sweep() // second post-grace observation
	if event := r10ReadFailover(t, events); event.Machine != "node-a" || event.Phase != hubFailoverPhaseDown || !event.EmittedAt.Equal(clock) {
		t.Fatalf("down event=%+v", event)
	}

	r6Write(t, watched, map[string]any{"type": "ping"})
	r6Eventually(t, "watched node recovered", func() bool { return r6NodeState(r6Nodes(t, server), "node-a") == "connected" })
	hub.Sweep() // first clear observation
	clock = clock.Add(500 * time.Millisecond)
	hub.Sweep() // second clear observation
	if event := r10ReadFailover(t, events); event.Machine != "node-a" || event.Phase != hubFailoverPhaseUp || !event.EmittedAt.Equal(clock) {
		t.Fatalf("up event=%+v", event)
	}
}

func TestHubR10AcceptingFlagAppearsInNodesAndStatus(t *testing.T) {
	_, server := r6HubServer(t, time.Now, nil)
	defer server.Close()
	root := t.TempDir()
	nodeEnv := filepath.Join(root, "node.env")
	operatorEnv := filepath.Join(root, "operator.env")
	if err := os.WriteFile(nodeEnv, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(operatorEnv, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+r6OperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	daemon, code, err := newDaemonForCLI([]string{
		"--socket", filepath.Join(root, "daemon.sock"),
		"--hub-url", r6WSURL(server.URL, ""),
		"--hub-token-env", nodeEnv,
		"--hub-accepting",
	}, daemonCLIDeps{AllowInsecureForTests: true})
	if err != nil || code != ExitOK || daemon == nil || daemon.cfg.Hub.Client == nil || !daemon.cfg.Hub.Client.accepting {
		t.Fatalf("accepting daemon configuration: daemon=%v code=%d err=%v", daemon, code, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		daemon.cfg.Hub.Client.Run(ctx)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("accepting hub client did not stop")
		}
	}()

	defaultNode := r6DialAgent(t, server.URL, "node-b", r6NodeBToken)
	defer defaultNode.Close(websocket.StatusNormalClosure, "")
	r6Write(t, defaultNode, map[string]any{"type": "hello", "machine_id": "node-b", "version": "r10-default"})
	r6Eventually(t, "accepting and default nodes", func() bool {
		nodes := r6Nodes(t, server)
		return len(nodes) == 2 && r10NodeAccepting(nodes, "node-a") && !r10NodeAccepting(nodes, "node-b")
	})

	var stdout, stderr bytes.Buffer
	if code := runHubStatusCLI([]string{"--hub-url", server.URL, "--hub-token-env", operatorEnv}, &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("hub-status code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := stdout.String(); !bytes.Contains([]byte(got), []byte("ACCEPTING")) || !bytes.Contains([]byte(got), []byte("node-a\twatched\tconnected\ttrue")) || !bytes.Contains([]byte(got), []byte("node-b\twatched\tconnected\tfalse")) {
		t.Fatalf("hub-status omitted accepting values: %q", got)
	}
}

type r10FailoverWire struct {
	Type      string
	Machine   string
	Phase     string
	EmittedAt time.Time
}

func r10ReadFailover(t *testing.T, connection *websocket.Conn) r10FailoverWire {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var fields map[string]json.RawMessage
	if err := wsjson.Read(ctx, connection, &fields); err != nil {
		t.Fatalf("read failover event: %v", err)
	}
	if len(fields) != 4 {
		t.Fatalf("failover event has non-closed shape: %v", fields)
	}
	var event r10FailoverWire
	var emittedAt string
	if err := json.Unmarshal(fields["type"], &event.Type); err != nil || event.Type != "failover" || json.Unmarshal(fields["machine"], &event.Machine) != nil || json.Unmarshal(fields["phase"], &event.Phase) != nil || json.Unmarshal(fields["emitted_at"], &emittedAt) != nil || !parseHubFailoverEmittedAt(emittedAt, &event.EmittedAt) || !machineIDPattern.MatchString(event.Machine) || !validHubFailoverPhase(event.Phase) {
		t.Fatalf("invalid failover event: fields=%v err=%v", fields, err)
	}
	return event
}

func r10ExpectNoEvent(t *testing.T, connection *websocket.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	var fields map[string]json.RawMessage
	if err := wsjson.Read(ctx, connection, &fields); err == nil {
		t.Fatalf("presence-only node emitted event: %v", fields)
	}
}

func r10NodeAccepting(nodes []r6NodeWire, machineID string) bool {
	for _, node := range nodes {
		if node.MachineID == machineID {
			return node.Accepting
		}
	}
	return false
}
