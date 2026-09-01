package panewire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestR8PresenceOnlyNodeSuppressesPresenceAndCheckAlerts(t *testing.T) {
	clock := time.Date(2026, 8, 31, 5, 0, 0, 0, time.UTC)
	capture := &r7TelegramCapture{token: "r8-presence-only-bot-token"}
	server := httptest.NewServer(capture)
	defer server.Close()
	notifier, err := newHubTelegramNotifier(hubTelegramEnv{BotToken: capture.token, ChatID: "r8-presence-only-chat"}, hubNotifierDeps{
		HTTPClient: server.Client(), BaseURL: server.URL, AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{
			"operator": r6OperatorToken,
			"node-a":   r6NodeAToken,
			"node-b":   r6NodeBToken,
		},
		AlertNodes:  map[string]struct{}{"node-a": {}},
		Now:         func() time.Time { return clock },
		GracePeriod: defaultHubGracePeriod,
		Notifier:    notifier,
	})
	if err != nil {
		t.Fatal(err)
	}

	agent := &hubAgent{}
	hub.connect("node-b", "r8-b", "fixture", agent, false)
	hub.disconnect("node-b", agent)
	clock = clock.Add(defaultHubGracePeriod)
	hub.Sweep() // first post-grace observation
	clock = clock.Add(time.Second)
	hub.Sweep() // second post-grace observation: a removed scope gate alerts here
	if got := capture.Messages(); len(got) != 0 {
		t.Fatalf("presence-only disconnect notified: %v", got)
	}

	agent = &hubAgent{}
	hub.connect("node-b", "r8-b", "fixture", agent, false)
	r8Heartbeat(t, hub, "node-b", agent, HubCheckFail)
	r8Heartbeat(t, hub, "node-b", agent, HubCheckFail)
	if got := capture.Messages(); len(got) != 0 {
		t.Fatalf("presence-only check notified: %v", got)
	}

	nodes := hub.Nodes()
	if len(nodes) != 1 || nodes[0].MachineID != "node-b" || nodes[0].AlertClass != "presence-only" {
		t.Fatalf("presence-only node was not retained in node view: %+v", nodes)
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/nodes", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer "+r6OperatorToken)
	response := httptest.NewRecorder()
	hub.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("node view status=%d, want 200", response.Code)
	}
	var body struct {
		Nodes []HubNode `json:"nodes"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil || len(body.Nodes) != 1 || body.Nodes[0].AlertClass != "presence-only" {
		t.Fatalf("node API omitted presence-only classification: body=%+v err=%v", body, err)
	}
}

func TestR8AlertNodesCLIRejectsUnregisteredMachineID(t *testing.T) {
	root := t.TempDir()
	authPath := filepath.Join(root, "hub-auth.env")
	if err := os.WriteFile(authPath, []byte("HUB_TOKEN_operator="+r6OperatorToken+"\nHUB_TOKEN_node-a="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--alert-nodes", "node-typo"}, nil)
	if hub != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("unknown alert node accepted: hub=%v code=%d err=%v", hub, code, err)
	}
}

func TestR8AlertNodesOmittedWatchesAllNodes(t *testing.T) {
	root := t.TempDir()
	authPath := filepath.Join(root, "hub-auth.env")
	if err := os.WriteFile(authPath, []byte("HUB_TOKEN_operator="+r6OperatorToken+"\nHUB_TOKEN_node-a="+r6NodeAToken+"\nHUB_TOKEN_node-b="+r6NodeBToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath}, nil)
	if err != nil || code != ExitOK || hub == nil {
		t.Fatalf("hub without alert-nodes: hub=%v code=%d err=%v", hub, code, err)
	}
	if hub.alertClass("node-a") != "watched" || hub.alertClass("node-b") != "watched" {
		t.Fatalf("omitted alert-nodes did not preserve all-watched default")
	}

	hub, _, code, err = newHubServerForCLI([]string{"--hub-auth", authPath, "--alert-nodes", "node-a"}, nil)
	if err != nil || code != ExitOK || hub == nil {
		t.Fatalf("hub with alert-nodes: hub=%v code=%d err=%v", hub, code, err)
	}
	if hub.alertClass("node-a") != "watched" || hub.alertClass("node-b") != "presence-only" {
		t.Fatalf("alert-nodes did not classify nodes: node-a=%q node-b=%q", hub.alertClass("node-a"), hub.alertClass("node-b"))
	}
}

func TestR8HubStatusRendersNodeClass(t *testing.T) {
	nodes := []HubNode{{
		MachineID: "node-b", AlertClass: "presence-only", State: "connected",
		ConnectedSince: time.Date(2026, 8, 31, 5, 0, 0, 0, time.UTC), RemoteMeta: map[string]string{},
	}}
	if !validHubStatusNodes(nodes) {
		t.Fatal("valid presence-only node rejected")
	}
	nodes[0].AlertClass = "invalid"
	if validHubStatusNodes(nodes) {
		t.Fatal("invalid node class accepted")
	}
	nodes[0].AlertClass = "presence-only"
	var output bytes.Buffer
	renderHubStatus(&output, nodes)
	if got := output.String(); !strings.Contains(got, "MACHINE\tCLASS\tSTATE") || !strings.Contains(got, "node-b\tpresence-only\tconnected") {
		t.Fatalf("hub-status omitted node class: %q", got)
	}
}

func r8Heartbeat(t *testing.T, hub *HubServer, machineID string, agent *hubAgent, status HubCheckStatus) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"type": "event", "kind": "heartbeat", "payload": map[string]any{"status": "alive", "checks": map[string]HubCheckStatus{"service": status}},
	})
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage(machineID, "fixture", agent, payload)
}
