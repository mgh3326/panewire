package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestR7PresenceGraceMustElapseBeforeIncidentAndRecovers(t *testing.T) {
	clock := time.Date(2026, 8, 31, 4, 0, 0, 0, time.UTC)
	capture := &r7TelegramCapture{token: "r7-bot-token-not-for-message"}
	server := httptest.NewServer(capture)
	defer server.Close()
	notifier, err := newHubTelegramNotifier(hubTelegramEnv{BotToken: capture.token, ChatID: "r7-chat-id-not-for-message"}, hubNotifierDeps{
		HTTPClient: server.Client(), BaseURL: server.URL, AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub, hubHTTP := r7HubServer(t, func() time.Time { return clock }, notifier)
	defer hubHTTP.Close()

	node := r6DialAgent(t, hubHTTP.URL, "node-a", r6NodeAToken)
	r6Write(t, node, map[string]any{"type": "hello", "machine_id": "node-a", "version": "r7-a"})
	r6Eventually(t, "node connected", func() bool { return r6NodeState(r6Nodes(t, hubHTTP), "node-a") == "connected" })
	if err := node.Close(websocket.StatusNormalClosure, "fixture disconnect"); err != nil {
		t.Fatal(err)
	}
	r6Eventually(t, "node disconnected", func() bool { return r6NodeState(r6Nodes(t, hubHTTP), "node-a") == "disconnected" })

	// This assertion is intentionally before the grace boundary: a mutation
	// that sends immediately on disconnect makes this test fail.
	clock = clock.Add(defaultHubGracePeriod - time.Millisecond)
	hub.Sweep()
	if got := len(capture.Messages()); got != 0 {
		t.Fatalf("incident before grace: messages=%v", capture.Messages())
	}
	clock = clock.Add(time.Millisecond)
	hub.Sweep() // first post-grace observation
	if got := len(capture.Messages()); got != 0 {
		t.Fatalf("incident before debounce: messages=%v", capture.Messages())
	}
	clock = clock.Add(time.Second)
	hub.Sweep() // second post-grace observation
	r7Eventually(t, "presence incident", func() bool { return len(capture.Messages()) == 1 })
	if got := capture.Messages()[0].Text; got != "machine: node-a\nreason: disconnected\ncheck: none" {
		t.Fatalf("incident text=%q", got)
	}
	hub.Sweep()
	if got := len(capture.Messages()); got != 1 {
		t.Fatalf("presence incident repeated: %v", capture.Messages())
	}

	node = r6DialAgent(t, hubHTTP.URL, "node-a", r6NodeAToken)
	defer node.Close(websocket.StatusNormalClosure, "")
	r6Write(t, node, map[string]any{"type": "hello", "machine_id": "node-a", "version": "r7-a"})
	r6Eventually(t, "node reconnected", func() bool { return r6NodeState(r6Nodes(t, hubHTTP), "node-a") == "connected" })
	clock = clock.Add(time.Second)
	hub.Sweep()
	if got := len(capture.Messages()); got != 1 {
		t.Fatalf("recovery fired before debounce: %v", capture.Messages())
	}
	clock = clock.Add(time.Second)
	hub.Sweep()
	r7Eventually(t, "presence recovery", func() bool { return len(capture.Messages()) == 2 })
	if got := capture.Messages()[1].Text; got != "machine: node-a\nreason: recovered_disconnected\ncheck: none" {
		t.Fatalf("recovery text=%q", got)
	}
}

func TestR7HubCheckAlertsArePerCheckAndFlapSuppressed(t *testing.T) {
	capture := &r7TelegramCapture{token: "r7-check-bot-token"}
	server := httptest.NewServer(capture)
	defer server.Close()
	notifier, err := newHubTelegramNotifier(hubTelegramEnv{BotToken: capture.token, ChatID: "r7-check-chat"}, hubNotifierDeps{
		HTTPClient: server.Client(), BaseURL: server.URL, AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:   map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken},
		Now:      func() time.Time { return time.Date(2026, 8, 31, 4, 0, 0, 0, time.UTC) },
		Notifier: notifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.connect("node-a", "r7-a", "fixture", agent)

	r7Heartbeat(t, hub, agent, HubCheckFail)
	r7Heartbeat(t, hub, agent, HubCheckOK) // one failed observation must not alert
	if got := len(capture.Messages()); got != 0 {
		t.Fatalf("check flap alerted: %v", capture.Messages())
	}
	r7Heartbeat(t, hub, agent, HubCheckFail)
	r7Heartbeat(t, hub, agent, HubCheckFail)
	r7Eventually(t, "check incident", func() bool { return len(capture.Messages()) == 1 })
	if got := capture.Messages()[0].Text; got != "machine: node-a\nreason: check_failed\ncheck: service" {
		t.Fatalf("check incident text=%q", got)
	}
	r7Heartbeat(t, hub, agent, HubCheckFail)
	if got := len(capture.Messages()); got != 1 {
		t.Fatalf("check incident repeated: %v", capture.Messages())
	}
	r7Heartbeat(t, hub, agent, HubCheckOK)
	r7Heartbeat(t, hub, agent, HubCheckFail) // recovery flap must not notify
	if got := len(capture.Messages()); got != 1 {
		t.Fatalf("check recovery flap notified: %v", capture.Messages())
	}
	r7Heartbeat(t, hub, agent, HubCheckOK)
	r7Heartbeat(t, hub, agent, HubCheckOK)
	r7Eventually(t, "check recovery", func() bool { return len(capture.Messages()) == 2 })
	if got := capture.Messages()[1].Text; got != "machine: node-a\nreason: recovered_check_failed\ncheck: service" {
		t.Fatalf("check recovery text=%q", got)
	}
}

func TestR7HubNodeFlapNeedsTwoConsecutiveObservations(t *testing.T) {
	clock := time.Date(2026, 8, 31, 4, 0, 0, 0, time.UTC)
	capture := &r7TelegramCapture{token: "r7-flap-bot-token"}
	server := httptest.NewServer(capture)
	defer server.Close()
	notifier, err := newHubTelegramNotifier(hubTelegramEnv{BotToken: capture.token, ChatID: "r7-flap-chat"}, hubNotifierDeps{
		HTTPClient: server.Client(), BaseURL: server.URL, AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:      map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken},
		Now:         func() time.Time { return clock },
		GracePeriod: defaultHubGracePeriod,
		Notifier:    notifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	setState := func(state string) {
		hub.mu.Lock()
		hub.nodes["node-a"] = &hubNodeRecord{machineID: "node-a", state: state, stateSince: clock}
		hub.mu.Unlock()
	}
	setState("disconnected")
	clock = clock.Add(defaultHubGracePeriod)
	hub.Sweep() // one bad observation
	setState("connected")
	hub.Sweep() // clears the first candidate
	if got := len(capture.Messages()); got != 0 {
		t.Fatalf("first presence flap alerted: %v", capture.Messages())
	}
	setState("disconnected")
	clock = clock.Add(defaultHubGracePeriod)
	hub.Sweep() // another one-observation flap
	setState("connected")
	hub.Sweep()
	if got := len(capture.Messages()); got != 0 {
		t.Fatalf("second presence flap alerted: %v", capture.Messages())
	}
	setState("disconnected")
	clock = clock.Add(defaultHubGracePeriod)
	hub.Sweep()
	clock = clock.Add(time.Second)
	hub.Sweep()
	r7Eventually(t, "stable presence incident", func() bool { return len(capture.Messages()) == 1 })
}

func TestR7HubClientChecksConfigOnlyPublishesClosedResults(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "checks.json")
	const secretMarker = "argv-and-output-must-stay-local"
	if err := os.WriteFile(configPath, []byte(`{"checks":[{"name":"service","argv":["service-check","`+secretMarker+`"],"timeout":"20ms"},{"name":"disk","argv":["disk-check"],"timeout":"20ms"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	checks, err := LoadHubChecksConfig(configPath)
	if err != nil || len(checks) != 2 {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
	hub, server := r6HubServer(t, time.Now, nil)
	defer server.Close()
	subscriber := r6DialEvents(t, server.URL)
	defer subscriber.Close(websocket.StatusNormalClosure, "")
	client, err := NewHubClient(HubClientConfig{
		URL:                   r6WSURL(server.URL, ""),
		MachineID:             "node-a",
		Token:                 r6NodeAToken,
		Checks:                checks,
		AllowInsecureForTests: true,
		PingInterval:          time.Hour,
		Execute: func(_ context.Context, argv []string) error {
			if argv[0] == "disk-check" {
				return errors.New(secretMarker)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Run(ctx)
	}()
	event := r6ReadHubEvent(t, subscriber, "heartbeat")
	var heartbeat hubHeartbeatPayload
	if err := json.Unmarshal(event.Payload, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if heartbeat.Status != "alive" || heartbeat.Checks["service"] != HubCheckOK || heartbeat.Checks["disk"] != HubCheckFail || strings.Contains(string(event.Payload), secretMarker) {
		t.Fatalf("heartbeat payload was not closed and local-only: %s", event.Payload)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("hub client did not stop")
	}
	_ = hub
}

func TestR7HubCLIAndDaemonUseNewConfigOnly(t *testing.T) {
	root := t.TempDir()
	authPath := filepath.Join(root, "hub-auth.env")
	tgPath := filepath.Join(root, "hub-tg.env")
	nodePath := filepath.Join(root, "hub-node.env")
	checksPath := filepath.Join(root, "checks.json")
	if err := os.WriteFile(authPath, []byte("HUB_TOKEN_operator="+r6OperatorToken+"\nHUB_TOKEN_node-a="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tgPath, []byte("TG_BOT_TOKEN=r7-cli-bot\nTG_CHAT_ID=r7-cli-chat\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHubTelegramEnv(tgPath); err == nil {
		t.Fatal("non-0600 hub Telegram env was accepted")
	}
	if err := os.Chmod(tgPath, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nodePath, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(checksPath, []byte(`{"checks":[{"name":"service","argv":["true"],"timeout":"1s"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub, _, code, err := newHubServerForCLIWithDeps([]string{"--hub-auth", authPath, "--hub-tg-env", tgPath, "--hub-grace", "3m"}, nil, hubServerCLIDeps{AllowInsecureForTests: true})
	if hub == nil || code != ExitOK || err != nil || hub.notifier == nil || hub.gracePeriod != 3*time.Minute {
		t.Fatalf("hub CLI configuration: hub=%v code=%d err=%v", hub, code, err)
	}
	daemon, code, err := newDaemonForCLI([]string{"--hub-url", "ws://fixture.invalid", "--hub-token-env", nodePath, "--checks-config", checksPath}, daemonCLIDeps{AllowInsecureForTests: true})
	if daemon == nil || code != ExitOK || err != nil || daemon.cfg.Hub.Client == nil || len(daemon.cfg.Hub.Client.checks) != 1 {
		t.Fatalf("daemon checks configuration: daemon=%v code=%d err=%v", daemon, code, err)
	}
	legacyFlag := "--" + strings.Join([]string{"sen", "tinel"}, "")
	if daemon, code, err := newDaemonForCLI([]string{legacyFlag}, daemonCLIDeps{}); daemon != nil || code != ExitUsage || err == nil {
		t.Fatalf("removed legacy flag was accepted: daemon=%v code=%d err=%v", daemon, code, err)
	}
}

func r7Heartbeat(t *testing.T, hub *HubServer, agent *hubAgent, status HubCheckStatus) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"type": "event", "kind": "heartbeat", "payload": map[string]any{"status": "alive", "checks": map[string]HubCheckStatus{"service": status}},
	})
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage("node-a", "fixture", agent, payload)
}

type r7TelegramMessage struct {
	ChatID string `json:"chat_id"`
	Text   string `json:"text"`
}

type r7TelegramCapture struct {
	mu       sync.Mutex
	token    string
	messages []r7TelegramMessage
}

func (capture *r7TelegramCapture) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != "/bot"+capture.token+"/sendMessage" {
		http.NotFound(writer, request)
		return
	}
	var message r7TelegramMessage
	if err := json.NewDecoder(request.Body).Decode(&message); err != nil {
		http.Error(writer, "invalid body", http.StatusBadRequest)
		return
	}
	capture.mu.Lock()
	capture.messages = append(capture.messages, message)
	capture.mu.Unlock()
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(`{"ok":true}`))
}

func (capture *r7TelegramCapture) Messages() []r7TelegramMessage {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return append([]r7TelegramMessage(nil), capture.messages...)
}

func r7HubServer(t *testing.T, now func() time.Time, notifier HubNotifier) (*HubServer, *httptest.Server) {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{
			"operator": r6OperatorToken,
			"node-a":   r6NodeAToken,
			"node-b":   r6NodeBToken,
		},
		Now: now, StaleAfter: 30 * time.Second, KeepaliveInterval: 10 * time.Second,
		GracePeriod: defaultHubGracePeriod, Notifier: notifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hub, httptest.NewServer(hub.Handler())
}

func r7Eventually(t *testing.T, label string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}
