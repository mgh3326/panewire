package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	r6OperatorToken         = "r6-fixture-operator-token-not-for-logs"
	r6NodeAToken            = "r6-fixture-node-a-token-not-for-logs"
	r6NodeBToken            = "r6-fixture-node-b-token-not-for-logs"
	r61CFAccessClientID     = "r61-fixture-cf-client-id-not-for-logs"
	r61CFAccessClientSecret = "r61-fixture-cf-client-secret-not-for-logs"
)

func TestR6HubAuthenticationAndTokenPrivacy(t *testing.T) {
	var logs bytes.Buffer
	hub, server := r6HubServer(t, func() time.Time { return time.Date(2026, 8, 31, 3, 0, 0, 0, time.UTC) }, slog.New(slog.NewTextHandler(&logs, nil)))
	defer server.Close()
	health, err := server.Client().Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("healthz status=%d, want 200", health.StatusCode)
	}

	for index, token := range []string{"", "wrong-token", r6NodeAToken} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/v1/nodes", nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("nodes auth case=%d status=%d, want 401", index, response.StatusCode)
		}
		if strings.Contains(string(body), r6OperatorToken) || strings.Contains(string(body), r6NodeAToken) {
			t.Fatal("auth response exposed a token")
		}
	}

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/v1/nodes", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+r6OperatorToken)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("operator nodes status=%d, want 200", response.StatusCode)
	}

	for _, headers := range []http.Header{
		nil,
		{"X-Panewire-Machine-ID": []string{"node-a"}, "Authorization": []string{"Bearer wrong-token"}},
		{"X-Panewire-Machine-ID": []string{"node-a"}, "Authorization": []string{"Bearer " + r6NodeBToken}},
	} {
		conn, handshake, err := websocket.Dial(t.Context(), r6WSURL(server.URL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers})
		if err == nil {
			_ = conn.Close(websocket.StatusNormalClosure, "")
			t.Fatal("unauthenticated agent websocket was accepted")
		}
		if handshake == nil || handshake.StatusCode != http.StatusUnauthorized {
			t.Fatalf("agent handshake=%v err=%v, want 401", handshake, err)
		}
	}
	conn, handshake, err := websocket.Dial(t.Context(), r6WSURL(server.URL, "/v1/events"), &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer wrong-token"}}})
	if err == nil {
		_ = conn.Close(websocket.StatusNormalClosure, "")
		t.Fatal("unauthenticated events websocket was accepted")
	}
	if handshake == nil || handshake.StatusCode != http.StatusUnauthorized {
		t.Fatalf("events handshake=%v err=%v, want 401", handshake, err)
	}
	for _, marker := range []string{r6OperatorToken, r6NodeAToken, r6NodeBToken, r61CFAccessClientID, r61CFAccessClientSecret} {
		if strings.Contains(logs.String(), marker) {
			t.Fatal("hub logs exposed a fixture credential")
		}
	}
	_ = hub
}

func TestR61CFAccessHeadersForHubWebSocketAndStatus(t *testing.T) {
	root := t.TempDir()
	nodeEnv := filepath.Join(root, "hub-node.env")
	operatorEnv := filepath.Join(root, "hub-operator.env")
	cfEnv := filepath.Join(root, "hub-cf.env")
	if err := os.WriteFile(nodeEnv, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(operatorEnv, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+r6OperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfEnv, []byte("CF_ACCESS_CLIENT_ID="+r61CFAccessClientID+"\nCF_ACCESS_CLIENT_SECRET="+r61CFAccessClientSecret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}

	websocketHeaders := make(chan http.Header, 1)
	httpHeaders := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("CF-Access-Client-Id") != r61CFAccessClientID || request.Header.Get("CF-Access-Client-Secret") != r61CFAccessClientSecret {
			http.Error(writer, "Cloudflare Access service token required", http.StatusForbidden)
			return
		}
		switch request.URL.Path {
		case "/v1/agent":
			websocketHeaders <- request.Header.Clone()
			connection, err := websocket.Accept(writer, request, nil)
			if err == nil {
				defer connection.CloseNow()
				ctx, cancel := context.WithTimeout(request.Context(), time.Second)
				defer cancel()
				_, _, _ = connection.Read(ctx)
			}
		case "/v1/nodes":
			if request.Header.Get(hubAuthorizationHeader) != "Bearer "+r6OperatorToken {
				http.Error(writer, "operator token required", http.StatusUnauthorized)
				return
			}
			httpHeaders <- request.Header.Clone()
			_ = json.NewEncoder(writer).Encode(struct {
				Nodes []HubNode `json:"nodes"`
			}{})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	daemon, code, err := newDaemonForCLI([]string{"--hub-url", r6WSURL(server.URL, ""), "--hub-token-env", nodeEnv, "--hub-cf-env", cfEnv}, daemonCLIDeps{AllowInsecureForTests: true})
	if daemon == nil || code != ExitOK || err != nil || daemon.cfg.Hub.Client == nil || daemon.cfg.Hub.Client.cfAccessClientID != r61CFAccessClientID || daemon.cfg.Hub.Client.cfAccessSecret != r61CFAccessClientSecret {
		t.Fatalf("daemon Cloudflare Access flag was not applied: daemon=%v code=%d err=%v", daemon, code, err)
	}
	client, err := buildHubDaemonClient(r6WSURL(server.URL, ""), nodeEnv, cfEnv, nil, false, daemonCLIDeps{AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Run(ctx)
	}()
	select {
	case headers := <-websocketHeaders:
		if headers.Get("CF-Access-Client-Id") != r61CFAccessClientID || headers.Get("CF-Access-Client-Secret") != r61CFAccessClientSecret {
			t.Fatal("hub websocket omitted Cloudflare Access headers")
		}
	case <-time.After(time.Second):
		t.Fatal("Cloudflare Access fake server rejected the hub websocket")
	}

	var stdout, stderr bytes.Buffer
	if code := runHubStatusCLI([]string{"--hub-url", server.URL, "--hub-token-env", operatorEnv, "--hub-cf-env", cfEnv}, &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("hub-status code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	select {
	case headers := <-httpHeaders:
		if headers.Get("CF-Access-Client-Id") != r61CFAccessClientID || headers.Get("CF-Access-Client-Secret") != r61CFAccessClientSecret {
			t.Fatal("hub-status omitted Cloudflare Access headers")
		}
	case <-time.After(time.Second):
		t.Fatal("Cloudflare Access fake server rejected hub-status")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("hub client did not stop")
	}
	for _, marker := range []string{r6NodeAToken, r6OperatorToken, r61CFAccessClientID, r61CFAccessClientSecret} {
		if strings.Contains(stdout.String()+stderr.String(), marker) {
			t.Fatalf("credential marker %q leaked to CLI output", marker)
		}
	}
}

func TestR61CFAccessEnvUsesMode0600RegularFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub-cf.env")
	body := []byte("CF_ACCESS_CLIENT_ID=" + r61CFAccessClientID + "\nCF_ACCESS_CLIENT_SECRET=" + r61CFAccessClientSecret + "\n")
	if err := os.WriteFile(path, body, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHubCFAccessEnv(path); err == nil {
		t.Fatal("non-0600 Cloudflare Access env was accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	env, err := loadHubCFAccessEnv(path)
	if err != nil || env.ClientID != r61CFAccessClientID || env.ClientSecret != r61CFAccessClientSecret {
		t.Fatalf("Cloudflare Access env load failed: err=%v", err)
	}
	symlink := path + ".symlink"
	if err := os.Symlink(path, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHubCFAccessEnv(symlink); err == nil {
		t.Fatal("symlinked Cloudflare Access env was accepted")
	}
}

func TestR6HubPresenceDisconnectAndStaleRecovery(t *testing.T) {
	now := time.Date(2026, 8, 31, 3, 0, 0, 0, time.UTC)
	hub, server := r6HubServer(t, func() time.Time { return now }, nil)
	defer server.Close()

	nodeA := r6DialAgent(t, server.URL, "node-a", r6NodeAToken)
	defer nodeA.Close(websocket.StatusNormalClosure, "")
	nodeB := r6DialAgent(t, server.URL, "node-b", r6NodeBToken)
	defer nodeB.Close(websocket.StatusNormalClosure, "")
	r6Write(t, nodeA, map[string]any{"type": "hello", "machine_id": "node-a", "version": "r6-a"})
	r6Write(t, nodeB, map[string]any{"type": "hello", "machine_id": "node-b", "version": "r6-b"})
	r6Eventually(t, "both nodes connected", func() bool {
		nodes := r6Nodes(t, server)
		return len(nodes) == 2 && r6NodeState(nodes, "node-a") == "connected" && r6NodeState(nodes, "node-b") == "connected"
	})

	if got := r6NodeMeta(r6Nodes(t, server), "node-a")["version"]; got != "r6-a" {
		t.Fatalf("node-a remote_meta version=%q, want r6-a", got)
	}
	if err := nodeB.Close(websocket.StatusNormalClosure, "fixture disconnect"); err != nil {
		t.Fatal(err)
	}
	r6Eventually(t, "node-b immediately disconnected", func() bool {
		return r6NodeState(r6Nodes(t, server), "node-b") == "disconnected"
	})

	now = now.Add(31 * time.Second)
	hub.Sweep()
	if state := r6NodeState(r6Nodes(t, server), "node-a"); state != "stale" {
		t.Fatalf("node-a state=%q, want stale after 30s without keepalive", state)
	}
	r6Write(t, nodeA, map[string]any{"type": "ping"})
	r6Eventually(t, "node-a recovered", func() bool {
		return r6NodeState(r6Nodes(t, server), "node-a") == "connected"
	})
	if got := r6NodeLastPingMS(r6Nodes(t, server), "node-a"); got != 0 {
		t.Fatalf("node-a last_ping_ms=%d, want 0 after ping", got)
	}
}

func TestR6HubEventFanoutAndClosedVocabulary(t *testing.T) {
	now := time.Date(2026, 8, 31, 3, 0, 0, 0, time.UTC)
	hub, server := r6HubServer(t, func() time.Time { return now }, nil)
	defer server.Close()
	node := r6DialAgent(t, server.URL, "node-a", r6NodeAToken)
	defer node.Close(websocket.StatusNormalClosure, "")
	r6Write(t, node, map[string]any{"type": "hello", "machine_id": "node-a", "version": "r6-a", "ignored": true})
	r6Eventually(t, "unknown hello field counted", func() bool { return hub.UnknownMessageCount() == 1 })
	if got := hub.UnknownMessageCount(); got != 1 {
		t.Fatalf("unknown field count=%d, want 1", got)
	}
	if len(r6Nodes(t, server)) != 0 {
		t.Fatal("hello with an unknown field must be ignored, not accepted")
	}
	r6Write(t, node, map[string]any{"type": "hello", "machine_id": "node-a", "version": "r6-a"})
	r6Eventually(t, "node connected after valid hello", func() bool { return r6NodeState(r6Nodes(t, server), "node-a") == "connected" })

	first := r6DialEvents(t, server.URL)
	defer first.Close(websocket.StatusNormalClosure, "")
	second := r6DialEvents(t, server.URL)
	defer second.Close(websocket.StatusNormalClosure, "")
	r6Write(t, node, map[string]any{"type": "event", "kind": "not-a-kind", "payload": map[string]any{"message": "ignored"}})
	r6Eventually(t, "unknown event kind counted", func() bool { return hub.UnknownMessageCount() == 2 })
	if got := hub.UnknownMessageCount(); got != 2 {
		t.Fatalf("unknown kind count=%d, want 2", got)
	}
	r6Write(t, node, map[string]any{"type": "event", "kind": "note", "payload": map[string]any{"message": "fixture event"}})
	for index, subscriber := range []*websocket.Conn{first, second} {
		var event struct {
			MachineID string          `json:"machine_id"`
			Kind      string          `json:"kind"`
			Payload   json.RawMessage `json:"payload"`
		}
		readContext, cancel := context.WithTimeout(t.Context(), time.Second)
		err := wsjson.Read(readContext, subscriber, &event)
		cancel()
		if err != nil {
			t.Fatalf("subscriber %d did not receive event: %v", index, err)
		}
		if event.MachineID != "node-a" || event.Kind != "note" || !strings.Contains(string(event.Payload), "fixture event") {
			t.Fatalf("subscriber %d event=%+v", index, event)
		}
	}
}

func TestR6HubAuthFileLoopbackAndStatusCLI(t *testing.T) {
	root := t.TempDir()
	authPath := filepath.Join(root, "hub-auth.env")
	authBody := "HUB_TOKEN_operator=" + r6OperatorToken + "\nHUB_TOKEN_node-a=" + r6NodeAToken + "\n"
	if err := os.WriteFile(authPath, []byte(authBody), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHubAuthFile(authPath); err == nil {
		t.Fatal("non-0600 hub auth file was accepted")
	}
	if err := os.Chmod(authPath, 0600); err != nil {
		t.Fatal(err)
	}
	tokens, err := loadHubAuthFile(authPath)
	if err != nil || tokens["operator"] != r6OperatorToken || tokens["node-a"] != r6NodeAToken {
		t.Fatalf("hub auth file did not load expected token entries: err=%v", err)
	}
	if hub, _, code, err := newHubServerForCLI([]string{"--listen", "0.0.0.0:9377", "--hub-auth", authPath}, nil); hub != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("non-loopback hub=%v code=%d err=%v", hub, code, err)
	}

	now := time.Date(2026, 8, 31, 3, 0, 0, 0, time.UTC)
	_, server := r6HubServer(t, func() time.Time { return now }, nil)
	defer server.Close()
	node := r6DialAgent(t, server.URL, "node-a", r6NodeAToken)
	defer node.Close(websocket.StatusNormalClosure, "")
	r6Write(t, node, map[string]any{"type": "hello", "machine_id": "node-a", "version": "r6-a"})
	r6Eventually(t, "hub-status fixture node", func() bool { return r6NodeState(r6Nodes(t, server), "node-a") == "connected" })

	operatorEnv := filepath.Join(root, "operator.env")
	if err := os.WriteFile(operatorEnv, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+r6OperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := runHubStatusCLI([]string{"--hub-url", server.URL, "--hub-token-env", operatorEnv}, &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("hub-status code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "MACHINE") || !strings.Contains(stdout.String(), "node-a") || strings.Contains(stdout.String(), r6OperatorToken) {
		t.Fatal("hub-status output was malformed or exposed a token")
	}
}

func TestR6HubClientReconnectBackoffWithoutSleep(t *testing.T) {
	_, server := r6HubServer(t, time.Now, nil)
	defer server.Close()
	subscriber := r6DialEvents(t, server.URL)
	defer subscriber.Close(websocket.StatusNormalClosure, "")

	var attempts int
	waits := make(chan time.Duration, 2)
	warnings := make(chan string, 2)
	client, err := NewHubClient(HubClientConfig{
		URL:                   r6WSURL(server.URL, ""),
		MachineID:             "node-a",
		Token:                 r6NodeAToken,
		AllowInsecureForTests: true,
		PingInterval:          time.Hour,
		Dial: func(ctx context.Context, endpoint string, options *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
			attempts++
			if attempts == 1 {
				return nil, nil, errors.New("fixture hub unavailable")
			}
			return websocket.Dial(ctx, endpoint, options)
		},
		Wait: func(ctx context.Context, delay time.Duration) error {
			select {
			case waits <- delay:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		Warn: func(message string) { warnings <- message },
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
	select {
	case delay := <-waits:
		if delay != time.Second {
			t.Fatalf("first backoff=%s, want 1s", delay)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not request injected backoff")
	}
	select {
	case warning := <-warnings:
		if !strings.Contains(warning, "hub unavailable") || strings.Contains(warning, r6NodeAToken) {
			t.Fatal("hub retry warning was absent or exposed a token")
		}
	case <-time.After(time.Second):
		t.Fatal("client did not warn about the unavailable hub")
	}
	var event struct {
		MachineID string `json:"machine_id"`
		Kind      string `json:"kind"`
	}
	readContext, readCancel := context.WithTimeout(t.Context(), time.Second)
	err = wsjson.Read(readContext, subscriber, &event)
	readCancel()
	if err != nil {
		t.Fatalf("reconnected client did not publish heartbeat: %v", err)
	}
	if attempts != 2 || event.MachineID != "node-a" || event.Kind != "heartbeat" {
		t.Fatalf("attempts=%d event=%+v", attempts, event)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("client did not stop after cancellation")
	}
	if got := nextHubBackoff(32*time.Second, time.Minute); got != time.Minute {
		t.Fatalf("backoff cap=%s, want 1m", got)
	}
}

func TestR6HubUnavailableDoesNotBlockStage1OrStage2(t *testing.T) {
	root := t.TempDir()
	shortRoot, err := os.MkdirTemp("/tmp", "pw-r6-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortRoot) })
	store, err := OpenStore(filepath.Join(root, "panewire.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewHubClient(HubClientConfig{
		URL:                   "ws://fixture.invalid",
		MachineID:             "node-a",
		Token:                 r6NodeAToken,
		AllowInsecureForTests: true,
		Dial: func(context.Context, string, *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
			return nil, nil, errors.New("fixture hub unavailable")
		},
		Wait: func(ctx context.Context, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	probe := &r6Stage2Probe{published: make(chan struct{}, 1), received: make(chan struct{}, 1)}
	daemon := NewDaemon(Config{
		Store:         store,
		SocketPath:    filepath.Join(shortRoot, "daemon.sock"),
		SchemaCommand: []string{"false"},
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Hub:           HubDaemonConfig{Enabled: true, Client: client},
		Stage2: Stage2Config{
			Enabled: true, PollInterval: time.Hour, Publisher: probe, Receiver: probe,
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := daemon.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer daemon.Stop()
	select {
	case <-probe.published:
	case <-time.After(time.Second):
		t.Fatal("stage2 publisher did not run while hub was unavailable")
	}
	select {
	case <-probe.received:
	case <-time.After(time.Second):
		t.Fatal("stage2 receiver did not run while hub was unavailable")
	}
	path := filepath.Join(root, "stage1-ready.txt")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if code := RunCLI([]string{"wait", "--file", path, "--settle", "0s", "--timeout", "1s"}, CLIConfig{SocketPath: daemon.SocketPath()}); code != ExitOK {
		t.Fatalf("stage1 wait code=%d while hub was unavailable", code)
	}
}

func TestR6HubDaemonFlagGate(t *testing.T) {
	root := t.TempDir()
	envPath := filepath.Join(root, "hub-node.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if daemon, code, err := newDaemonForCLI([]string{"--socket", filepath.Join(root, "default.sock")}, daemonCLIDeps{}); daemon == nil || code != ExitOK || err != nil || daemon.cfg.Hub.Enabled {
		t.Fatalf("default hub daemon=%v code=%d err=%v hub=%+v", daemon, code, err, daemon.cfg.Hub)
	}
	if daemon, code, err := newDaemonForCLI([]string{"--hub-url", "ws://fixture.invalid"}, daemonCLIDeps{AllowInsecureForTests: true}); daemon != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("partial hub daemon=%v code=%d err=%v", daemon, code, err)
	}
	if daemon, code, err := newDaemonForCLI([]string{"--hub-url", "ws://fixture.invalid", "--hub-token-env", envPath}, daemonCLIDeps{AllowInsecureForTests: true}); daemon == nil || code != ExitOK || err != nil || !daemon.cfg.Hub.Enabled || daemon.cfg.Hub.Client == nil {
		t.Fatalf("configured hub daemon=%v code=%d err=%v", daemon, code, err)
	}
}

type r6EventWire struct {
	MachineID string          `json:"machine_id"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
}

func r6ReadHubEvent(t *testing.T, subscriber *websocket.Conn, wantKind string) r6EventWire {
	t.Helper()
	var event r6EventWire
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	err := wsjson.Read(ctx, subscriber, &event)
	cancel()
	if err != nil {
		t.Fatalf("read %s event: %v", wantKind, err)
	}
	if event.Kind != wantKind {
		t.Fatalf("event kind=%q, want %q (%+v)", event.Kind, wantKind, event)
	}
	return event
}

type r6Stage2Probe struct {
	published chan struct{}
	received  chan struct{}
}

func (probe *r6Stage2Probe) PublishPending(context.Context) error {
	select {
	case probe.published <- struct{}{}:
	default:
	}
	return nil
}

func (probe *r6Stage2Probe) PollOnce(context.Context) error {
	select {
	case probe.received <- struct{}{}:
	default:
	}
	return nil
}

type r6NodeWire struct {
	MachineID      string            `json:"machine_id"`
	Accepting      bool              `json:"accepting"`
	ConnectedSince time.Time         `json:"connected_since"`
	LastPingMS     int64             `json:"last_ping_ms"`
	RemoteMeta     map[string]string `json:"remote_meta"`
	State          string            `json:"state"`
}

func r6HubServer(t *testing.T, now func() time.Time, logger *slog.Logger) (*HubServer, *httptest.Server) {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{
			"operator": r6OperatorToken,
			"node-a":   r6NodeAToken,
			"node-b":   r6NodeBToken,
		},
		Now:               now,
		StaleAfter:        30 * time.Second,
		KeepaliveInterval: 10 * time.Second,
		Logger:            logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	return hub, server
}

func r6DialAgent(t *testing.T, baseURL, machineID, token string) *websocket.Conn {
	t.Helper()
	headers := make(http.Header)
	headers.Set("X-Panewire-Machine-ID", machineID)
	headers.Set("Authorization", "Bearer "+token)
	connection, response, err := websocket.Dial(t.Context(), r6WSURL(baseURL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatalf("agent dial response=%v err=%v", response, err)
	}
	return connection
}

func r6DialEvents(t *testing.T, baseURL string) *websocket.Conn {
	t.Helper()
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+r6OperatorToken)
	connection, response, err := websocket.Dial(t.Context(), r6WSURL(baseURL, "/v1/events"), &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatalf("events dial response=%v err=%v", response, err)
	}
	return connection
}

func r6Write(t *testing.T, connection *websocket.Conn, value any) {
	t.Helper()
	context, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := wsjson.Write(context, connection, value); err != nil {
		t.Fatal(err)
	}
}

func r6Nodes(t *testing.T, server *httptest.Server) []r6NodeWire {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/v1/nodes", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+r6OperatorToken)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("nodes status=%d", response.StatusCode)
	}
	var body struct {
		Nodes []r6NodeWire `json:"nodes"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body.Nodes
}

func r6NodeState(nodes []r6NodeWire, machineID string) string {
	for _, node := range nodes {
		if node.MachineID == machineID {
			return node.State
		}
	}
	return ""
}

func r6NodeMeta(nodes []r6NodeWire, machineID string) map[string]string {
	for _, node := range nodes {
		if node.MachineID == machineID {
			return node.RemoteMeta
		}
	}
	return nil
}

func r6NodeLastPingMS(nodes []r6NodeWire, machineID string) int64 {
	for _, node := range nodes {
		if node.MachineID == machineID {
			return node.LastPingMS
		}
	}
	return -1
}

func r6WSURL(baseURL, endpoint string) string {
	return "ws" + strings.TrimPrefix(baseURL, "http") + endpoint
}

func r6Eventually(t *testing.T, label string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}
