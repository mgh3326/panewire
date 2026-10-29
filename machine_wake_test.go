package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const nodeWakeFixtureMAC = "02:00:00:00:00:09"

func nodeWakeHub(t *testing.T, now *time.Time) *HubServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wake.json")
	if err := os.WriteFile(path, []byte(`{"targets":[{"machine":"desktop","via":"rpi"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "rpi": r6NodeBToken, "desktop": "desktop-node-wake-fixture-token"}, WakeConfigPath: path, Now: func() time.Time { return *now }, KeepaliveInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func TestHubWakeConfigParse(t *testing.T) {
	targets, err := ParseHubWakeConfig([]byte(`{"targets":[{"machine":"desktop","via":"rpi"},{"machine":"other","via":"rpi"}]}`))
	if err != nil || len(targets) != 2 || targets["desktop"].Via != "rpi" {
		t.Fatalf("valid wake config rejected: targets=%+v err=%v", targets, err)
	}
	for name, raw := range map[string]string{
		"duplicate":    `{"targets":[{"machine":"desktop","via":"rpi"},{"machine":"desktop","via":"other"}]}`,
		"self via":     `{"targets":[{"machine":"desktop","via":"desktop"}]}`,
		"operator tgt": `{"targets":[{"machine":"operator","via":"rpi"}]}`,
		"operator via": `{"targets":[{"machine":"desktop","via":"operator"}]}`,
		"bad machine":  `{"targets":[{"machine":"DE SK","via":"rpi"}]}`,
		"empty":        `{"targets":[]}`,
		"unknown key":  `{"targets":[{"machine":"desktop","via":"rpi","mac":"02:00:00:00:00:01"}]}`,
		"trailing":     `{"targets":[{"machine":"desktop","via":"rpi"}]} {}`,
	} {
		if _, err := ParseHubWakeConfig([]byte(raw)); err == nil {
			t.Fatalf("%s wake config accepted", name)
		}
	}
}

func TestNodeWakeConfigParse(t *testing.T) {
	targets, err := ParseNodeWakeConfig([]byte(`{"targets":[{"machine":"desktop","mac":"` + nodeWakeFixtureMAC + `","broadcast":"10.0.0.255:9"}]}`))
	if err != nil || len(targets) != 1 || targets["desktop"].Broadcast != "10.0.0.255:9" || targets["desktop"].MAC != nodeWakeFixtureMAC {
		t.Fatalf("valid wake targets rejected: targets=%+v err=%v", targets, err)
	}
	for name, raw := range map[string]string{
		"bad mac":      `{"targets":[{"machine":"desktop","mac":"not-a-mac","broadcast":"10.0.0.255:9"}]}`,
		"ipv6 mac":     `{"targets":[{"machine":"desktop","mac":"02:00:00:00:00:00:09","broadcast":"10.0.0.255:9"}]}`,
		"no port":      `{"targets":[{"machine":"desktop","mac":"02:00:00:00:00:09","broadcast":"10.0.0.255"}]}`,
		"hostname dst": `{"targets":[{"machine":"desktop","mac":"02:00:00:00:00:09","broadcast":"localhost:9"}]}`,
		"ipv6 dst":     `{"targets":[{"machine":"desktop","mac":"02:00:00:00:00:09","broadcast":"[::1]:9"}]}`,
		"operator tgt": `{"targets":[{"machine":"operator","mac":"02:00:00:00:00:09","broadcast":"10.0.0.255:9"}]}`,
		"duplicate":    `{"targets":[{"machine":"desktop","mac":"02:00:00:00:00:09","broadcast":"10.0.0.255:9"},{"machine":"desktop","mac":"02:00:00:00:00:10","broadcast":"10.0.0.255:9"}]}`,
		"unknown key":  `{"targets":[{"machine":"desktop","mac":"02:00:00:00:00:09","broadcast":"10.0.0.255:9","extra":1}]}`,
	} {
		if _, err := ParseNodeWakeConfig([]byte(raw)); err == nil {
			t.Fatalf("%s wake targets accepted", name)
		}
	}
}

func TestParseHubOutboundWakeShape(t *testing.T) {
	emitted := "2026-09-01T09:00:00Z"
	parsed, ok := parseHubOutbound([]byte(`{"type":"wake","machine":"desktop","emitted_at":"` + emitted + `"}`))
	if !ok || parsed.Type != "wake" || parsed.Machine != "desktop" || parsed.EmittedAt.Format(time.RFC3339) != emitted {
		t.Fatalf("wake event parse=%+v ok=%t", parsed, ok)
	}
	for name, raw := range map[string]string{
		"extra field":     `{"type":"wake","machine":"desktop","emitted_at":"` + emitted + `","mac":"02:00:00:00:00:09"}`,
		"missing emit":    `{"type":"wake","machine":"desktop"}`,
		"bad emit":        `{"type":"wake","machine":"desktop","emitted_at":"not-a-time"}`,
		"operator":        `{"type":"wake","machine":"op erator","emitted_at":"` + emitted + `"}`,
		"payload carrier": `{"type":"wake","machine":"desktop","emitted_at":"` + emitted + `","argv":["sh"]}`,
	} {
		if _, ok := parseHubOutbound([]byte(raw)); ok {
			t.Fatalf("%s wake event accepted", name)
		}
	}
}

func TestHubClientHandleHubWake(t *testing.T) {
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var warnings []string
	client := &HubClient{
		wakeTargets: map[string]NodeWakeTarget{
			"desktop": {Machine: "desktop", MAC: nodeWakeFixtureMAC, Broadcast: listener.LocalAddr().String()},
		},
		wakeSeen: make(map[string]time.Time),
		warn:     func(message string) { warnings = append(warnings, message) },
	}
	emittedAt := time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC)
	message := hubOutboundMessage{Type: "wake", Machine: "desktop", EmittedAt: emittedAt}
	client.handleHubWake(context.Background(), message)
	wantMAC, err := net.ParseMAC(nodeWakeFixtureMAC)
	if err != nil {
		t.Fatal(err)
	}
	r105AssertMagicPacket(t, r105ReadMagicPacket(t, listener), wantMAC)
	// Same (machine, emitted_at) is deduped; a fresh emitted_at sends again.
	client.handleHubWake(context.Background(), message)
	r105ExpectNoMagicPacket(t, listener)
	client.handleHubWake(context.Background(), hubOutboundMessage{Type: "wake", Machine: "desktop", EmittedAt: emittedAt.Add(time.Nanosecond)})
	r105AssertMagicPacket(t, r105ReadMagicPacket(t, listener), wantMAC)
	// A machine outside this node's allowlist sends nothing and warns.
	client.handleHubWake(context.Background(), hubOutboundMessage{Type: "wake", Machine: "other", EmittedAt: emittedAt})
	r105ExpectNoMagicPacket(t, listener)
	if len(warnings) != 1 {
		t.Fatalf("warnings=%v", warnings)
	}
}

func TestHubNodeWakeDispatchAndConfirm(t *testing.T) {
	now := time.Date(2026, 9, 5, 2, 0, 0, 0, time.UTC)
	hub := nodeWakeHub(t, &now)
	waker := &hubAgent{wakes: make(chan hubWakeRequestEvent, 2)}
	hub.connect("rpi", "nw", "fixture", waker, false)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	done := make(chan *http.Response, 1)
	go func() {
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/nodes/wake", bytes.NewReader([]byte(`{"machine":"desktop","timeout":"5s"}`)))
		request.Header.Set(hubAuthorizationHeader, "Bearer "+r6OperatorToken)
		response, _ := server.Client().Do(request)
		done <- response
	}()
	select {
	case event := <-waker.wakes:
		if event.Type != "wake" || event.Machine != "desktop" || event.EmittedAt.IsZero() {
			t.Fatalf("unexpected wake event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not dispatch to the configured waker")
	}
	hub.connect("desktop", "nw", "fixture", &hubAgent{}, false)
	var response *http.Response
	select {
	case response = <-done:
	case <-time.After(time.Second):
		t.Fatal("request did not await target heartbeat")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("wake status=%d", response.StatusCode)
	}
	var result hubWakeResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil || result.Machine != "desktop" || result.Via != "rpi" || result.State != "connected" || result.AlreadyConnected {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestHubNodeWakeAuthAndFailures(t *testing.T) {
	now := time.Date(2026, 9, 5, 3, 0, 0, 0, time.UTC)
	hub := nodeWakeHub(t, &now)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	post := func(token, body string) *http.Response {
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/nodes/wake", bytes.NewReader([]byte(body)))
		request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	// A node credential is never an operator credential.
	response := post(r6NodeBToken, `{"machine":"desktop"}`)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("node token accepted: %d", response.StatusCode)
	}
	// Unknown wake target.
	response = post(r6OperatorToken, `{"machine":"unlisted"}`)
	var reason struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(response.Body).Decode(&reason)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound || reason.Reason != "unknown_target" {
		t.Fatalf("unknown target status=%d reason=%+v", response.StatusCode, reason)
	}
	// Known target, waker not connected.
	response = post(r6OperatorToken, `{"machine":"desktop"}`)
	_ = json.NewDecoder(response.Body).Decode(&reason)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || reason.Reason != "waker_unavailable" {
		t.Fatalf("waker unavailable status=%d reason=%+v", response.StatusCode, reason)
	}
	// Already-connected target answers immediately without dispatching.
	hub.connect("desktop", "nw", "fixture", &hubAgent{}, false)
	response = post(r6OperatorToken, `{"machine":"desktop"}`)
	var result hubWakeResult
	_ = json.NewDecoder(response.Body).Decode(&result)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || !result.AlreadyConnected {
		t.Fatalf("already-connected status=%d result=%+v", response.StatusCode, result)
	}
	// Malformed bodies stay 400.
	for _, body := range []string{`{"machine":"desktop","timeout":"nope"}`, `{"machine":"desktop","argv":["sh"]}`, `{"machine":"operator"}`} {
		response = post(r6OperatorToken, body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("body %s status=%d", body, response.StatusCode)
		}
	}
}

func TestNodeWakeCLI(t *testing.T) {
	now := time.Date(2026, 9, 5, 4, 0, 0, 0, time.UTC)
	hub := nodeWakeHub(t, &now)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	hub.connect("desktop", "nw", "fixture", &hubAgent{}, false)
	env := filepath.Join(t.TempDir(), "operator.env")
	if err := os.WriteFile(env, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+r6OperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := runNodeWakeCLI([]string{"--hub-url", server.URL, "--hub-token-env", env, "--timeout", "5s", "desktop"}, &out, &errOut, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
	if code != ExitOK || !bytes.Contains(out.Bytes(), []byte("already_connected")) {
		t.Fatalf("wake code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
	// A node credential is rejected before any request is issued.
	nodeEnv := filepath.Join(t.TempDir(), "node.env")
	if err := os.WriteFile(nodeEnv, []byte("HUB_MACHINE_ID=desktop\nHUB_TOKEN=desktop-node-wake-fixture-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	code = runNodeWakeCLI([]string{"--hub-url", server.URL, "--hub-token-env", nodeEnv, "desktop"}, &out, &errOut, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
	if code != ExitConditionInvalid {
		t.Fatalf("node credential accepted: code=%d", code)
	}
	if code = runNodeCLI([]string{"bogus"}, &out, &errOut, hubCLIDeps{}); code != ExitUsage {
		t.Fatalf("subcommand code=%d", code)
	}
	if code = runNodeWakeCLI([]string{"--hub-url", server.URL, "--hub-token-env", env, "desktop", "extra"}, &out, &errOut, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}); code != ExitUsage {
		t.Fatalf("extra arg code=%d", code)
	}
}
