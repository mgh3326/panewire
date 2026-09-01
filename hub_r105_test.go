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
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestHubR105AgentStreamReceivesFixedFailoverEvent(t *testing.T) {
	clock := time.Date(2026, 9, 1, 9, 0, 0, 0, time.UTC)
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

	receiver := r6DialAgent(t, server.URL, "node-b", r6NodeBToken)
	defer receiver.Close(websocket.StatusNormalClosure, "")
	r6Write(t, receiver, map[string]any{"type": "hello", "machine_id": "node-b", "version": "r10.5-receiver"})
	watched := r6DialAgent(t, server.URL, "node-a", r6NodeAToken)
	defer watched.Close(websocket.StatusNormalClosure, "")
	r6Write(t, watched, map[string]any{"type": "hello", "machine_id": "node-a", "version": "r10.5-watched"})
	r6Eventually(t, "agent stream fixtures", func() bool { return len(r6Nodes(t, server)) == 2 })

	clock = clock.Add(time.Second)
	hub.Sweep() // stale
	clock = clock.Add(2 * time.Second)
	hub.Sweep() // first post-grace observation
	clock = clock.Add(time.Second)
	hub.Sweep() // second post-grace observation
	if event := r105ReadAgentFailover(t, receiver); event.Machine != "node-a" || event.Phase != hubFailoverPhaseDown {
		t.Fatalf("agent stream event=%+v", event)
	}
}

func TestHubR105FailoverWakeSendsOneMagicPacketAndRearms(t *testing.T) {
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	helloSeen := make(chan struct{})
	allowTarget := make(chan struct{})
	release := make(chan struct{})
	var allowTargetOnce sync.Once
	var releaseOnce sync.Once
	allowTargetEvents := func() { allowTargetOnce.Do(func() { close(allowTarget) }) }
	releaseEvents := func() { releaseOnce.Do(func() { close(release) }) }
	defer allowTargetEvents()
	defer releaseEvents()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/agent" {
			http.NotFound(writer, request)
			return
		}
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		var hello struct {
			Type      string `json:"type"`
			MachineID string `json:"machine_id"`
		}
		readContext, cancel := context.WithTimeout(request.Context(), time.Second)
		err = wsjson.Read(readContext, connection, &hello)
		cancel()
		if err != nil || hello.Type != "hello" || hello.MachineID != "node-b" {
			return
		}
		emittedAt := time.Date(2026, 9, 1, 9, 0, 0, 0, time.UTC)
		select {
		case <-helloSeen:
		default:
			close(helloSeen)
		}
		if wsjson.Write(request.Context(), connection, hubFailoverEvent{Type: "failover", Machine: "node-b", Phase: hubFailoverPhaseDown, EmittedAt: emittedAt}) != nil {
			return
		}
		select {
		case <-allowTarget:
		case <-request.Context().Done():
			return
		}
		down := hubFailoverEvent{Type: "failover", Machine: "node-a", Phase: hubFailoverPhaseDown, EmittedAt: emittedAt}
		if wsjson.Write(request.Context(), connection, down) != nil || wsjson.Write(request.Context(), connection, down) != nil {
			return
		}
		select {
		case <-release:
		case <-request.Context().Done():
			return
		}
		if wsjson.Write(request.Context(), connection, hubFailoverEvent{Type: "failover", Machine: "node-a", Phase: hubFailoverPhaseUp, EmittedAt: emittedAt}) != nil || wsjson.Write(request.Context(), connection, down) != nil {
			return
		}
		<-request.Context().Done()
	}))
	defer server.Close()

	const macText = "02:1a:2b:3c:4d:5e"
	client, err := NewHubClient(HubClientConfig{
		URL:                     r6WSURL(server.URL, ""),
		MachineID:               "node-b",
		Token:                   r6NodeBToken,
		FailoverWakeOn:          "node-a",
		FailoverWakeMAC:         macText,
		PingInterval:            time.Hour,
		AllowInsecureForTests:   true,
		failoverWakeDestination: listener.LocalAddr().String(),
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
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("failover wake hub client did not stop")
		}
	}()
	select {
	case <-helloSeen:
	case <-time.After(time.Second):
		t.Fatal("fake hub did not receive hello")
	}
	r105ExpectNoMagicPacket(t, listener) // a wrong machine must not select an action target

	wantMAC, err := net.ParseMAC(macText)
	if err != nil {
		t.Fatal(err)
	}
	allowTargetEvents()
	r105AssertMagicPacket(t, r105ReadMagicPacket(t, listener), wantMAC)
	r105ExpectNoMagicPacket(t, listener) // a debounce removal sends the second down here

	releaseEvents()
	r105AssertMagicPacket(t, r105ReadMagicPacket(t, listener), wantMAC)
}

func TestHubR105FailoverParserRejectsPayloadCarrier(t *testing.T) {
	if _, ok := parseHubOutbound([]byte(`{"type":"failover","machine":"node-a","phase":"down","emitted_at":"2026-09-01T09:00:00Z","payload":{"argv":["sh"]}}`)); ok {
		t.Fatal("failover payload carrier was accepted")
	}
}

func TestHubR11FailoverEmittedAtContract(t *testing.T) {
	emittedAt := time.Date(2026, 9, 1, 9, 0, 0, 123456789, time.UTC)
	wire, err := json.Marshal(hubFailoverEvent{Type: "failover", Machine: "node-a", Phase: hubFailoverPhaseDown, EmittedAt: emittedAt})
	if err != nil {
		t.Fatal(err)
	}
	parsed, ok := parseHubOutbound(wire)
	if !ok || !parsed.EmittedAt.Equal(emittedAt) || parsed.EmittedAt.Location() != time.UTC {
		t.Fatalf("failover emitted_at did not round-trip: parsed=%+v ok=%t", parsed, ok)
	}
	if _, ok := parseHubOutbound([]byte(`{"type":"failover","machine":"node-a","phase":"down"}`)); ok {
		t.Fatal("failover without emitted_at was accepted")
	}
	zeroWire, err := json.Marshal(hubFailoverEvent{Type: "failover", Machine: "node-a", Phase: hubFailoverPhaseDown})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := parseHubOutbound(zeroWire); ok {
		t.Fatal("failover with zero emitted_at was accepted")
	}
}

func TestHubR105FailoverWakeFlagsFailClosed(t *testing.T) {
	root := t.TempDir()
	envPath := filepath.Join(root, "node.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=node-b\nHUB_TOKEN="+r6NodeBToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	base := []string{"--socket", filepath.Join(root, "daemon.sock"), "--hub-url", "ws://fixture.invalid", "--hub-token-env", envPath}
	for _, fixture := range []struct {
		name string
		args []string
	}{
		{name: "wake-on only", args: []string{"--failover-wake-on", "node-a"}},
		{name: "wake-mac only", args: []string{"--failover-wake-mac", "02:1a:2b:3c:4d:5e"}},
		{name: "invalid mac", args: []string{"--failover-wake-on", "node-a", "--failover-wake-mac", "not-a-mac"}},
		{name: "operator target", args: []string{"--failover-wake-on", "operator", "--failover-wake-mac", "02:1a:2b:3c:4d:5e"}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			daemon, code, err := newDaemonForCLI(append(append([]string(nil), base...), fixture.args...), daemonCLIDeps{AllowInsecureForTests: true})
			if daemon != nil || code != ExitConditionInvalid || err == nil {
				t.Fatalf("unsafe failover wake configuration accepted: daemon=%v code=%d err=%v", daemon, code, err)
			}
		})
	}
	daemon, code, err := newDaemonForCLI(append(base, "--failover-wake-on", "node-a", "--failover-wake-mac", "02:1a:2b:3c:4d:5e"), daemonCLIDeps{AllowInsecureForTests: true})
	if daemon == nil || code != ExitOK || err != nil || daemon.cfg.Hub.Client == nil || daemon.cfg.Hub.Client.failoverWakeOn != "node-a" || daemon.cfg.Hub.Client.failoverWakeDest != hubFailoverWakeBroadcastAddress || !daemon.cfg.Hub.Client.failoverWakeArmed {
		t.Fatalf("fixed failover wake configuration was not armed: daemon=%v code=%d err=%v", daemon, code, err)
	}
}

func r105ReadMagicPacket(t *testing.T, listener *net.UDPConn) []byte {
	t.Helper()
	if err := listener.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 256)
	count, _, err := listener.ReadFromUDP(buffer)
	if err != nil {
		t.Fatalf("read magic packet: %v", err)
	}
	return append([]byte(nil), buffer[:count]...)
}

func r105ReadAgentFailover(t *testing.T, connection *websocket.Conn) r10FailoverWire {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		var fields map[string]json.RawMessage
		if err := wsjson.Read(ctx, connection, &fields); err != nil {
			t.Fatalf("read agent failover event: %v", err)
		}
		var eventType string
		if json.Unmarshal(fields["type"], &eventType) == nil && eventType == "ping" {
			continue
		}
		if len(fields) != 4 {
			t.Fatalf("agent failover event has non-closed shape: %v", fields)
		}
		var event r10FailoverWire
		var emittedAt string
		if err := json.Unmarshal(fields["type"], &event.Type); err != nil || event.Type != "failover" || json.Unmarshal(fields["machine"], &event.Machine) != nil || json.Unmarshal(fields["phase"], &event.Phase) != nil || json.Unmarshal(fields["emitted_at"], &emittedAt) != nil || !parseHubFailoverEmittedAt(emittedAt, &event.EmittedAt) || !machineIDPattern.MatchString(event.Machine) || !validHubFailoverPhase(event.Phase) {
			t.Fatalf("invalid agent failover event: fields=%v err=%v", fields, err)
		}
		return event
	}
}

func r105ExpectNoMagicPacket(t *testing.T, listener *net.UDPConn) {
	t.Helper()
	if err := listener.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 256)
	if _, _, err := listener.ReadFromUDP(buffer); err == nil {
		t.Fatal("duplicate failover down sent a second magic packet")
	} else if networkError, ok := err.(net.Error); !ok || !networkError.Timeout() {
		t.Fatalf("unexpected UDP read error: %v", err)
	}
}

func r105AssertMagicPacket(t *testing.T, packet []byte, mac net.HardwareAddr) {
	t.Helper()
	if len(packet) != 6+16*len(mac) {
		t.Fatalf("magic packet length=%d, want %d", len(packet), 6+16*len(mac))
	}
	if !bytes.Equal(packet[:6], bytes.Repeat([]byte{0xff}, 6)) {
		t.Fatalf("magic packet prefix=%x, want six ff bytes", packet[:6])
	}
	for index := 6; index < len(packet); index += len(mac) {
		if !bytes.Equal(packet[index:index+len(mac)], mac) {
			t.Fatalf("magic packet MAC at byte %d=%x, want %x", index, packet[index:index+len(mac)], mac)
		}
	}
}
