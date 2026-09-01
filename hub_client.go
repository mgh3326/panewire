package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// HubDial and HubWait make retry behavior deterministic in fixture tests. The
// production defaults are websocket.Dial and a context-aware timer.
type HubDial func(context.Context, string, *websocket.DialOptions) (*websocket.Conn, *http.Response, error)
type HubWait func(context.Context, time.Duration) error

// HubClientConfig configures the optional outbound-only panewired sidecar
// channel. A zero config is never started by Daemon, preserving stage1/2's
// historical behavior.
type HubClientConfig struct {
	URL                   string
	MachineID             string
	Token                 string
	CFAccessClientID      string
	CFAccessClientSecret  string
	Accepting             bool
	FailoverWakeOn        string
	FailoverWakeMAC       string
	Checks                []HubCheck
	Execute               HubCheckExecutor
	PingInterval          time.Duration
	InitialBackoff        time.Duration
	MaxBackoff            time.Duration
	AllowInsecureForTests bool
	Dial                  HubDial
	Wait                  HubWait
	Warn                  func(string)

	// failoverWakeDestination is a package-private fixture override. Production
	// always uses the fixed broadcast destination below.
	failoverWakeDestination string
}

// HubDaemonConfig keeps the optional hub process separate from stage2's
// durable transport configuration. A nil Client is a harmless no-op for direct
// library users; the CLI rejects an incomplete requested hub configuration.
type HubDaemonConfig struct {
	Enabled bool
	Client  *HubClient
}

type hubClientEvent struct {
	Kind    string
	Payload json.RawMessage
}

// HubClient owns a bounded in-memory event queue. It is intentionally not a
// durable relay: Supabase remains responsible for offline stage2 delivery.
type HubClient struct {
	endpoint          string
	machineID         string
	token             string
	cfAccessClientID  string
	cfAccessSecret    string
	accepting         bool
	failoverWakeOn    string
	failoverWakeMAC   net.HardwareAddr
	failoverWakeDest  string
	failoverWakeMu    sync.Mutex
	failoverWakeArmed bool
	checks            []HubCheck
	execute           HubCheckExecutor
	pingInterval      time.Duration
	initialBackoff    time.Duration
	maxBackoff        time.Duration
	dial              HubDial
	wait              HubWait
	warn              func(string)
	events            chan hubClientEvent
}

// NewHubClient validates the public base URL and all local inputs without
// opening a connection. Production accepts only wss URLs; ws is fixture-only.
func NewHubClient(config HubClientConfig) (*HubClient, error) {
	endpoint, err := hubWSEndpoint(config.URL, config.AllowInsecureForTests)
	if err != nil || config.MachineID == hubOperatorMachineID || !machineIDPattern.MatchString(config.MachineID) || !validHubToken(config.Token) || !validHubChecks(config.Checks) || (config.CFAccessClientID == "") != (config.CFAccessClientSecret == "") || (config.CFAccessClientID != "" && (!validHubCFAccessValue(config.CFAccessClientID) || !validHubCFAccessValue(config.CFAccessClientSecret))) {
		return nil, errors.New("hub client configuration is invalid")
	}
	wakeRequested := config.FailoverWakeOn != "" || config.FailoverWakeMAC != ""
	var wakeMAC net.HardwareAddr
	if wakeRequested {
		if config.FailoverWakeOn == "" || config.FailoverWakeMAC == "" || config.FailoverWakeOn == hubOperatorMachineID || !machineIDPattern.MatchString(config.FailoverWakeOn) {
			return nil, errors.New("hub client configuration is invalid")
		}
		var wakeErr error
		wakeMAC, wakeErr = parseHubFailoverWakeMAC(config.FailoverWakeMAC)
		if wakeErr != nil {
			return nil, errors.New("hub client configuration is invalid")
		}
	}
	wakeDestination := hubFailoverWakeBroadcastAddress
	if config.failoverWakeDestination != "" {
		address, resolveErr := net.ResolveUDPAddr("udp4", config.failoverWakeDestination)
		if !wakeRequested || resolveErr != nil || address.IP == nil || address.IP.To4() == nil || address.Port <= 0 {
			return nil, errors.New("hub client configuration is invalid")
		}
		wakeDestination = address.String()
	}
	if config.PingInterval <= 0 {
		config.PingInterval = 10 * time.Second
	}
	if config.InitialBackoff <= 0 {
		config.InitialBackoff = time.Second
	}
	if config.MaxBackoff <= 0 {
		config.MaxBackoff = time.Minute
	}
	if config.MaxBackoff < config.InitialBackoff {
		return nil, errors.New("hub client configuration is invalid")
	}
	if config.Dial == nil {
		config.Dial = websocket.Dial
	}
	if config.Wait == nil {
		config.Wait = waitHubRetry
	}
	if config.Warn == nil {
		config.Warn = func(string) {}
	}
	if config.Execute == nil {
		config.Execute = executeHubCheck
	}
	return &HubClient{
		endpoint: endpoint, machineID: config.MachineID, token: config.Token, cfAccessClientID: config.CFAccessClientID, cfAccessSecret: config.CFAccessClientSecret, accepting: config.Accepting,
		failoverWakeOn: config.FailoverWakeOn, failoverWakeMAC: wakeMAC, failoverWakeDest: wakeDestination, failoverWakeArmed: wakeRequested,
		checks: cloneHubChecks(config.Checks), execute: config.Execute,
		pingInterval: config.PingInterval, initialBackoff: config.InitialBackoff, maxBackoff: config.MaxBackoff,
		dial: config.Dial, wait: config.Wait, warn: config.Warn, events: make(chan hubClientEvent, 64),
	}, nil
}

const hubFailoverWakeBroadcastAddress = "255.255.255.255:9"

func parseHubFailoverWakeMAC(raw string) (net.HardwareAddr, error) {
	mac, err := net.ParseMAC(raw)
	if err != nil || len(mac) != 6 {
		return nil, errors.New("invalid failover wake MAC")
	}
	return append(net.HardwareAddr(nil), mac...), nil
}

func hubWakeMagicPacket(mac net.HardwareAddr) []byte {
	if len(mac) != 6 {
		return nil
	}
	packet := make([]byte, 6+16*len(mac))
	for index := 0; index < 6; index++ {
		packet[index] = 0xff
	}
	for index := 6; index < len(packet); index += len(mac) {
		copy(packet[index:], mac)
	}
	return packet
}

func sendHubFailoverWakePacket(ctx context.Context, destination string, mac net.HardwareAddr) error {
	if ctx.Err() != nil || len(mac) != 6 {
		return errors.New("failover wake unavailable")
	}
	address, err := net.ResolveUDPAddr("udp4", destination)
	if err != nil || address.IP == nil || address.IP.To4() == nil || address.Port <= 0 {
		return errors.New("failover wake unavailable")
	}
	connection, err := net.DialUDP("udp4", nil, address)
	if err != nil {
		return errors.New("failover wake unavailable")
	}
	defer connection.Close()
	deadline := time.Now().Add(5 * time.Second)
	if contextDeadline, exists := ctx.Deadline(); exists && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := connection.SetWriteDeadline(deadline); err != nil {
		return errors.New("failover wake unavailable")
	}
	packet := hubWakeMagicPacket(mac)
	if written, err := connection.Write(packet); err != nil || written != len(packet) {
		return errors.New("failover wake unavailable")
	}
	return nil
}

func validHubCFAccessValue(value string) bool {
	if value == "" || len(value) > 512 {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func hubWSEndpoint(raw string, allowInsecureForTests bool) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", errors.New("invalid hub URL")
	}
	switch parsed.Scheme {
	case "wss":
	case "ws":
		if !allowInsecureForTests {
			return "", errors.New("invalid hub URL")
		}
	default:
		return "", errors.New("invalid hub URL")
	}
	parsed.Path = "/v1/agent"
	return parsed.String(), nil
}

func waitHubRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Publish queues one closed-vocabulary event without blocking daemon work. A
// full relay queue drops only that optional event and emits a constant warning.
func (client *HubClient) Publish(kind string, payload any) bool {
	if !knownHubEventKind(kind) {
		return false
	}
	encoded, err := json.Marshal(payload)
	if err != nil || len(encoded) == 0 || !json.Valid(encoded) {
		return false
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(encoded, &object) != nil || object == nil {
		return false
	}
	event := hubClientEvent{Kind: kind, Payload: append(json.RawMessage(nil), encoded...)}
	select {
	case client.events <- event:
		return true
	default:
		client.warn("hub event queue full; dropping optional event")
		return false
	}
}

// Run reconnects until its context is canceled. Any hub error is isolated to
// this goroutine: it never propagates into stage1 or stage2 loops.
func (client *HubClient) Run(ctx context.Context) {
	backoff := client.initialBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		headers := make(http.Header)
		headers.Set(hubMachineIDHeader, client.machineID)
		headers.Set(hubAuthorizationHeader, "Bearer "+client.token)
		if client.cfAccessClientID != "" {
			headers.Set("CF-Access-Client-Id", client.cfAccessClientID)
			headers.Set("CF-Access-Client-Secret", client.cfAccessSecret)
		}
		connection, _, err := client.dial(ctx, client.endpoint, &websocket.DialOptions{HTTPHeader: headers})
		if err == nil {
			backoff = client.initialBackoff
			err = client.serve(ctx, connection)
			_ = connection.CloseNow()
		}
		if ctx.Err() != nil {
			return
		}
		// Neither error nor endpoint data is included: a transport response can
		// reflect a credential or bearer value through an intermediary.
		client.warn("hub unavailable; retrying")
		if client.wait(ctx, backoff) != nil {
			return
		}
		backoff = nextHubBackoff(backoff, client.maxBackoff)
	}
}

func nextHubBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

type hubClientConnection struct {
	connection *websocket.Conn
	writeMu    sync.Mutex
}

type hubOutboundMessage struct {
	Type      string
	Machine   string
	Phase     string
	EmittedAt time.Time
}

func (client *HubClient) serve(ctx context.Context, connection *websocket.Conn) error {
	connection.SetReadLimit(hubMaxMessageBytes)
	peer := &hubClientConnection{connection: connection}
	if err := peer.write(ctx, struct {
		Type      string `json:"type"`
		MachineID string `json:"machine_id"`
		Version   string `json:"version"`
		Accepting bool   `json:"accepting,omitempty"`
	}{Type: "hello", MachineID: client.machineID, Version: "panewired-r10", Accepting: client.accepting}); err != nil {
		return err
	}
	if err := peer.write(ctx, hubClientWireEvent(client.heartbeatEvent(ctx))); err != nil {
		return err
	}
	readErrors := make(chan error, 1)
	go func() {
		for {
			messageType, payload, err := connection.Read(ctx)
			if err != nil {
				select {
				case readErrors <- err:
				case <-ctx.Done():
				}
				return
			}
			if messageType != websocket.MessageText {
				continue
			}
			message, ok := parseHubOutbound(payload)
			if !ok {
				continue
			}
			switch message.Type {
			case "ping":
				if err := peer.write(ctx, hubOutbound{Type: "pong"}); err != nil {
					select {
					case readErrors <- err:
					case <-ctx.Done():
					}
					return
				}
			case "failover":
				client.handleHubFailover(ctx, message)
			}
		}
	}()
	ticker := time.NewTicker(client.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErrors:
			return err
		case event := <-client.events:
			if err := peer.write(ctx, hubClientWireEvent(event)); err != nil {
				return err
			}
		case <-ticker.C:
			if err := peer.write(ctx, hubOutbound{Type: "ping"}); err != nil {
				return err
			}
			if err := peer.write(ctx, hubClientWireEvent(client.heartbeatEvent(ctx))); err != nil {
				return err
			}
		}
	}
}

func (client *HubClient) heartbeatEvent(ctx context.Context) hubClientEvent {
	payload, _ := json.Marshal(hubHeartbeatPayload{Status: "alive", Checks: runHubChecks(ctx, client.checks, client.execute)})
	return hubClientEvent{Kind: "heartbeat", Payload: payload}
}

func hubClientWireEvent(event hubClientEvent) struct {
	Type    string          `json:"type"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
} {
	return struct {
		Type    string          `json:"type"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Type: "event", Kind: event.Kind, Payload: event.Payload}
}

func parseHubOutbound(payload []byte) (hubOutboundMessage, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || fields == nil {
		return hubOutboundMessage{}, false
	}
	rawType, exists := fields["type"]
	if !exists {
		return hubOutboundMessage{}, false
	}
	var message hubOutboundMessage
	if json.Unmarshal(rawType, &message.Type) != nil {
		return hubOutboundMessage{}, false
	}
	switch message.Type {
	case "ping", "pong":
		if len(fields) != 1 {
			return hubOutboundMessage{}, false
		}
	case "failover":
		var emittedAt string
		if len(fields) != 4 || json.Unmarshal(fields["machine"], &message.Machine) != nil || json.Unmarshal(fields["phase"], &message.Phase) != nil || json.Unmarshal(fields["emitted_at"], &emittedAt) != nil || !parseHubFailoverEmittedAt(emittedAt, &message.EmittedAt) || !machineIDPattern.MatchString(message.Machine) || !validHubFailoverPhase(message.Phase) {
			return hubOutboundMessage{}, false
		}
	default:
		return hubOutboundMessage{}, false
	}
	return message, true
}

// parseHubFailoverEmittedAt accepts only the canonical RFC3339 UTC rendering
// produced by the hub. Its value is audit metadata, never a wake eligibility
// input.
func parseHubFailoverEmittedAt(value string, destination *time.Time) bool {
	if destination == nil || !strings.HasSuffix(value, "Z") {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.IsZero() || parsed.Format(time.RFC3339Nano) != value {
		return false
	}
	*destination = parsed.UTC()
	return true
}

func (client *HubClient) handleHubFailover(ctx context.Context, message hubOutboundMessage) {
	if client.failoverWakeOn == "" || message.Machine != client.failoverWakeOn || !validHubFailoverPhase(message.Phase) {
		return
	}
	client.failoverWakeMu.Lock()
	if message.Phase == hubFailoverPhaseUp {
		client.failoverWakeArmed = true
		client.failoverWakeMu.Unlock()
		return
	}
	if !client.failoverWakeArmed {
		client.failoverWakeMu.Unlock()
		return
	}
	client.failoverWakeArmed = false
	mac := append(net.HardwareAddr(nil), client.failoverWakeMAC...)
	destination := client.failoverWakeDest
	client.failoverWakeMu.Unlock()
	if err := sendHubFailoverWakePacket(ctx, destination, mac); err != nil {
		client.warn("failover wake unavailable")
	}
}

func (peer *hubClientConnection) write(ctx context.Context, value any) error {
	peer.writeMu.Lock()
	defer peer.writeMu.Unlock()
	writeContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return wsjson.Write(writeContext, peer.connection, value)
}
