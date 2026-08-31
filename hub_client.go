package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/sentinel"
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
	SentinelEnabled       bool
	PingInterval          time.Duration
	InitialBackoff        time.Duration
	MaxBackoff            time.Duration
	AllowInsecureForTests bool
	Dial                  HubDial
	Wait                  HubWait
	Warn                  func(string)
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
	endpoint         string
	machineID        string
	token            string
	cfAccessClientID string
	cfAccessSecret   string
	sentinelEnabled  bool
	pingInterval     time.Duration
	initialBackoff   time.Duration
	maxBackoff       time.Duration
	dial             HubDial
	wait             HubWait
	warn             func(string)
	events           chan hubClientEvent
}

// NewHubClient validates the public base URL and all local inputs without
// opening a connection. Production accepts only wss URLs; ws is fixture-only.
func NewHubClient(config HubClientConfig) (*HubClient, error) {
	endpoint, err := hubWSEndpoint(config.URL, config.AllowInsecureForTests)
	if err != nil || config.MachineID == hubOperatorMachineID || !machineIDPattern.MatchString(config.MachineID) || !validHubToken(config.Token) || (config.CFAccessClientID == "") != (config.CFAccessClientSecret == "") || (config.CFAccessClientID != "" && (!validHubCFAccessValue(config.CFAccessClientID) || !validHubCFAccessValue(config.CFAccessClientSecret))) {
		return nil, errors.New("hub client configuration is invalid")
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
	return &HubClient{
		endpoint: endpoint, machineID: config.MachineID, token: config.Token, cfAccessClientID: config.CFAccessClientID, cfAccessSecret: config.CFAccessClientSecret, sentinelEnabled: config.SentinelEnabled,
		pingInterval: config.PingInterval, initialBackoff: config.InitialBackoff, maxBackoff: config.MaxBackoff,
		dial: config.Dial, wait: config.Wait, warn: config.Warn, events: make(chan hubClientEvent, 64),
	}, nil
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

// PublishSentinelAlert is gated even if a caller holds a HubClient reference:
// a node without --sentinel can never emit the sentinel_alert vocabulary.
func (client *HubClient) PublishSentinelAlert(alert sentinel.Alert) bool {
	if !client.sentinelEnabled {
		return false
	}
	checks := make(map[string]sentinel.CheckStatus, len(alert.Checks))
	for name, status := range alert.Checks {
		checks[name] = status
	}
	return client.Publish("sentinel_alert", struct {
		Recovery  bool                            `json:"recovery"`
		MachineID string                          `json:"machine_id"`
		Reason    string                          `json:"reason"`
		LastSeen  time.Time                       `json:"last_seen"`
		Checks    map[string]sentinel.CheckStatus `json:"checks"`
	}{Recovery: alert.Recovery, MachineID: alert.MachineID, Reason: alert.Reason, LastSeen: alert.LastSeen.UTC(), Checks: checks})
}

// Run reconnects until its context is canceled. Any hub error is isolated to
// this goroutine: it never propagates into stage1, stage2, or sentinel loops.
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

func (client *HubClient) serve(ctx context.Context, connection *websocket.Conn) error {
	connection.SetReadLimit(hubMaxMessageBytes)
	peer := &hubClientConnection{connection: connection}
	if err := peer.write(ctx, struct {
		Type      string `json:"type"`
		MachineID string `json:"machine_id"`
		Version   string `json:"version"`
	}{Type: "hello", MachineID: client.machineID, Version: "panewired-r6"}); err != nil {
		return err
	}
	if err := peer.write(ctx, hubClientWireEvent(hubClientEvent{Kind: "heartbeat", Payload: json.RawMessage(`{"status":"alive"}`)})); err != nil {
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
			if kind, ok := parseHubOutbound(payload); ok && kind == "ping" {
				if err := peer.write(ctx, hubOutbound{Type: "pong"}); err != nil {
					select {
					case readErrors <- err:
					case <-ctx.Done():
					}
					return
				}
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
			if err := peer.write(ctx, hubClientWireEvent(hubClientEvent{Kind: "heartbeat", Payload: json.RawMessage(`{"status":"alive"}`)})); err != nil {
				return err
			}
		}
	}
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

func parseHubOutbound(payload []byte) (string, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || len(fields) != 1 {
		return "", false
	}
	rawType, exists := fields["type"]
	if !exists {
		return "", false
	}
	var kind string
	if json.Unmarshal(rawType, &kind) != nil || (kind != "ping" && kind != "pong") {
		return "", false
	}
	return kind, true
}

func (peer *hubClientConnection) write(ctx context.Context, value any) error {
	peer.writeMu.Lock()
	defer peer.writeMu.Unlock()
	writeContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return wsjson.Write(writeContext, peer.connection, value)
}
