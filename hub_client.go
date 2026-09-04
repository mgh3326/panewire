package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
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
	URLs                  []string
	MachineID             string
	Token                 string
	CFAccessClientID      string
	CFAccessClientSecret  string
	Accepting             bool
	JobsInboxRoot         string
	FailoverWakeOn        string
	FailoverWakeMAC       string
	BurstWakeMAC          string
	BurstPoweroffAllowed  bool
	Checks                []HubCheck
	Execute               HubCheckExecutor
	PingInterval          time.Duration
	InitialBackoff        time.Duration
	MaxBackoff            time.Duration
	PreferRetry           time.Duration
	Version               string
	UpdateHTTPClient      *http.Client
	ExecutablePath        string // fixture seam; production uses os.Executable.
	Restart               func() // fixture seam; production exits for its supervisor.
	AllowInsecureForTests bool
	Dial                  HubDial
	Wait                  HubWait
	Warn                  func(string)
	relayInject           func(context.Context, string, string) bool // fixture seam

	// failoverWakeDestination is a package-private fixture override. Production
	// always uses the fixed broadcast destination below.
	failoverWakeDestination string
	burstPoweroff           func(context.Context) error // fixture seam; production is fixed sudo -n poweroff.
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
	endpoint             string
	endpoints            []string
	machineID            string
	token                string
	cfAccessClientID     string
	cfAccessSecret       string
	accepting            bool
	jobsInboxRoot        string
	failoverWakeOn       string
	failoverWakeMAC      net.HardwareAddr
	failoverWakeDest     string
	failoverWakeMu       sync.Mutex
	failoverWakeArmed    bool
	burstWakeMAC         net.HardwareAddr
	burstPoweroffAllowed bool
	burstPoweroff        func(context.Context) error
	burstMu              sync.Mutex
	burstSeen            map[string]time.Time
	checks               []HubCheck
	execute              HubCheckExecutor
	pingInterval         time.Duration
	initialBackoff       time.Duration
	maxBackoff           time.Duration
	preferRetry          time.Duration
	version              string
	updateHTTPClient     *http.Client
	executablePath       string
	restart              func()
	dial                 HubDial
	wait                 HubWait
	warn                 func(string)
	relayInject          func(context.Context, string, string) bool
	events               chan hubClientEvent
	completedJobs        map[string]uint64
	completedReports     map[string]struct{}
	assignedJobs         map[string]uint64
	assignmentMu         sync.Mutex
	burstHoldsActive     bool
}

// NewHubClient validates the public base URL and all local inputs without
// opening a connection. Production accepts only wss URLs; ws is fixture-only.
func NewHubClient(config HubClientConfig) (*HubClient, error) {
	rawURLs := append([]string(nil), config.URLs...)
	if config.URL != "" {
		rawURLs = append([]string{config.URL}, rawURLs...)
	}
	if len(rawURLs) == 0 {
		return nil, errors.New("hub client configuration is invalid")
	}
	endpoints := make([]string, 0, len(rawURLs))
	seen := make(map[string]struct{}, len(rawURLs))
	for _, raw := range rawURLs {
		for _, item := range strings.Split(raw, ",") {
			endpoint, err := hubWSEndpoint(strings.TrimSpace(item), config.AllowInsecureForTests)
			if err != nil {
				return nil, errors.New("hub client configuration is invalid")
			}
			if _, duplicate := seen[endpoint]; duplicate {
				continue
			}
			seen[endpoint] = struct{}{}
			endpoints = append(endpoints, endpoint)
		}
	}
	if len(endpoints) == 0 || config.MachineID == hubOperatorMachineID || !machineIDPattern.MatchString(config.MachineID) || !validHubToken(config.Token) || !validHubChecks(config.Checks) || (config.CFAccessClientID == "") != (config.CFAccessClientSecret == "") || (config.CFAccessClientID != "" && (!validHubCFAccessValue(config.CFAccessClientID) || !validHubCFAccessValue(config.CFAccessClientSecret))) {
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
	burstMACText := config.BurstWakeMAC
	if burstMACText == "" && wakeRequested {
		burstMACText = config.FailoverWakeMAC
	}
	var burstMAC net.HardwareAddr
	if burstMACText != "" {
		var burstErr error
		burstMAC, burstErr = parseHubFailoverWakeMAC(burstMACText)
		if burstErr != nil {
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
	if config.PreferRetry <= 0 {
		config.PreferRetry = 10 * time.Minute
		if raw := os.Getenv("HUB_PREFER_RETRY"); raw != "" {
			if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
				config.PreferRetry = parsed
			}
		}
	}
	if config.Version == "" {
		config.Version = "panewired-r19b"
	}
	if !hubVersionPattern.MatchString(config.Version) {
		return nil, errors.New("hub client configuration is invalid")
	}
	if config.UpdateHTTPClient == nil {
		config.UpdateHTTPClient = &http.Client{Timeout: 15 * time.Second}
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
	if config.burstPoweroff == nil {
		config.burstPoweroff = executeHubBurstPoweroff
	}
	if config.Restart == nil {
		config.Restart = func() { os.Exit(0) }
	}
	return &HubClient{
		endpoint: endpoints[0], endpoints: endpoints, machineID: config.MachineID, token: config.Token, cfAccessClientID: config.CFAccessClientID, cfAccessSecret: config.CFAccessClientSecret, accepting: config.Accepting, jobsInboxRoot: config.JobsInboxRoot,
		failoverWakeOn: config.FailoverWakeOn, failoverWakeMAC: wakeMAC, failoverWakeDest: wakeDestination, failoverWakeArmed: wakeRequested,
		burstWakeMAC: burstMAC, burstPoweroffAllowed: config.BurstPoweroffAllowed, burstPoweroff: config.burstPoweroff, burstSeen: make(map[string]time.Time),
		checks: cloneHubChecks(config.Checks), execute: config.Execute,
		pingInterval: config.PingInterval, initialBackoff: config.InitialBackoff, maxBackoff: config.MaxBackoff, preferRetry: config.PreferRetry, version: config.Version, updateHTTPClient: config.UpdateHTTPClient, executablePath: config.ExecutablePath, restart: config.Restart,
		dial: config.Dial, wait: config.Wait, warn: config.Warn, relayInject: config.relayInject, events: make(chan hubClientEvent, 64), completedJobs: make(map[string]uint64), completedReports: make(map[string]struct{}), assignedJobs: make(map[string]uint64),
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
		connection, endpoint, err := client.dialAny(ctx)
		if err == nil {
			backoff = client.initialBackoff
			client.endpoint = endpoint
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

func (client *HubClient) dialHeaders(endpoint string) http.Header {
	headers := make(http.Header)
	headers.Set(hubMachineIDHeader, client.machineID)
	headers.Set(hubAuthorizationHeader, "Bearer "+client.token)
	parsed, _ := url.Parse(endpoint)
	// A tailnet endpoint authenticates with the hub token only. Cloudflare
	// Access credentials are deliberately never sent to a numeric tailnet peer.
	if client.cfAccessClientID != "" && (parsed == nil || !isTailnetIPv4(net.ParseIP(parsed.Hostname()))) {
		headers.Set("CF-Access-Client-Id", client.cfAccessClientID)
		headers.Set("CF-Access-Client-Secret", client.cfAccessSecret)
	}
	return headers
}

func (client *HubClient) dialAny(ctx context.Context) (*websocket.Conn, string, error) {
	var last error
	for _, endpoint := range client.endpoints {
		connection, _, err := client.dial(ctx, endpoint, &websocket.DialOptions{HTTPHeader: client.dialHeaders(endpoint)})
		if err == nil {
			return connection, endpoint, nil
		}
		last = err
	}
	return nil, "", last
}

// dialPreferred is non-disruptive: it never touches the current socket unless
// a better endpoint has completed its TCP/WebSocket handshake.
func (client *HubClient) dialPreferred(ctx context.Context) (*websocket.Conn, string, bool) {
	current := 0
	for index, endpoint := range client.endpoints {
		if endpoint == client.endpoint {
			current = index
			break
		}
	}
	for _, endpoint := range client.endpoints[:current] {
		connection, _, err := client.dial(ctx, endpoint, &websocket.DialOptions{HTTPHeader: client.dialHeaders(endpoint)})
		if err == nil {
			return connection, endpoint, true
		}
	}
	return nil, "", false
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
	Type        string
	Machine     string
	Phase       string
	EmittedAt   time.Time
	WakeMAC     string
	JobID       string
	Epoch       uint64
	HoldsActive bool
	Pane        string
	Text        string
	RequestID   string
	URL         string
	SHA256      string
	Version     string
}

func defaultHubRelayInject(ctx context.Context, pane, text string) bool {
	if exec.CommandContext(ctx, "herdr", "agent", "prompt", pane, text).Run() != nil {
		return false
	}
	out, err := exec.CommandContext(ctx, "herdr", "agent", "read", pane, "--lines", "10").Output()
	if err != nil || !bytes.Contains(out, []byte("[Pasted text")) {
		return err == nil
	}
	// The relay-handoff contract permits exactly one return only when the
	// pasted-text chip proves the prompt remained in the composer.
	if exec.CommandContext(ctx, "herdr", "agent", "send-keys", pane, "return").Run() != nil {
		return false
	}
	out, err = exec.CommandContext(ctx, "herdr", "agent", "read", pane, "--lines", "10").Output()
	return err == nil && !bytes.Contains(out, []byte("[Pasted text"))
}

func (client *HubClient) serve(ctx context.Context, connection *websocket.Conn) error {
	connection.SetReadLimit(hubMaxMessageBytes)
	peer := &hubClientConnection{connection: connection}
	if err := peer.write(ctx, struct {
		Type      string `json:"type"`
		MachineID string `json:"machine_id"`
		Version   string `json:"version"`
		Accepting bool   `json:"accepting,omitempty"`
	}{Type: "hello", MachineID: client.machineID, Version: client.version, Accepting: client.accepting}); err != nil {
		return err
	}
	if err := peer.write(ctx, hubClientWireEvent(client.heartbeatEvent(ctx))); err != nil {
		return err
	}
	for _, event := range client.jobCompletionEvents() {
		if err := peer.write(ctx, hubClientWireEvent(event)); err != nil {
			return err
		}
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
			case "burst":
				client.handleHubBurst(ctx, message)
			case "job.revoked":
				if err := writeHubRevocation(client.jobsInboxRoot, hubJobRevokedEvent{Type: message.Type, JobID: message.JobID, Epoch: message.Epoch}); err != nil {
					client.warn("job revocation local write unavailable")
				} else if err := peer.write(ctx, hubClientWireEvent(hubClientEvent{Kind: "job.revocation.ack", Payload: hubJobCompletionPayload(message.JobID, message.Epoch)})); err != nil {
					select {
					case readErrors <- err:
					case <-ctx.Done():
					}
					return
				}
			case "job.assigned":
				client.assignmentMu.Lock()
				client.assignedJobs[message.JobID] = message.Epoch
				client.assignmentMu.Unlock()
			case "burst.holds":
				client.burstMu.Lock()
				client.burstHoldsActive = message.HoldsActive
				client.burstMu.Unlock()
			case "relay.inject":
				inject := client.relayInject
				if inject == nil {
					inject = defaultHubRelayInject
				}
				kind := "relay.unconfirmed"
				if inject(ctx, message.Pane, message.Text) {
					kind = "relay.delivered"
				}
				response, _ := json.Marshal(struct {
					JobID string `json:"job_id"`
					Pane  string `json:"pane"`
				}{message.JobID, message.Pane})
				if err := peer.write(ctx, hubClientWireEvent(hubClientEvent{Kind: kind, Payload: response})); err != nil {
					select {
					case readErrors <- err:
					case <-ctx.Done():
					}
					return
				}
			case "update.available":
				go client.handleHubUpdate(message)
			case "quota.request":
				go client.handleHubQuota(ctx, peer, message)
			}
		}
	}()
	ticker := time.NewTicker(client.pingInterval)
	defer ticker.Stop()
	prefer := time.NewTicker(client.preferRetry)
	defer prefer.Stop()
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
			for _, event := range client.jobCompletionEvents() {
				if err := peer.write(ctx, hubClientWireEvent(event)); err != nil {
					return err
				}
			}
		case <-prefer.C:
			attemptContext, cancel := context.WithTimeout(ctx, 5*time.Second)
			candidate, endpoint, switched := client.dialPreferred(attemptContext)
			cancel()
			if switched {
				// Returning hands the already-open candidate to Run; the old
				// connection stays live until this point, so a failed preference
				// probe cannot cause an outage.
				client.endpoint = endpoint
				_ = connection.CloseNow()
				return client.serve(ctx, candidate)
			}
		}
	}
}

func (client *HubClient) heartbeatEvent(ctx context.Context) hubClientEvent {
	active := scanHubActiveJobs(client.jobsInboxRoot)
	client.assignmentMu.Lock()
	for i := range active {
		if epoch := client.assignedJobs[active[i].JobID]; epoch > active[i].Epoch {
			active[i].Epoch = epoch
		}
	}
	client.assignmentMu.Unlock()
	client.burstMu.Lock()
	holdsActive := client.burstHoldsActive
	client.burstMu.Unlock()
	heartbeat := hubHeartbeatPayload{Status: "alive", Checks: runHubChecks(ctx, client.checks, client.execute), ActiveJobs: active, HoldsActive: holdsActive}
	if load, err := collectHubHostLoad(ctx); err == nil {
		heartbeat.HostLoad = &load
	}
	payload, _ := json.Marshal(heartbeat)
	return hubClientEvent{Kind: "heartbeat", Payload: payload}
}

func hubJobCompletionPayload(jobID string, epoch uint64) json.RawMessage {
	payload, _ := json.Marshal(struct {
		JobID string `json:"job_id"`
		Epoch uint64 `json:"epoch"`
	}{JobID: jobID, Epoch: epoch})
	return payload
}

func hubJobCompletionPayloadForJob(job HubActiveJob) json.RawMessage {
	payload, _ := json.Marshal(struct {
		JobID          string `json:"job_id"`
		Epoch          uint64 `json:"epoch"`
		OwnerLane      string `json:"owner_lane,omitempty"`
		Label          string `json:"label,omitempty"`
		Host           string `json:"host,omitempty"`
		ReportPath     string `json:"report_path,omitempty"`
		ReportLastLine string `json:"report_last_line,omitempty"`
	}{job.JobID, job.Epoch, job.OwnerLane, job.Label, job.Host, job.ReportPath, job.ReportLastLine})
	return payload
}

// jobCompletionEvents is the node-side producer for the fenced completion
// contract. It emits only a local terminal-event ID/epoch once per epoch.
func (client *HubClient) jobCompletionEvents() []hubClientEvent {
	completed := scanHubCompletedJobs(client.jobsInboxRoot)
	client.assignmentMu.Lock()
	for index := range completed {
		if assigned := client.assignedJobs[completed[index].JobID]; assigned > completed[index].Epoch {
			completed[index].Epoch = assigned
		}
	}
	client.assignmentMu.Unlock()
	events := make([]hubClientEvent, 0, len(completed))
	for _, job := range completed {
		key := job.JobID + "\x00" + strconv.FormatUint(job.Epoch, 10) + "\x00" + job.ReportPath
		if client.completedReports == nil {
			client.completedReports = make(map[string]struct{})
		}
		if _, sent := client.completedReports[key]; sent {
			continue
		}
		client.completedJobs[job.JobID] = job.Epoch
		client.completedReports[key] = struct{}{}
		events = append(events, hubClientEvent{Kind: "job.completed", Payload: hubJobCompletionPayloadForJob(job)})
	}
	return events
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
	case "burst":
		var emittedAt string
		if json.Unmarshal(fields["machine"], &message.Machine) != nil || json.Unmarshal(fields["phase"], &message.Phase) != nil || json.Unmarshal(fields["emitted_at"], &emittedAt) != nil || !parseHubFailoverEmittedAt(emittedAt, &message.EmittedAt) || !machineIDPattern.MatchString(message.Machine) || !validHubBurstPhase(message.Phase) {
			return hubOutboundMessage{}, false
		}
		if message.Phase == hubFailoverPhaseUp {
			if len(fields) != 5 || json.Unmarshal(fields["wake_mac"], &message.WakeMAC) != nil {
				return hubOutboundMessage{}, false
			}
			mac, err := parseHubFailoverWakeMAC(message.WakeMAC)
			if err != nil || len(mac) != 6 {
				return hubOutboundMessage{}, false
			}
		} else if len(fields) != 4 {
			return hubOutboundMessage{}, false
		}
	case "job.revoked":
		if len(fields) != 3 || json.Unmarshal(fields["job_id"], &message.JobID) != nil || json.Unmarshal(fields["epoch"], &message.Epoch) != nil || !hubJobIDPattern.MatchString(message.JobID) || message.Epoch == 0 {
			return hubOutboundMessage{}, false
		}
	case "job.assigned":
		if len(fields) != 3 || json.Unmarshal(fields["job_id"], &message.JobID) != nil || json.Unmarshal(fields["epoch"], &message.Epoch) != nil || !hubJobIDPattern.MatchString(message.JobID) || message.Epoch == 0 {
			return hubOutboundMessage{}, false
		}
	case "burst.holds":
		if len(fields) != 2 || json.Unmarshal(fields["holds_active"], &message.HoldsActive) != nil {
			return hubOutboundMessage{}, false
		}
	case "relay.inject":
		if len(fields) != 4 || json.Unmarshal(fields["job_id"], &message.JobID) != nil || json.Unmarshal(fields["pane"], &message.Pane) != nil || json.Unmarshal(fields["text"], &message.Text) != nil || !hubJobIDPattern.MatchString(message.JobID) || message.Pane == "" || len(message.Pane) > 128 || !validHubNoteText(message.Text) {
			return hubOutboundMessage{}, false
		}
	case "update.available":
		if len(fields) != 4 || json.Unmarshal(fields["version"], &message.Version) != nil || json.Unmarshal(fields["sha256"], &message.SHA256) != nil || json.Unmarshal(fields["url"], &message.URL) != nil || !hubVersionPattern.MatchString(message.Version) || !validHubSHA256(message.SHA256) || !validHubUpdateURL(message.URL) {
			return hubOutboundMessage{}, false
		}
	case "quota.request":
		var tool string
		if len(fields) != 3 || json.Unmarshal(fields["request_id"], &message.RequestID) != nil || json.Unmarshal(fields["tool"], &tool) != nil || !validHubRequestID(message.RequestID) || tool != "scopefuel" {
			return hubOutboundMessage{}, false
		}
	default:
		return hubOutboundMessage{}, false
	}
	return message, true
}

func (client *HubClient) handleHubBurst(ctx context.Context, message hubOutboundMessage) {
	if !validHubBurstPhase(message.Phase) {
		return
	}
	key := message.Phase + ":" + message.EmittedAt.Format(time.RFC3339Nano)
	client.burstMu.Lock()
	if _, seen := client.burstSeen[key]; seen {
		client.burstMu.Unlock()
		return
	}
	client.burstSeen[key] = message.EmittedAt
	mac := append(net.HardwareAddr(nil), client.burstWakeMAC...)
	poweroffAllowed, poweroff := client.burstPoweroffAllowed, client.burstPoweroff
	client.burstMu.Unlock()
	if message.Phase == hubFailoverPhaseUp {
		policyMAC, err := parseHubFailoverWakeMAC(message.WakeMAC)
		if err != nil || len(mac) != 6 || !bytes.Equal(mac, policyMAC) {
			client.warn("burst wake unavailable")
			return
		}
		if err := sendHubFailoverWakePacket(ctx, hubFailoverWakeBroadcastAddress, policyMAC); err != nil {
			client.warn("burst wake unavailable")
		}
		return
	}
	if !poweroffAllowed {
		return
	}
	// The hub is authoritative for idle evaluation, but a target that has
	// already received a live hold must also reject a delayed down event. This
	// prevents an in-flight or future emitter from defeating the hold locally.
	client.burstMu.Lock()
	holdsActive := client.burstHoldsActive
	client.burstMu.Unlock()
	if holdsActive {
		return
	}
	if err := poweroff(ctx); err != nil {
		client.warn("burst poweroff unavailable")
	}
}

func executeHubBurstPoweroff(ctx context.Context) error {
	command := exec.CommandContext(ctx, "sudo", "-n", "/usr/sbin/poweroff")
	command.Stdout, command.Stderr = io.Discard, io.Discard
	return command.Run()
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
