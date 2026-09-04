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
	MachineID             string
	Token                 string
	CFAccessClientID      string
	CFAccessClientSecret  string
	Accepting             bool
	RelayInjectTimeout    time.Duration
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
	AllowInsecureForTests bool
	Dial                  HubDial
	Wait                  HubWait
	Warn                  func(string)
	// RelayStore is the daemon's --db SQLite store. It is optional for library
	// callers, but the daemon attaches its store before starting the client.
	RelayStore  *Store
	relayInject func(context.Context, string, string) bool // fixture seam

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
	Kind     string
	Payload  json.RawMessage
	relayKey string
}

// HubClient owns a bounded in-memory event queue. It is intentionally not a
// durable relay: Supabase remains responsible for offline stage2 delivery.
type HubClient struct {
	endpoint             string
	machineID            string
	token                string
	cfAccessClientID     string
	cfAccessSecret       string
	accepting            bool
	r19a                 r19aClientState
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
	dial                 HubDial
	wait                 HubWait
	warn                 func(string)
	relayInject          func(context.Context, string, string) bool
	events               chan hubClientEvent
	completedJobs        map[string]uint64
	completedReports     map[string]struct{}
	relayKnown           map[string]struct{}
	relayStore           *Store
	relayStartedAt       time.Time
	relayMu              sync.Mutex
	assignedJobs         map[string]uint64
	assignmentMu         sync.Mutex
	burstHoldsActive     bool
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
	client := &HubClient{
		endpoint: endpoint, machineID: config.MachineID, token: config.Token, cfAccessClientID: config.CFAccessClientID, cfAccessSecret: config.CFAccessClientSecret, accepting: config.Accepting, jobsInboxRoot: config.JobsInboxRoot,
		failoverWakeOn: config.FailoverWakeOn, failoverWakeMAC: wakeMAC, failoverWakeDest: wakeDestination, failoverWakeArmed: wakeRequested,
		burstWakeMAC: burstMAC, burstPoweroffAllowed: config.BurstPoweroffAllowed, burstPoweroff: config.burstPoweroff, burstSeen: make(map[string]time.Time),
		r19a:   newR19aClientState(config),
		checks: cloneHubChecks(config.Checks), execute: config.Execute,
		pingInterval: config.PingInterval, initialBackoff: config.InitialBackoff, maxBackoff: config.MaxBackoff,
		dial: config.Dial, wait: config.Wait, warn: config.Warn, relayInject: config.relayInject, events: make(chan hubClientEvent, 64), completedJobs: make(map[string]uint64), completedReports: make(map[string]struct{}), relayKnown: make(map[string]struct{}), relayStore: config.RelayStore, relayStartedAt: time.Now().UTC(), assignedJobs: make(map[string]uint64),
	}
	if config.RelayStore != nil {
		client.reloadRelaySent()
	}
	return client, nil
}

// setRelayStore binds the client's relay journal to the daemon's --db store
// at daemon startup, after the daemon has opened that database.
func (client *HubClient) setRelayStore(store *Store) {
	client.relayMu.Lock()
	client.relayStore = store
	client.relayStartedAt = time.Now().UTC()
	client.relayMu.Unlock()
	client.reloadRelaySent()
}

func (client *HubClient) reloadRelaySent() {
	client.relayMu.Lock()
	store := client.relayStore
	client.completedReports = make(map[string]struct{})
	client.relayKnown = make(map[string]struct{})
	client.relayMu.Unlock()
	if store == nil {
		return
	}
	records, err := store.LoadRelaySent(context.Background(), time.Now().UTC().Add(-hubJobActiveMaxAge()))
	if err != nil {
		client.warn("relay sent journal unavailable")
		return
	}
	client.relayMu.Lock()
	defer client.relayMu.Unlock()
	for _, record := range records {
		client.relayKnown[record.Key] = struct{}{}
		if record.HubAck == "delivered" || record.HubAck == "unconfirmed" {
			client.completedReports[record.Key] = struct{}{}
		}
	}
}

func (client *HubClient) relaySent(event hubClientEvent) {
	if event.relayKey == "" {
		return
	}
	client.relayMu.Lock()
	store := client.relayStore
	if client.relayKnown == nil {
		client.relayKnown = make(map[string]struct{})
	}
	client.relayKnown[event.relayKey] = struct{}{}
	client.relayMu.Unlock()
	if store != nil && store.RecordRelaySent(context.Background(), event.relayKey, time.Now().UTC()) != nil {
		client.warn("relay sent journal unavailable")
	}
}

func (client *HubClient) relayAcknowledged(message hubOutboundMessage) {
	key := hubRelayKey(message.Kind, message.JobID, message.Epoch, message.ReportPath, message.Reason)
	client.relayMu.Lock()
	if client.completedReports == nil {
		client.completedReports = make(map[string]struct{})
	}
	client.completedReports[key] = struct{}{}
	store := client.relayStore
	client.relayMu.Unlock()
	if store != nil && store.RecordRelayAck(context.Background(), key, message.Status) != nil {
		client.warn("relay acknowledgement journal unavailable")
	}
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
	Status      string
	Kind        string
	ReportPath  string
	Reason      string
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
	// An unacknowledged write is retried once on every new connection; accepted
	// delivery/unconfirmed results remain suppressed from the SQLite journal.
	client.reloadRelaySent()
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
	for _, event := range client.jobCompletionEvents() {
		if err := peer.write(ctx, hubClientWireEvent(event)); err != nil {
			return err
		}
		client.relaySent(event)
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
			if message.Type == "relay.inject" {
				go client.handleRelayInject(ctx, peer, message)
				continue
			}
			if message.Type == "relay.ack" {
				client.relayAcknowledged(message)
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
			client.relaySent(event)
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
				client.relaySent(event)
			}
		}
	}
}

// handleRelayInject is isolated from serve so transport extensions can add
// their own outbound message cases without changing relay acknowledgement.
func (client *HubClient) handleRelayInject(ctx context.Context, peer *hubClientConnection, message hubOutboundMessage) {
	client.respondRelayInject(ctx, peer, message)
}

const defaultRelayInjectTimeout = 10 * time.Second

func relayInjectTimeout(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	if timeout, err := time.ParseDuration(os.Getenv("RELAY_INJECT_TIMEOUT")); err == nil && timeout > 0 {
		return timeout
	}
	return defaultRelayInjectTimeout
}

func (client *HubClient) respondRelayInject(parent context.Context, peer *hubClientConnection, message hubOutboundMessage) {
	inject := client.relayInject
	if inject == nil {
		inject = defaultHubRelayInject
	}
	ctx, cancel := context.WithTimeout(parent, client.r19a.relayInjectTimeout)
	delivered := inject(ctx, message.Pane, message.Text)
	cancel()
	kind := "relay.unconfirmed"
	if delivered {
		kind = "relay.delivered"
	}
	response, _ := json.Marshal(struct {
		JobID string `json:"job_id"`
		Pane  string `json:"pane"`
	}{message.JobID, message.Pane})
	_ = peer.write(parent, hubClientWireEvent(hubClientEvent{Kind: kind, Payload: response}))
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

// hubJobCompletionPayloadForJob carries the claim's agent_label alongside the
// terminal record. The hub needs it to late-register a job it never saw in a
// heartbeat; it is metadata on an already-terminal record, not a claim.
func hubJobCompletionPayloadForJob(job HubActiveJob, eventTime time.Time, replay bool) json.RawMessage {
	payload, _ := json.Marshal(struct {
		JobID          string    `json:"job_id"`
		Epoch          uint64    `json:"epoch"`
		AgentLabel     string    `json:"agent_label,omitempty"`
		OwnerLane      string    `json:"owner_lane,omitempty"`
		Label          string    `json:"label,omitempty"`
		Host           string    `json:"host,omitempty"`
		ReportPath     string    `json:"report_path,omitempty"`
		ReportLastLine string    `json:"report_last_line,omitempty"`
		EventTime      time.Time `json:"event_time,omitempty"`
		Replay         bool      `json:"replay,omitempty"`
	}{job.JobID, job.Epoch, job.AgentLabel, job.OwnerLane, job.Label, job.Host, job.ReportPath, job.ReportLastLine, eventTime, replay})
	return payload
}

func compactHubRelayEventText(value string, normalizeNewlines bool) string {
	value, _ = truncateHubRelayPayloadText(value, normalizeNewlines)
	return value
}

// jobCompletionEvents is the node-side producer for the fenced completion
// contract. It emits only a local terminal-event ID/epoch once per epoch.
func (client *HubClient) jobCompletionEvents() []hubClientEvent {
	completed := scanHubRelayEvents(client.jobsInboxRoot)
	client.assignmentMu.Lock()
	for index := range completed {
		if assigned := client.assignedJobs[completed[index].JobID]; assigned > completed[index].Epoch {
			completed[index].Epoch = assigned
		}
	}
	client.assignmentMu.Unlock()
	events := make([]hubClientEvent, 0, len(completed))
	for _, job := range completed {
		key := hubRelayKey(job.Kind, job.JobID, job.Epoch, job.ReportPath, job.Reason)
		client.relayMu.Lock()
		if client.completedReports == nil {
			client.completedReports = make(map[string]struct{})
		}
		if client.relayKnown == nil {
			client.relayKnown = make(map[string]struct{})
		}
		if _, sent := client.completedReports[key]; sent {
			client.relayMu.Unlock()
			continue
		}
		_, known := client.relayKnown[key]
		startedAt := client.relayStartedAt
		client.completedReports[key] = struct{}{}
		client.relayMu.Unlock()
		if job.Kind == "job.completed" {
			client.completedJobs[job.JobID] = job.Epoch
		}
		replay := !known && !job.EventTime.After(startedAt)
		payload := hubJobCompletionPayloadForJob(job.HubActiveJob, job.EventTime, replay)
		if job.Kind == "job.escalate" || job.Kind == "job.joined" {
			payload, _ = json.Marshal(struct {
				JobID          string    `json:"job_id"`
				Epoch          uint64    `json:"epoch"`
				AgentLabel     string    `json:"agent_label,omitempty"`
				OwnerLane      string    `json:"owner_lane,omitempty"`
				Label          string    `json:"label,omitempty"`
				Host           string    `json:"host,omitempty"`
				ReportPath     string    `json:"report_path,omitempty"`
				ReportLastLine string    `json:"report_last_line,omitempty"`
				Reason         string    `json:"reason"`
				Question       string    `json:"question,omitempty"`
				PR             string    `json:"pr,omitempty"`
				Head           string    `json:"head,omitempty"`
				PaneID         string    `json:"pane_id,omitempty"`
				EventTime      time.Time `json:"event_time,omitempty"`
				Replay         bool      `json:"replay,omitempty"`
			}{job.JobID, job.Epoch, job.AgentLabel, job.OwnerLane, job.Label, job.Host, job.ReportPath, compactHubRelayEventText(job.ReportLastLine, false), compactHubRelayEventText(job.Reason, false), compactHubRelayEventText(job.Question, true), job.PR, job.Head, job.PaneID, job.EventTime, replay})
		}
		events = append(events, hubClientEvent{Kind: job.Kind, Payload: payload, relayKey: key})
	}
	return events
}

func hubRelayKey(kind, jobID string, epoch uint64, reportPath, reason string) string {
	return kind + "\x00" + jobID + "\x00" + strconv.FormatUint(epoch, 10) + "\x00" + reportPath + "\x00" + reason
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
	case "relay.ack":
		if len(fields) < 6 || len(fields) > 7 || json.Unmarshal(fields["status"], &message.Status) != nil || json.Unmarshal(fields["kind"], &message.Kind) != nil || json.Unmarshal(fields["job_id"], &message.JobID) != nil || json.Unmarshal(fields["epoch"], &message.Epoch) != nil || json.Unmarshal(fields["report_path"], &message.ReportPath) != nil || message.Status != "delivered" && message.Status != "unconfirmed" || (message.Kind != "job.completed" && message.Kind != "job.escalate" && message.Kind != "job.joined") || !hubJobIDPattern.MatchString(message.JobID) || message.Epoch == 0 || strings.ContainsAny(message.ReportPath, "\x00\r\n") {
			return hubOutboundMessage{}, false
		}
		if raw, exists := fields["reason"]; exists && (json.Unmarshal(raw, &message.Reason) != nil || strings.ContainsAny(message.Reason, "\x00\r\n")) {
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
