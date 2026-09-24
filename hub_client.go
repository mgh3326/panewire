package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
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
	URL                  string
	URLs                 []string
	MachineID            string
	Token                string
	CFAccessClientID     string
	CFAccessClientSecret string
	Accepting            bool
	RelayInjectTimeout   time.Duration
	JobsInboxRoot        string
	FailoverWakeOn       string
	FailoverWakeMAC      string
	BurstWakeMAC         string
	BurstPoweroffAllowed bool
	Checks               []HubCheck
	Execute              HubCheckExecutor
	PingInterval         time.Duration
	InitialBackoff       time.Duration
	MaxBackoff           time.Duration
	PreferRetry          time.Duration
	Version              string
	UpdateHTTPClient     *http.Client
	// UpdateRepository pins the GitHub owner/repo whose release assets this
	// node installs; empty selects hubUpdateDefaultRepository.
	UpdateRepository      string
	ExecutablePath        string // fixture seam; production uses os.Executable.
	Restart               func() // fixture seam; production exits for its supervisor.
	AllowInsecureForTests bool
	Dial                  HubDial
	Wait                  HubWait
	Warn                  func(string)
	// SpawnConfigPath is the local, operator-provisioned job.spawn policy.
	// It is injectable so tests never consult a user's real configuration.
	SpawnConfigPath     string
	relayCommand        relayCommandRunner                               // fixture seam for agent get/wait.
	relayInject         func(context.Context, string, string) bool       // fixture seam
	hostLoadCollector   func(context.Context) (HubHostLoad, error)       // fixture seam
	hostMemoryCollector func(context.Context) (*HubHostMemory, error)    // fixture seam
	quotaCollector      func(context.Context) (*HubQuotaSnapshot, error) // fixture seam
	sessionCollector    func(context.Context) ([]HerdrAgentState, error) // fixture seam

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
	// relayKey names the outbox row this event stands for, and relayPending
	// says the row is still waiting for its send stamp. sent_at is written by
	// commitRelaySent once the write has actually left the node, so an event
	// that never reached the wire carries no stamp at all.
	relayKey     relayOutboxKey
	relayPending bool
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
	preferRetry          time.Duration
	version              string
	updateHTTPClient     *http.Client
	updateRepository     string
	executablePath       string
	restart              func()
	dial                 HubDial
	wait                 HubWait
	warn                 func(string)
	relayInject          func(context.Context, string, string) bool
	relayInjectVerdict   func(context.Context, string, string, []string) relayInjectResult // #547 three-way fixture seam; relayInject wins when set
	hostLoadCollector    func(context.Context) (HubHostLoad, error)
	hostMemoryCollector  func(context.Context) (*HubHostMemory, error)
	quotaCollector       func(context.Context) (*HubQuotaSnapshot, error)
	sessionCollector     func(context.Context) ([]HerdrAgentState, error)
	events               chan hubClientEvent
	completedJobs        map[string]uint64
	completedReports     map[string]struct{}
	// relayInflight holds the keys selected for a send that has not been
	// stamped yet. It keeps the scan and `panewire emit` from offering the
	// same record twice while it is in a write queue.
	relayInflight map[string]struct{}
	outbox        *Store
	outboxMu      sync.Mutex
	// now is the outbox retry clock. Tests pin it; production leaves it nil.
	now               func() time.Time
	assignedJobs      map[string]uint64
	assignmentMu      sync.Mutex
	burstHoldsActive  bool
	updateMu          sync.Mutex
	updateInFlight    bool
	panesAlive        panesAliveFunc
	panesAliveMu      sync.Mutex
	sessionMu         sync.Mutex
	spawnConfigPath   string
	spawnMu           sync.Mutex
	spawnSeenMu       sync.Mutex
	spawnSeen         map[string]struct{}
	spawnTimeoutExtra time.Duration // fixture seam; production is 60 seconds.
	busyRelayMu       sync.Mutex
	busyRelay         *relayBusyManager
	relayCommand      relayCommandRunner   // fixture seam for agent get/wait.
	relayEmitter      func(hubClientEvent) // fixture/connection-owned writer.
	relayRecvSeqMu    sync.Mutex
	relayRecvSeq      int64 // assigned synchronously by the hub read loop.
	idleWakeMu        sync.Mutex
	idleWake          *idleWakeManager
	stallBeatMu       sync.Mutex
	stallBeat         func() *hubStallBeatPayload
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
		config.Version = "panewire-dev"
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
	if config.quotaCollector == nil && !config.AllowInsecureForTests {
		config.quotaCollector = collectHubQuota
	}
	if config.burstPoweroff == nil {
		config.burstPoweroff = executeHubBurstPoweroff
	}
	if config.Restart == nil {
		config.Restart = func() { os.Exit(0) }
	}
	if config.UpdateRepository == "" {
		config.UpdateRepository = hubUpdateDefaultRepository
	}
	return &HubClient{
		endpoint: endpoints[0], endpoints: endpoints, machineID: config.MachineID, token: config.Token, cfAccessClientID: config.CFAccessClientID, cfAccessSecret: config.CFAccessClientSecret, accepting: config.Accepting, jobsInboxRoot: config.JobsInboxRoot, spawnConfigPath: defaultHubSpawnConfigPath(config.SpawnConfigPath), spawnSeen: make(map[string]struct{}), spawnTimeoutExtra: 60 * time.Second,
		failoverWakeOn: config.FailoverWakeOn, failoverWakeMAC: wakeMAC, failoverWakeDest: wakeDestination, failoverWakeArmed: wakeRequested,
		burstWakeMAC: burstMAC, burstPoweroffAllowed: config.BurstPoweroffAllowed, burstPoweroff: config.burstPoweroff, burstSeen: make(map[string]time.Time),
		r19a:   newR19aClientState(config),
		checks: cloneHubChecks(config.Checks), execute: config.Execute,
		pingInterval: config.PingInterval, initialBackoff: config.InitialBackoff, maxBackoff: config.MaxBackoff, preferRetry: config.PreferRetry, version: config.Version, updateHTTPClient: config.UpdateHTTPClient, updateRepository: config.UpdateRepository, executablePath: config.ExecutablePath, restart: config.Restart,
		dial: config.Dial, wait: config.Wait, warn: config.Warn, relayInject: config.relayInject, relayCommand: config.relayCommand, hostLoadCollector: config.hostLoadCollector, hostMemoryCollector: config.hostMemoryCollector, quotaCollector: config.quotaCollector, sessionCollector: config.sessionCollector, events: make(chan hubClientEvent, 64), completedJobs: make(map[string]uint64), completedReports: make(map[string]struct{}), assignedJobs: make(map[string]uint64),
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
	Type            string
	Machine         string
	Phase           string
	EmittedAt       time.Time
	WakeMAC         string
	JobID           string
	Epoch           uint64
	HoldsActive     bool
	Pane            string
	Text            string
	DeliverPolicy   string
	Kind            string
	ReportPath      string
	Reason          string
	EventID         int64
	Lane            string
	ProducerEventID string
	IdleWakeEventID string
	Eligible        bool
	RequestID       string
	URL             string
	SHA256          string
	Version         string
	CWDKey          string
	BriefInline     string
	Args            []string
	WaitSeconds     int
	RecvSeq         int64 // local read-loop order; never part of the wire shape.
}

func defaultHubRelayInject(ctx context.Context, pane, text string) bool {
	outcome := defaultHubRelayInjectVerdict(ctx, pane, text, nil).Outcome
	return outcome == relayInjectDelivered || outcome == relayInjectQueued
}

// relayInjectOutcome is the three-way result of one relay inject (#547,
// hk:doc decision/2026-09-21/task547-esc-relay-contract). A bool could only
// say delivered or retry, and a retry re-injects -- which duplicates the
// message whenever it is in fact already sitting in the pane.
type relayInjectOutcome int

const (
	relayInjectDelivered relayInjectOutcome = iota
	// relayInjectRetryable: herdr rejected the send, so nothing was typed
	// and re-injecting cannot duplicate. A verdict the verification reads
	// cannot prove is never retryable -- the text may already be in the
	// pane (#683).
	relayInjectRetryable
	// relayInjectMaybeInPane: the message may already be in the pane
	// (composer, queue, or transcript). Never inject it again.
	relayInjectMaybeInPane
	// relayInjectQueued: the harness accepted the message into its
	// submit-later queue while it was busy -- claude and codex show their
	// queue banner, devin its own queue state. No return keypress -- that
	// would cut the running turn -- and no retry: the harness submits its
	// queue when the turn ends.
	relayInjectQueued
)

type relayInjectResult struct {
	Outcome relayInjectOutcome
	Harness string
	// Evidence names the read source and rule behind Outcome
	// ("recent-unwrapped:marker_echo", "queued_banner", ...), prefixed by
	// the phase that produced it ("presend:", "after_return:").
	Evidence string
}

// defaultHubRelayInjectVerdict injects text into pane. members are the texts
// of the held items batched into text (nil for a direct inject); the devin
// path checks each one before typing.
func defaultHubRelayInjectVerdict(ctx context.Context, pane, text string, members []string) relayInjectResult {
	harness := relayInjectHarness(ctx, pane)
	result := relayInjectForHarness(ctx, pane, harness, text, members)
	if result.Outcome == relayInjectMaybeInPane {
		return result
	}
	// The harness was read before the send so devin could take its own path.
	// If the pane's agent changed while this inject ran, the verdict was
	// made with the wrong harness's rules and proves nothing.
	if after := relayInjectHarness(ctx, pane); harness != "" && after != "" && !strings.EqualFold(after, harness) {
		return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: after, Evidence: "harness_changed:" + harness + "->" + after}
	}
	return result
}

func relayInjectForHarness(ctx context.Context, pane, harness, text string, members []string) relayInjectResult {
	if strings.EqualFold(harness, "devin") {
		return devinRelayInject(ctx, pane, text, members)
	}
	// #683: verify before typing, on every attempt -- a hub replay of an
	// already-landed row arrives as a fresh inject, and pasting a text that
	// is already on the pane is the duplicate the #650 report measured. The
	// visible read doubles as the #626 'before' snapshot for the
	// return-once rule: a chip seen afterwards is provably this inject's
	// own only if this read had no chip.
	presend := relayReadPane(ctx, pane)
	if !presend.anyOK {
		// Both reads failed: nothing about the pane is known, and typing
		// blind is exactly how a replay of a maybe-in-pane row duplicates.
		// Same fail-closed shape as devin's presend:read_failed.
		return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: harness, Evidence: "presend:unproven:read_failed"}
	}
	// A failed visible read leaves the presend banner state unknown, which
	// counts as "already queued" -- the postsend banner then proves nothing
	// either (tester N-r2-3).
	presendQueued := !presend.visibleOK || relayQueuedBanner(harness, presend.visible)
	switch result, rule := relayClassifyReads(harness, presend, text, members, false, presendQueued); result {
	case "marker_observed":
		return relayInjectResult{Outcome: relayInjectDelivered, Harness: harness, Evidence: "presend:" + rule}
	case "member_present":
		return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: harness, Evidence: "presend:" + rule}
	case "composer_residue":
		// An earlier attempt's paste may still be sitting in the composer.
		// The return-once contract submits it only when the composer
		// provably holds nothing but this text; with no earlier read a
		// paste chip is unowned and never earns a keypress.
		return relayVerifyOutcome(ctx, pane, harness, text, "", "presend:", presendQueued, presend, members)
	}
	if exec.CommandContext(ctx, "herdr", "agent", "prompt", pane, text).Run() != nil {
		// herdr may have typed part of the text before failing; the next
		// attempt's presend check decides whether a retry would duplicate
		// it.
		return relayInjectResult{Outcome: relayInjectRetryable, Harness: harness, Evidence: "prompt_failed"}
	}
	return relayVerifyOutcome(ctx, pane, harness, text, presend.visible, "", presendQueued, relayReadPane(ctx, pane), members)
}

func relayInjectHarness(ctx context.Context, pane string) string {
	out, err := exec.CommandContext(ctx, "herdr", "agent", "get", pane).Output()
	if err != nil {
		return ""
	}
	var response struct {
		Result struct {
			Agent struct {
				Agent   string `json:"agent"`
				Harness string `json:"harness"`
				Kind    string `json:"kind"`
			} `json:"agent"`
		} `json:"result"`
	}
	if json.Unmarshal(out, &response) != nil {
		return ""
	}
	for _, candidate := range []string{response.Result.Agent.Harness, response.Result.Agent.Kind, response.Result.Agent.Agent} {
		if candidate != "" {
			return candidate
		}
	}
	return ""
}

// harnessHasSubmissionEvidence reports whether classifySubmission can ever
// prove this harness's submission one way or the other. queued and
// marker_observed are gated on harness being claude, codex, or devin (see
// classifySubmission); any other harness (e.g. grok, which exposes no prompt
// in recent-unwrapped at all -- see 2026-09-14 fleet notes) can only ever
// land on composer_residue (and only if it happens to emit claude's literal
// paste-chip text, which most don't) or unproven. See the unproven carve-out
// below for why that distinction matters. The hub relay sends devin through
// devinRelayInject (#547) instead, so it never reaches this carve-out there.
func harnessHasSubmissionEvidence(harness string) bool {
	return strings.EqualFold(harness, "claude") || strings.EqualFold(harness, "codex") || strings.EqualFold(harness, "devin")
}

// relayComposerSoleChipRE is claude's paste chip alone, after
// compactWhitespace: "[Pasted text #1 +12 lines]" -> "[Pastedtext#1+12lines]".
var relayComposerSoleChipRE = regexp.MustCompile(`^\[Pastedtext#\d+(?:\+\d+lines?)?\]$`)

// devinComposerQueueHint is devin's composer while it holds a queued message,
// captured live (see devinQueued); Enter there sends the whole queue.
const devinComposerQueueHint = "Press Enter to send queued messages now"

// relayComposer returns the live composer's text for harness, without the
// prompt glyph and with whitespace removed, and whether it was located.
// Only claude and devin draw a composer between two dividers; codex draws
// none and its placeholder is plain text, so its composer is never located.
func relayComposer(harness, screen string) (string, bool) {
	isDivider, glyph := isDividerLine, "❯"
	switch {
	case strings.EqualFold(harness, "claude"):
	case strings.EqualFold(harness, "devin"):
		isDivider, glyph = isDevinDividerLine, "❭"
	default:
		return "", false
	}
	region, ok := composerRegionWith(screen, isDivider)
	if !ok {
		return "", false
	}
	return strings.TrimPrefix(compactWhitespace(region), glyph), true
}

// relayComposerReturnSafe reports whether one return keypress on screen can
// only submit what this inject typed (#626, hk:doc
// task/2026-09-24/phantom-suggestion-submitted). A composer is not
// necessarily empty before an inject: a Claude Code prompt suggestion or an
// unsent draft reads as plain composer text in herdr agent read, and herdr's
// paste lands after it. So return is allowed only when the live composer is
// located and holds:
//
//   - nothing;
//   - exactly text, ignoring whitespace, which wrapping and continuation
//     indents change;
//   - a single paste chip, but only if before -- the screen read just before
//     the paste -- shows a located composer with no chip: a chip hides its
//     content, so only its absence beforehand proves it is this inject's;
//   - devin only: its queue hint, but only if before shows no queue: the
//     keypress sends every queued message, not just this one.
//
// Anything else, including a composer that cannot be located, withholds the
// return. The rule names are the evidence recorded with the outcome.
func relayComposerReturnSafe(harness, screen, text, before string) (bool, string) {
	content, ok := relayComposer(harness, screen)
	if !ok {
		return false, "composer_unlocated"
	}
	switch {
	case content == "":
		return true, "composer_empty"
	case content == compactWhitespace(text):
		return true, "composer_self"
	case relayComposerSoleChipRE.MatchString(content):
		if prior, ok := relayComposer(harness, before); !ok || strings.Contains(prior, "[Pastedtext#") {
			return false, "composer_chip_unowned"
		}
		return true, "composer_self_chip"
	case strings.EqualFold(harness, "devin") && content == compactWhitespace(devinComposerQueueHint):
		if _, ok := relayComposer(harness, before); !ok || devinQueued(before) {
			return false, "composer_queue_unowned"
		}
		return true, "composer_queue_hint"
	}
	return false, "composer_foreign"
}

// relayVerifyVisibleLines / relayVerifyUnwrappedLines are the two read
// windows a claude/codex verification consults (#683): the visible screen
// for the composer and the queue banner, and recent-unwrapped for a
// transcript echo that a busy pane has already scrolled past the visible
// window -- the #650 AC3 false negative was exactly such an echo.
const (
	relayVerifyVisibleLines   = "60"
	relayVerifyUnwrappedLines = "400"
)

// relayPaneReads pairs the two read sources; anyOK reports whether at least
// one answered, so a dead herdr is told apart from an empty pane, and
// visibleOK marks whether the banner/composer source answered at all.
type relayPaneReads struct {
	visible, unwrapped string
	anyOK, visibleOK   bool
}

func relayReadPane(ctx context.Context, pane string) relayPaneReads {
	var reads relayPaneReads
	if out, err := exec.CommandContext(ctx, "herdr", "agent", "read", pane, "--source", "visible", "--lines", relayVerifyVisibleLines).Output(); err == nil {
		reads.visible, reads.anyOK, reads.visibleOK = string(out), true, true
	}
	if out, err := exec.CommandContext(ctx, "herdr", "agent", "read", pane, "--source", "recent-unwrapped", "--lines", relayVerifyUnwrappedLines).Output(); err == nil {
		reads.unwrapped, reads.anyOK = string(out), true
	}
	return reads
}

// relayContainsMarker reports whether compacted text holds any marker.
// Whitespace is ignored because a wrapped screen can split the marker
// across lines.
func relayContainsMarker(compacted string, markers []string) bool {
	for _, marker := range markers {
		if marker != "" && strings.Contains(compacted, compactWhitespace(marker)) {
			return true
		}
	}
	return false
}

// relayQueuedBanner is the documented "accepted into the submit-later
// queue" evidence for the relay (#683): claude's "Press up to edit queued
// messages" banner, codex's "Messages to be submitted after next tool
// call", and devin's queue state. Landing in the harness's own queue means
// the message was delivered to it -- the harness submits the queue itself
// when the turn ends -- and it never earns a return keypress.
func relayQueuedBanner(harness, screen string) bool {
	switch {
	case strings.EqualFold(harness, "claude"), strings.EqualFold(harness, "codex"):
		return strings.Contains(screen, "Press up to edit queued messages") ||
			strings.Contains(screen, "Messages to be submitted after next tool call")
	case strings.EqualFold(harness, "devin"):
		return devinQueued(screen)
	}
	return false
}

// relayNormalizeEcho compacts whitespace and drops the inline markdown
// delimiters claude's echo renderer removes (proven live: **…** and `…`
// vanish from the drawn echo; *…*, _…_ and ~~…~~ are the same family).
// Normalizing both sides keeps the comparison honest for the relay texts
// that carry markdown -- 42% of real job.completed texts, per the #683
// tester's live capture (M1). Two texts differing only in delimiters
// collapse to one; acceptable, they render identically on the pane.
func relayNormalizeEcho(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '*', '`', '_', '~':
			return -1
		}
		return r
	}, compactWhitespace(s))
}

// relayWrapContinuation reports whether line is a word-wrap continuation of
// the row above it: indented, with no leading transcript row glyph. An
// indented non-glyph transcript row is indistinguishable from a wrap, and
// the false flag only ever merges a real row into the previous one, which
// withholds a delivered claim rather than inventing one.
func relayWrapContinuation(line string) bool {
	trimmed := strings.TrimLeft(line, " ")
	if trimmed == "" || trimmed == line {
		return false
	}
	for _, glyph := range []string{"❯", "❭", "○", "⏺", "─", "✻", "⏵", "│", "·", "›", "●", "◆"} {
		if strings.HasPrefix(trimmed, glyph) {
			return false
		}
	}
	return true
}

// relayEchoContains reports whether text's stripped body appears in screen
// ending at a line boundary. Identity needs the whole body: rounds of one
// job share both 48-rune markers (same label and host give the same head,
// the same report path the same tail; only the middle differs), and a bare
// substring match lets a shorter message hide inside a longer echo that
// merely contains it (#683 tester B1'). joinWraps folds indented
// continuation rows into their parent line first: the visible read is
// physical rows, and without it a body ending at a wrap break counts as a
// line end even though that echo keeps going (tester B1''). The
// recent-unwrapped source is already logical lines and is never joined.
func relayEchoContains(screen, text string, joinWraps bool) bool {
	body := relayNormalizeEcho(stripRelayBoilerplate(text))
	if body == "" {
		return false
	}
	var lines []string
	for _, line := range strings.Split(screen, "\n") {
		if joinWraps && len(lines) > 0 && relayWrapContinuation(line) {
			lines[len(lines)-1] += line
			continue
		}
		lines = append(lines, line)
	}
	for _, line := range lines {
		if strings.HasSuffix(relayNormalizeEcho(line), body) {
			return true
		}
	}
	return false
}

// relayTranscriptEchoes names the first source whose transcript -- the read
// with the composer region cut away, because recent-unwrapped can carry the
// live composer rows (#683 tester S3) -- holds echo evidence of text.
func relayTranscriptEchoes(harness string, reads relayPaneReads, text string) string {
	if relayEchoContains(promptTranscript(harness, reads.visible), text, true) {
		return "visible"
	}
	if relayEchoContains(promptTranscript(harness, reads.unwrapped), text, false) {
		return "recent-unwrapped"
	}
	return ""
}

// relayClassifyReads folds the two read sources into one submission
// classification. postSend says the reads were taken after this inject's
// paste and presendQueued says a queue banner was already showing then:
// only a banner that newly appeared proves this message joined the queue,
// because a pre-existing banner belongs to an older queue (#683 tester S1).
// Rules, in order:
//
//   - composer_residue: the located composer holds a paste chip or a
//     fragment of the text -- any fragment counts, because residue only
//     ever costs a withheld return, never a delivery claim -- or a chip
//     floats on a screen whose composer cannot be located;
//   - queued (postSend only): the harness's queue banner newly appeared;
//   - marker_observed: the text's whole stripped body echoed in the visible
//     transcript above the composer or in the recent-unwrapped transcript,
//     ending at a line boundary (fragments are shared between messages and
//     prove nothing -- #683 tester B1');
//   - member_present (presend only): no echo of the batch text, but a batch
//     member is on the pane -- part of the batch may have landed by another
//     route, so the batch is neither pasted nor claimed;
//   - unproven.
func relayClassifyReads(harness string, reads relayPaneReads, text string, members []string, postSend, presendQueued bool) (string, string) {
	if composer, ok := relayComposer(harness, reads.visible); ok {
		if strings.Contains(composer, "[Pastedtext#") {
			return "composer_residue", "composer_divider"
		}
		for _, value := range append([]string{text}, members...) {
			if relayContainsMarker(composer, devinRelayMarkers(value, nil)) {
				return "composer_residue", "composer_divider"
			}
		}
	} else if pasteChipRE.MatchString(reads.visible) {
		return "composer_residue", "paste_chip"
	}
	if postSend && !presendQueued && relayQueuedBanner(harness, reads.visible) {
		return "queued", "queued_banner"
	}
	if source := relayTranscriptEchoes(harness, reads, text); source != "" {
		return "marker_observed", source + ":marker_echo"
	}
	if !postSend {
		for _, member := range members {
			if source := relayTranscriptEchoes(harness, reads, member); source != "" {
				return "member_present", source + ":member_echo"
			}
		}
	}
	return "unproven", "none"
}

// relayComposerState names what the visible read showed of the composer for
// unproven evidence: text still pending in it, an empty composer, or no
// located composer at all (a composer taller than the window, or a harness
// that draws none).
func relayComposerState(harness, visible string) string {
	content, ok := relayComposer(harness, visible)
	switch {
	case !ok:
		return "composer_unlocated"
	case content != "":
		return "composer_pending"
	}
	return "composer_empty"
}

// relayUnproven is the #683 policy: an undecidable verdict after herdr
// accepted the send is maybe-in-pane, never a re-inject. The durable relay
// row stays undelivered, and a hub replay -- which meets the presend check
// before anything is typed -- is the late-delivery path. A harness with no
// submission-evidence path at all keeps the #264 D2 AC0 carve-out:
// delivered on no negative evidence (e.g. grok, which exposes no prompt in
// recent-unwrapped -- see 2026-09-14 fleet notes).
func relayUnproven(harness string, reads relayPaneReads, prefix string) relayInjectResult {
	if !reads.anyOK {
		return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: harness, Evidence: prefix + "unproven:read_failed"}
	}
	if !harnessHasSubmissionEvidence(harness) {
		return relayInjectResult{Outcome: relayInjectDelivered, Harness: harness}
	}
	return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: harness, Evidence: prefix + "unproven:" + relayComposerState(harness, reads.visible)}
}

// relayVerifyOutcome maps one verification round to an inject outcome,
// spending the single return keypress the #626 contract permits on a
// composer_residue result and verifying once more after it. presendQueued
// records whether a queue banner was already showing before this inject's
// paste; prefix namespaces the evidence ("presend:", "after_return:").
func relayVerifyOutcome(ctx context.Context, pane, harness, text, before, prefix string, presendQueued bool, reads relayPaneReads, members []string) relayInjectResult {
	maybe := func(evidence string) relayInjectResult {
		return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: harness, Evidence: prefix + evidence}
	}
	result, rule := relayClassifyReads(harness, reads, text, members, true, presendQueued)
	if result == "composer_residue" {
		// The relay-handoff contract permits exactly one return, and only
		// when the live composer provably holds nothing but this inject's
		// text -- the keypress submits whatever the composer holds. A
		// withheld return is may-be-in-pane, never a re-inject.
		if safe, why := relayComposerReturnSafe(harness, reads.visible, text, before); !safe {
			return maybe("return_withheld:" + why)
		}
		if exec.CommandContext(ctx, "herdr", "agent", "send-keys", pane, "return").Run() != nil {
			return maybe("return_failed")
		}
		// A banner already up on the residue screen is an older queue, so it
		// keeps excluding the queued rule for the post-return reads; a
		// failed residue-screen read leaves that state unknown, which
		// excludes it too.
		presendQueued = presendQueued || !reads.visibleOK || relayQueuedBanner(harness, reads.visible)
		reads = relayReadPane(ctx, pane)
		prefix += "after_return:"
		result, rule = relayClassifyReads(harness, reads, text, members, true, presendQueued)
	}
	switch result {
	case "marker_observed":
		return relayInjectResult{Outcome: relayInjectDelivered, Harness: harness, Evidence: prefix + rule}
	case "queued":
		return relayInjectResult{Outcome: relayInjectQueued, Harness: harness, Evidence: prefix + rule}
	case "composer_residue":
		// The one return did not clear it; another keypress is never sent.
		return maybe(rule)
	}
	return relayUnproven(harness, reads, prefix)
}

// relayInjectVerifySubmission is relayInjectVerify with no pre-paste read,
// so a paste chip on screen never counts as this inject's own.
func relayInjectVerifySubmission(ctx context.Context, pane, harness, text string) bool {
	outcome := relayInjectVerify(ctx, pane, harness, text, "").Outcome
	return outcome == relayInjectDelivered || outcome == relayInjectQueued
}

// relayInjectVerify verifies a claude/codex relay inject that herdr already
// accepted; before is the visible screen read just before the paste. It is
// delivered on echo or queue proof, and relayInjectMaybeInPane whenever the
// reads cannot prove what happened (#683) -- the text may already be in the
// pane, so an undecidable verdict is never a re-inject.
func relayInjectVerify(ctx context.Context, pane, harness, text, before string) relayInjectResult {
	return relayVerifyOutcome(ctx, pane, harness, text, before, "", relayQueuedBanner(harness, before), relayReadPane(ctx, pane), nil)
}

// relayBoilerplateRE strips the fixed prefixes relay.go and busy_relay.go put
// in front of every relay text. markerFor keeps the first 24 runes, and for a
// relay text those are the same boilerplate on every message -- a devin
// marker taken from them would match any earlier relay message on screen.
var relayBoilerplateRE = regexp.MustCompile(`^\s*(?:\(같은 내용이 두 번 보이면 재실행 금지\)|\[대기 만료 \d+분\]|\[batch \d+건\]|1\))\s*`)

const devinRelayMarkerRunes = 48

func stripRelayBoilerplate(text string) string {
	body := strings.TrimSpace(text)
	for {
		stripped := relayBoilerplateRE.ReplaceAllString(body, "")
		if stripped == body {
			return body
		}
		body = stripped
	}
}

// devinRelayMarker is the part of a relay text that identifies the message:
// the text after the boilerplate, e.g. "[event] <lane> :: <text>".
func devinRelayMarker(text string) string {
	runes := []rune(stripRelayBoilerplate(text))
	if len(runes) > devinRelayMarkerRunes {
		runes = runes[:devinRelayMarkerRunes]
	}
	return string(runes)
}

// devinRelayTailMarker is the end of a relay text. A long message scrolls its
// head out of any read window while its tail stays at the bottom, next to
// devin's composer.
func devinRelayTailMarker(text string) string {
	runes := []rune(stripRelayBoilerplate(text))
	if len(runes) > devinRelayMarkerRunes {
		runes = runes[len(runes)-devinRelayMarkerRunes:]
	}
	return strings.TrimSpace(string(runes))
}

// devinRelayMarkers is every marker that identifies text or one of the batch
// members folded into it: head and tail of each. Any one of them on screen
// means that message may already be in the pane.
func devinRelayMarkers(text string, members []string) []string {
	var markers []string
	seen := map[string]bool{}
	for _, value := range append([]string{text}, members...) {
		for _, marker := range []string{devinRelayMarker(value), devinRelayTailMarker(value)} {
			if marker != "" && !seen[marker] {
				seen[marker] = true
				markers = append(markers, marker)
			}
		}
	}
	return markers
}

// devin verification reads: the visible screen for the queue banner and the
// composer, and a long recent-unwrapped window for the transcript, which a
// single long message can fill by itself.
const (
	devinRelayVisibleLines   = "60"
	devinRelayUnwrappedLines = "400"
)

// devinPaneRead is one devin verification read: the visible screen (queue
// banner and composer) and recent-unwrapped (the transcript, unsplit by line
// wrapping), classified together.
type devinPaneRead struct {
	visible, unwrapped string
}

func readDevinPane(ctx context.Context, pane string) (devinPaneRead, error) {
	visible, err := exec.CommandContext(ctx, "herdr", "agent", "read", pane, "--source", "visible", "--lines", devinRelayVisibleLines).Output()
	if err != nil {
		return devinPaneRead{}, err
	}
	unwrapped, err := exec.CommandContext(ctx, "herdr", "agent", "read", pane, "--source", "recent-unwrapped", "--lines", devinRelayUnwrappedLines).Output()
	if err != nil {
		return devinPaneRead{}, err
	}
	return devinPaneRead{visible: string(visible), unwrapped: string(unwrapped)}, nil
}

// classify folds sources and texts into one result, ordering devin's rules
// so a message only in devin's queue never reads as submitted: residue in a
// located composer first, then the queue state, and only then a transcript
// echo. Residue and echo count on any fragment of a text (a long devin
// echo can scroll its head out of the window while the tail stays). The
// queue arm additionally needs a fragment of one of the texts on the
// screen -- a foreign or pre-existing queue without it proves nothing
// (#683 S1). Echoes are matched on the transcript with the composer cut,
// because recent-unwrapped can carry the live composer rows (#683 S3).
func (read devinPaneRead) classify(texts []string) (string, string) {
	sources := []struct{ name, text string }{{"visible", read.visible}, {"recent-unwrapped", read.unwrapped}}
	for _, source := range sources {
		region, ok := composerRegionWith(source.text, isDevinDividerLine)
		if !ok {
			continue
		}
		compact := compactWhitespace(region)
		for _, text := range texts {
			if relayContainsMarker(compact, devinRelayMarkers(text, nil)) {
				return "composer_residue", source.name + ":composer_divider"
			}
		}
	}
	for _, source := range sources {
		if !devinQueued(source.text) {
			continue
		}
		compact := compactWhitespace(source.text)
		for _, text := range texts {
			if relayContainsMarker(compact, devinRelayMarkers(text, nil)) {
				return "queued", source.name + ":devin_queue_banner"
			}
		}
	}
	for _, source := range sources {
		transcript := relayNormalizeEcho(promptTranscript("devin", source.text))
		for _, text := range texts {
			for _, marker := range devinRelayMarkers(text, nil) {
				if marker != "" && strings.Contains(transcript, relayNormalizeEcho(marker)) {
					return "marker_observed", source.name + ":marker_echo"
				}
			}
		}
	}
	return "unproven", "visible+recent-unwrapped:none"
}

// markerSource names the first source that shows any marker of any text,
// or "". Presend matching stays fragment-weak: a partial hit only ever
// withholds the paste, never a delivery claim.
func (read devinPaneRead) markerSource(markers []string) string {
	for _, source := range []struct{ name, text string }{{"visible", read.visible}, {"recent-unwrapped", read.unwrapped}} {
		compact := compactWhitespace(source.text)
		for _, marker := range markers {
			if strings.Contains(compact, compactWhitespace(marker)) {
				return source.name
			}
		}
	}
	return ""
}

// devinRelayInject is the devin relay path (#547). It never types a message
// that may already be in the pane:
//
//   - Before sending, the visible screen (queue rows, composer) and the
//     recent-unwrapped transcript must lack every marker of the text and of
//     each batch member. This runs on every attempt, not only local
//     retries, because the hub replays an undelivered row to the node as a
//     fresh message.
//   - Once herdr has accepted the text it is in the pane somewhere. Only a
//     transcript echo proves it submitted; residue or a queue banner gets
//     exactly one return keypress; anything short of an echo after that --
//     including no sign of it at all, which is what a message scrolled past
//     the read window looks like -- is relayInjectMaybeInPane, never a retry.
//   - Only a send herdr rejected is retryable, and busy_relay.go caps devin
//     at one re-inject; the next attempt's presend check still runs first.
func devinRelayInject(ctx context.Context, pane, text string, members []string) relayInjectResult {
	texts := append([]string{text}, members...)
	markers := devinRelayMarkers(text, members)
	result := relayInjectResult{Harness: "devin"}
	if len(markers) == 0 {
		result.Outcome, result.Evidence = relayInjectMaybeInPane, "presend:empty_marker"
		return result
	}
	before, err := readDevinPane(ctx, pane)
	if err != nil {
		result.Outcome, result.Evidence = relayInjectMaybeInPane, "presend:read_failed"
		return result
	}
	if source := before.markerSource(markers); source != "" {
		result.Outcome, result.Evidence = relayInjectMaybeInPane, "presend:"+source+":marker_present"
		return result
	}
	if exec.CommandContext(ctx, "herdr", "agent", "prompt", pane, text).Run() != nil {
		// herdr may have typed part of the text before failing; the next
		// attempt's presend check decides whether a retry would duplicate it.
		result.Outcome, result.Evidence = relayInjectRetryable, "prompt_failed"
		return result
	}
	after, err := readDevinPane(ctx, pane)
	if err != nil {
		result.Outcome, result.Evidence = relayInjectMaybeInPane, "postsend:read_failed"
		return result
	}
	state, evidence := after.classify(texts)
	switch state {
	case "marker_observed":
		result.Outcome, result.Evidence = relayInjectDelivered, evidence
		return result
	case "unproven":
		result.Outcome, result.Evidence = relayInjectMaybeInPane, "postsend:"+evidence
		return result
	}
	// composer_residue or queued: devin's own hint says Enter submits a queued
	// message now, and Enter submits composer text. One keypress, never more.
	// For a queue, Enter on a busy devin interrupts the running command, so
	// it is sent only when the pane is proven idle; otherwise the message
	// stays queued for devin to submit at the end of its turn.
	if state == "queued" {
		if busy := devinBusy(ctx, pane, after); busy != "" {
			result.Outcome, result.Evidence = relayInjectQueued, evidence+" busy:"+busy
			return result
		}
	}
	// #626: the keypress submits whatever is in the composer (or, on the
	// queue hint, the whole queue), so it is sent only when that can be
	// nothing but this text; before is the presend read.
	if safe, rule := relayComposerReturnSafe("devin", after.visible, text, before.visible); !safe {
		result.Outcome, result.Evidence = relayInjectMaybeInPane, evidence+" return_withheld:"+rule
		return result
	}
	if exec.CommandContext(ctx, "herdr", "agent", "send-keys", pane, "return").Run() != nil {
		result.Outcome, result.Evidence = relayInjectMaybeInPane, evidence+" return_failed"
		return result
	}
	after, err = readDevinPane(ctx, pane)
	if err != nil {
		result.Outcome, result.Evidence = relayInjectMaybeInPane, evidence+" after_return:read_failed"
		return result
	}
	state, afterEvidence := after.classify(texts)
	if state == "marker_observed" {
		result.Outcome, result.Evidence = relayInjectDelivered, evidence+" after_return:"+afterEvidence
		return result
	}
	result.Outcome, result.Evidence = relayInjectMaybeInPane, evidence+" after_return:"+afterEvidence
	return result
}

// devinSpinnerRE matches devin's activity line, captured live as
// "⢀⣀ Running tools · 12s (esc twice to interrupt)" and
// "⣄⠀ Thinking · 5s (esc twice to interrupt)": one to three braille glyphs,
// a word, then an elapsed time. devin's idle logo is braille too, but as a
// longer run with no elapsed time.
var devinSpinnerRE = regexp.MustCompile(`^[\x{2800}-\x{28FF}]{1,3}\s+\pL[\pL ]*·\s*\d+[smh]`)

// devinBusy names why the pane must be treated as working, or returns "" when
// it is proven idle. It is conservative: an unreadable status counts as busy.
// Evidence, in order: herdr's agent_status, then -- because devin's status
// can read done during a long turn -- a spinner line or the working
// placeholder between the last transcript echo and the composer, in either
// source (a long queue can push the spinner out of the visible screen).
func devinBusy(ctx context.Context, pane string, read devinPaneRead) string {
	status, ok := relayAgentStatus(ctx, pane)
	if !ok {
		return "status_unknown"
	}
	if strings.EqualFold(status, "working") {
		return "agent_status_working"
	}
	for _, source := range []struct{ name, text string }{{"visible", read.visible}, {"recent-unwrapped", read.unwrapped}} {
		if strings.TrimSpace(source.text) == "" {
			return source.name + ":empty"
		}
		if why := devinActivity(source.text); why != "" {
			return source.name + ":" + why
		}
	}
	return ""
}

// devinActivity looks between the last transcript echo and the composer.
func devinActivity(screen string) string {
	lines := strings.Split(screen, "\n")
	end := len(lines)
	if start, ok := composerStartWith(lines, isDevinDividerLine); ok {
		if region, _ := composerRegionWith(screen, isDevinDividerLine); strings.Contains(region, "Guide Devin while it works") {
			return "working_placeholder"
		}
		end = start
	}
	for i := end - 1; i >= 0 && !strings.HasPrefix(lines[i], "❭"); i-- {
		if devinSpinnerRE.MatchString(strings.TrimSpace(lines[i])) || strings.Contains(compactWhitespace(lines[i]), "(esctwicetointerrupt)") {
			return "spinner"
		}
	}
	return ""
}

func relayAgentStatus(ctx context.Context, pane string) (string, bool) {
	out, err := exec.CommandContext(ctx, "herdr", "agent", "get", pane).Output()
	if err != nil {
		return "", false
	}
	var response struct {
		Result struct {
			Agent struct {
				Status string `json:"agent_status"`
			} `json:"agent"`
		} `json:"result"`
	}
	if json.Unmarshal(out, &response) != nil || response.Result.Agent.Status == "" {
		return "", false
	}
	return response.Result.Agent.Status, true
}

// serve runs the node side of one hub session. A preference switch replaces the
// live connection rather than nesting another session: serveConnection hands
// the already-open candidate back and this loop continues on it, so each
// session's ticker, preference ticker, and reader goroutine are released at the
// switch instead of at the end of a recursive chain.
func (client *HubClient) serve(ctx context.Context, connection *websocket.Conn) error {
	current := connection
	for {
		next, err := client.serveConnection(ctx, current)
		if next == nil {
			if current != connection {
				// Run only closes the connection it dialed.
				_ = current.CloseNow()
			}
			return err
		}
		current = next
	}
}

// serveConnection returns a non-nil connection only when the caller should
// continue the session on that already-open preferred endpoint; the superseded
// connection has been closed by then.
func (client *HubClient) serveConnection(ctx context.Context, connection *websocket.Conn) (*websocket.Conn, error) {
	connection.SetReadLimit(hubSpawnMaxBodyBytes)
	peer := &hubClientConnection{connection: connection}
	if err := peer.write(ctx, struct {
		Type      string `json:"type"`
		MachineID string `json:"machine_id"`
		Version   string `json:"version"`
		Accepting bool   `json:"accepting,omitempty"`
	}{Type: "hello", MachineID: client.machineID, Version: client.version, Accepting: client.accepting}); err != nil {
		return nil, err
	}
	// A hello reached the hub: a version under update probation has proven it
	// starts, so its consecutive-failure record is cleared.
	client.confirmHubUpdate()
	client.setRelayEmitter(func(event hubClientEvent) { _ = peer.write(ctx, hubClientWireEvent(event)) })
	client.relayBusyManager().resume(ctx)
	if err := peer.write(ctx, hubClientWireEvent(client.heartbeatEvent(ctx))); err != nil {
		return nil, err
	}
	if err := client.writeRelayEvents(client.jobCompletionEvents(), peer.relayWriter(ctx)); err != nil {
		return nil, err
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
			message, ok := parseHubOutboundPinned(payload, client.updateRepository)
			if !ok {
				continue
			}
			if message.Type == "relay.inject" {
				// Preserve websocket receive order before moving potentially blocking
				// agent get/wait/prompt work out of the read loop.
				message.RecvSeq = client.nextRelayRecvSeq()
				go client.handleRelayInject(ctx, peer, message)
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
			case "relay.persisted":
				client.recordRelayPersisted(message)
			case "idle-wake.route":
				if manager := client.idleWakeManager(); manager != nil {
					manager.ApplyRoute(ctx, hubIdleWakeRouteEvent{Type: message.Type, EventID: message.IdleWakeEventID, Pane: message.Pane, Eligible: message.Eligible, Lane: message.Lane, Text: message.Text, JobID: message.JobID, Reason: message.Reason})
				}
			case "burst.holds":
				client.burstMu.Lock()
				client.burstHoldsActive = message.HoldsActive
				client.burstMu.Unlock()
			case "relay.edit":
				client.relayBusyManager().edit(ctx, message.EventID, message.Text)
			case "relay.cancel":
				client.relayBusyManager().cancel(ctx, message.EventID)
			case "update.available":
				if !client.beginHubUpdate() {
					// One self-update at a time; the hub is told rather than
					// left to infer the outcome from a missing restart.
					if err := peer.write(ctx, hubOutbound{Type: "update.busy"}); err != nil {
						select {
						case readErrors <- err:
						case <-ctx.Done():
						}
						return
					}
					continue
				}
				go client.handleHubUpdate(ctx, peer, message)
			case "quota.request":
				go client.handleHubQuota(ctx, peer, message)
			case "job.spawn":
				go client.handleHubSpawn(ctx, peer, message)
			}
		}
	}()
	ticker := time.NewTicker(client.pingInterval)
	defer ticker.Stop()
	prefer := time.NewTicker(client.preferRetry)
	defer prefer.Stop()
	type preferenceResult struct {
		connection *websocket.Conn
		endpoint   string
		switched   bool
	}
	preferenceResults := make(chan preferenceResult, 1)
	serveDone := make(chan struct{})
	defer close(serveDone)
	preferenceProbing := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-readErrors:
			return nil, err
		case event := <-client.events:
			if err := peer.write(ctx, hubClientWireEvent(event)); err != nil {
				client.releaseRelayEvents(event)
				return nil, err
			}
			client.commitRelaySent(event)
		case <-ticker.C:
			if err := peer.write(ctx, hubOutbound{Type: "ping"}); err != nil {
				return nil, err
			}
			if err := peer.write(ctx, hubClientWireEvent(client.heartbeatEvent(ctx))); err != nil {
				return nil, err
			}
			if err := client.writeRelayEvents(client.jobCompletionEvents(), peer.relayWriter(ctx)); err != nil {
				return nil, err
			}
		case result := <-preferenceResults:
			preferenceProbing = false
			if result.switched {
				// Hand the already-open candidate back to serve, which
				// continues the session on it. The old connection stays live
				// until this point, so a failed preference probe cannot cause
				// an outage.
				client.endpoint = result.endpoint
				_ = connection.CloseNow()
				return result.connection, nil
			}
		case <-prefer.C:
			if preferenceProbing {
				continue
			}
			preferenceProbing = true
			go func() {
				attemptContext, cancel := context.WithTimeout(ctx, 5*time.Second)
				candidate, endpoint, switched := client.dialPreferred(attemptContext)
				cancel()
				result := preferenceResult{connection: candidate, endpoint: endpoint, switched: switched}
				select {
				case preferenceResults <- result:
				case <-serveDone:
					if candidate != nil {
						_ = candidate.CloseNow()
					}
				}
			}()
		}
	}
}

// relayWriter is the one-event write this connection performs. Taking it as a
// function is what lets writeRelayEvents be exercised with a write that fails
// half-way through a batch.
func (peer *hubClientConnection) relayWriter(ctx context.Context) func(hubClientEvent) error {
	return func(event hubClientEvent) error { return peer.write(ctx, hubClientWireEvent(event)) }
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
	client.relayBusyManager().offer(parent, message)
}

func (client *HubClient) relayInjectTimeout() time.Duration {
	if client.r19a.relayInjectTimeout > 0 {
		return client.r19a.relayInjectTimeout
	}
	return defaultRelayInjectTimeout
}

func (client *HubClient) relayStore() *Store {
	client.outboxMu.Lock()
	defer client.outboxMu.Unlock()
	return client.outbox
}

func (client *HubClient) relayHeldByKey(ctx context.Context, lane string, eventID int64) (relayHeld, bool, error) {
	store := client.relayStore()
	if store == nil {
		return relayHeld{}, false, nil
	}
	return store.RelayHeldByKey(ctx, lane, eventID)
}

func (client *HubClient) nextRelayRecvSeq() int64 {
	client.relayRecvSeqMu.Lock()
	defer client.relayRecvSeqMu.Unlock()
	client.relayRecvSeq++
	return client.relayRecvSeq
}

func (client *HubClient) seedRelayRecvSeq(value int64) {
	client.relayRecvSeqMu.Lock()
	if value > client.relayRecvSeq {
		client.relayRecvSeq = value
	}
	client.relayRecvSeqMu.Unlock()
}

func (client *HubClient) heartbeatEvent(ctx context.Context) hubClientEvent {
	active := scanHubActiveJobsWithPanes(ctx, client.jobsInboxRoot, client.panesAliveHook())
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
	if provider := client.stallBeatProvider(); provider != nil {
		if beat := provider(); beat != nil {
			heartbeat.StallDetect = beat
		}
	}
	collectLoad := client.hostLoadCollector
	if collectLoad == nil {
		collectLoad = collectHubHostLoad
	}
	if load, err := collectLoad(ctx); err == nil {
		heartbeat.HostLoad = &load
	} else {
		heartbeat.LoadError = err.Error()
	}
	collectMemory := client.hostMemoryCollector
	if collectMemory == nil {
		collectMemory = collectHubHostMemory
	}
	if memory, err := collectMemory(ctx); err == nil {
		heartbeat.HostMemory = memory
	}
	collectQuota := client.quotaCollector
	quota := unavailableHubQuotaSnapshot()
	if collectQuota != nil {
		collected, err := collectQuota(ctx)
		if err == nil && collected != nil && collected.valid() {
			quota = collected
		}
	}
	heartbeat.Quota = cloneHubQuotaSnapshot(quota)
	collectSessions := client.sessionSnapshotHook()
	if collectSessions == nil {
		payload, _ := json.Marshal(heartbeat)
		return hubClientEvent{Kind: "heartbeat", Payload: payload}
	}
	lookup, cancel := context.WithTimeout(ctx, hubSessionSnapshotTimeout)
	defer cancel()
	states, err := collectSessions(lookup)
	if err != nil {
		heartbeat.SnapshotStatus = hubSnapshotStatusUnavailable
		heartbeat.Sessions = nil
		heartbeat.Truncated = false
		payload, _ := marshalHubHeartbeatForWire(heartbeat)
		return hubClientEvent{Kind: "heartbeat", Payload: payload}
	}
	heartbeat.SnapshotStatus = hubSnapshotStatusOK
	payload := marshalHubHeartbeatWithSessions(heartbeat, states)
	return hubClientEvent{Kind: "heartbeat", Payload: payload}
}

// SetStallDetect registers the detector's beat provider. When it is nil the
// heartbeat carries no stall_detect field, which is how the hub knows this
// node never ran the detector rather than the detector having stopped.
func (client *HubClient) SetStallDetect(provider func() *hubStallBeatPayload) {
	client.stallBeatMu.Lock()
	client.stallBeat = provider
	client.stallBeatMu.Unlock()
}

func (client *HubClient) stallBeatProvider() func() *hubStallBeatPayload {
	client.stallBeatMu.Lock()
	defer client.stallBeatMu.Unlock()
	return client.stallBeat
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
func hubJobCompletionPayloadForJob(job HubActiveJob, eventID string, replay bool) json.RawMessage {
	payload, _ := json.Marshal(struct {
		JobID          string `json:"job_id"`
		Epoch          uint64 `json:"epoch"`
		AgentLabel     string `json:"agent_label,omitempty"`
		OwnerLane      string `json:"owner_lane,omitempty"`
		Label          string `json:"label,omitempty"`
		Host           string `json:"host,omitempty"`
		ReportPath     string `json:"report_path,omitempty"`
		ReportLastLine string `json:"report_last_line,omitempty"`
		EventID        string `json:"event_id,omitempty"`
		Replay         bool   `json:"replay,omitempty"`
	}{job.JobID, job.Epoch, job.AgentLabel, job.OwnerLane, job.Label, job.Host, job.ReportPath, job.ReportLastLine, eventID, replay})
	return payload
}

func compactHubRelayEventText(value string, normalizeNewlines bool) string {
	value, _ = truncateHubRelayPayloadText(value, normalizeNewlines)
	return value
}

// normalizeRelayEventText applies the hub's own receipt rules - the 240-rune
// bound and newline removal - on the node, before either the outbox key or the
// wire payload is built. The hub truncates these fields on arrival and rejects
// an embedded newline outright, so a value normalized on only one side is
// exactly how a relay.persisted acknowledgement stops naming the row it was
// meant to retire.
func normalizeRelayEventText(value string) string {
	return compactHubRelayEventText(value, true)
}

// relayEventWireForm is the one normalized form of a scanned record. The outbox
// key, the wire payload, and the hub's acknowledgement are all built from it,
// so the five key fields are the same strings on the scan path, the
// `panewire emit` path, and the restart replay path alike.
func relayEventWireForm(job hubScannedRelayEvent) hubScannedRelayEvent {
	if job.Kind == "lane.event" {
		// lane.event text was validated and, if necessary, byte-truncated only
		// by emit before its file was written. Do not apply the job.* 240-rune
		// normalizer here: a second shape would strand its persisted cursor.
		return job
	}
	job.OwnerLane = normalizeRelayEventText(job.OwnerLane)
	job.Label = normalizeRelayEventText(job.Label)
	job.Host = normalizeRelayEventText(job.Host)
	job.ReportPath = normalizeRelayEventText(job.ReportPath)
	job.ReportLastLine = normalizeRelayEventText(job.ReportLastLine)
	job.Reason = normalizeRelayEventText(job.Reason)
	job.Question = normalizeRelayEventText(job.Question)
	job.PR = normalizeRelayEventText(job.PR)
	job.Head = normalizeRelayEventText(job.Head)
	job.PaneID = normalizeRelayEventText(job.PaneID)
	job.EventID = normalizeRelayEventText(job.EventID)
	return job
}

// relayEventOutboxKeyFor keys the outbox by exactly what leaves the node. A
// job.completed payload carries no reason field, so the hub can only ever echo
// an empty reason for it; keying such a row by the record's own reason is a
// mismatch that leaves persisted_at NULL for good. Callers must pass the wire
// form: a key built from un-normalized text names a row nothing will ever
// acknowledge.
func relayEventOutboxKeyFor(job hubScannedRelayEvent) relayOutboxKey {
	if job.Kind == "lane.event" {
		return relayOutboxKey{Kind: job.Kind, JobID: job.JobID, Epoch: job.Epoch, Lane: job.OwnerLane, EventID: job.EventID}
	}
	reason := job.Reason
	if job.Kind == "job.completed" {
		reason = ""
	}
	// EventID distinguishes one durable event file from the job's next round;
	// an empty one preserves the legacy five-field key for old producers.
	return relayOutboxKey{Kind: job.Kind, JobID: job.JobID, Epoch: job.Epoch, ReportPath: job.ReportPath, Reason: reason, EventID: job.EventID}
}

// nowUTC is the outbox retry clock, injectable so a fixture can pin the
// sixty-second backoff instead of racing the wall clock.
func (client *HubClient) nowUTC() time.Time {
	if client.now != nil {
		return client.now().UTC()
	}
	return time.Now().UTC()
}

// jobCompletionEvents is the node-side producer for the fenced completion
// contract. It emits only a local terminal-event ID/epoch once per epoch.
func (client *HubClient) jobCompletionEvents() []hubClientEvent {
	completed := scanHubRelayEventsWithin(client.jobsInboxRoot, relayOutboxMaxAge())
	client.assignmentMu.Lock()
	for index := range completed {
		if assigned := client.assignedJobs[completed[index].JobID]; assigned > completed[index].Epoch {
			completed[index].Epoch = assigned
		}
	}
	client.assignmentMu.Unlock()
	events := make([]hubClientEvent, 0, len(completed))
	for _, job := range completed {
		if event, ok := client.relayEventForSend(job); ok {
			events = append(events, event)
		}
	}
	return events
}

// relayEventForSend applies the outbox gate and builds the wire payload. It is
// the single place that decides whether a scanned record is still owed to the
// hub, so `panewire emit` and the periodic scan cannot disagree.
func (client *HubClient) relayEventForSend(job hubScannedRelayEvent) (hubClientEvent, bool) {
	job = relayEventWireForm(job)
	key := relayEventOutboxKeyFor(job)
	if client.suppressPreMigrationRelayEvent(job, key) {
		return hubClientEvent{}, false
	}
	if client.outbox != nil {
		if outstanding, found, err := client.outbox.UnpersistedRelayOutboxKey(context.Background(), key); err != nil {
			client.warnMessage("relay outbox outstanding key unavailable")
		} else if found {
			// The scanner's assignment epoch is process memory. Reuse the one
			// outstanding row so the same retained event file is re-presented.
			key, job.Epoch = outstanding, outstanding.Epoch
		}
	}
	send, replay := client.selectRelayEvent(key)
	if !send {
		return hubClientEvent{}, false
	}
	if job.Kind == "job.completed" {
		client.completedJobs[job.JobID] = job.Epoch
	}
	if job.Kind == "lane.event" {
		payload, _ := json.Marshal(struct {
			OwnerLane string `json:"owner_lane"`
			EventID   string `json:"event_id"`
			Text      string `json:"text"`
			Epoch     uint64 `json:"epoch,omitempty"`
			Truncated bool   `json:"truncated,omitempty"`
			Replay    bool   `json:"replay,omitempty"`
		}{job.OwnerLane, job.EventID, job.Text, job.Epoch, job.Truncated, replay})
		return hubClientEvent{Kind: job.Kind, Payload: payload, relayKey: key, relayPending: true}, true
	}
	payload := hubJobCompletionPayloadForJob(job.HubActiveJob, job.EventID, replay)
	if job.Kind == "job.escalate" || job.Kind == "job.joined" || relayTerminalSignalKinds[job.Kind] {
		payload, _ = json.Marshal(struct {
			JobID          string `json:"job_id"`
			Epoch          uint64 `json:"epoch"`
			AgentLabel     string `json:"agent_label,omitempty"`
			OwnerLane      string `json:"owner_lane,omitempty"`
			Label          string `json:"label,omitempty"`
			Host           string `json:"host,omitempty"`
			ReportPath     string `json:"report_path,omitempty"`
			ReportLastLine string `json:"report_last_line,omitempty"`
			Reason         string `json:"reason"`
			Question       string `json:"question,omitempty"`
			PR             string `json:"pr,omitempty"`
			Head           string `json:"head,omitempty"`
			PaneID         string `json:"pane_id,omitempty"`
			EventID        string `json:"event_id,omitempty"`
			Replay         bool   `json:"replay,omitempty"`
		}{job.JobID, job.Epoch, job.AgentLabel, job.OwnerLane, job.Label, job.Host, job.ReportPath, job.ReportLastLine, job.Reason, job.Question, job.PR, job.Head, job.PaneID, job.EventID, replay})
	}
	return hubClientEvent{Kind: job.Kind, Payload: payload, relayKey: key, relayPending: true}, true
}

// suppressPreMigrationRelayEvent retires event files that predate this node's
// event-identity migration by more than the grace window. Deploying the keyed
// outbox used to replay the whole retained backlog as fresh notifications -
// including the completion an old binary already sent. The cutoff is this
// node's own migration instant minus relayMigrationGrace: events inside the
// window may be relayed (a rolling restart's in-flight work is still fresh
// news), everything older is recorded as suppressed and never offered. A
// missing migration stamp or an unidentifiable file fails open, since a lost
// notification is worse than a duplicate - and a store that never migrated
// has no stamp, so a recreated or relocated database suppresses nothing.
// Every suppression is logged once, the first scan that retires the file:
// silent loss is the failure mode this gate exists against.
func (client *HubClient) suppressPreMigrationRelayEvent(job hubScannedRelayEvent, key relayOutboxKey) bool {
	if client.outbox == nil || key.EventID == "" || job.Kind == "lane.event" || job.EventTime.IsZero() {
		return false
	}
	migratedAt, ok, err := client.outbox.RelayEventIDMigrationAt(context.Background())
	if err != nil {
		client.warnMessage("relay migration stamp unavailable")
		return false
	}
	cutoff := migratedAt.Add(-relayMigrationGrace)
	if !ok || !job.EventTime.Before(cutoff) {
		return false
	}
	recorded, err := client.outbox.RecordRelaySuppressed(context.Background(), key, client.nowUTC())
	if err != nil {
		client.warnMessage("relay suppression record failed")
	} else if recorded {
		client.warnMessage(fmt.Sprintf("relay suppressed %s %s for %s: event file predates this node's migration cutoff %s", job.Kind, key.EventID, key.JobID, cutoff.Format(time.RFC3339)))
	}
	return true
}

// selectRelayEvent answers "should this record go out now, and is it a replay".
// It deliberately writes nothing durable: sent_at belongs to commitRelaySent,
// once the write has actually left the node. Stamping here is what made a
// batch that died half-way mark records it never sent, so the backoff held
// them back and a restart repeated the whole thing.
//
// Without a durable outbox it degrades to the historical per-process memory,
// which is exactly the R19f behavior the SQLite table replaces.
func (client *HubClient) selectRelayEvent(key relayOutboxKey) (send bool, replay bool) {
	client.outboxMu.Lock()
	defer client.outboxMu.Unlock()
	if client.completedReports == nil {
		client.completedReports = make(map[string]struct{})
	}
	if client.relayInflight == nil {
		client.relayInflight = make(map[string]struct{})
	}
	text := key.String()
	// A record already queued for a write it has not been stamped for is not
	// offered again; otherwise the scan and `panewire emit` would both take it.
	if _, inflight := client.relayInflight[text]; inflight {
		return false, false
	}
	_, sentThisProcess := client.completedReports[text]
	if client.outbox == nil {
		if sentThisProcess {
			return false, false
		}
		client.relayInflight[text] = struct{}{}
		return true, false
	}
	state, err := client.outbox.RelayOutboxState(context.Background(), key)
	if err != nil {
		client.warnMessage("relay outbox state unavailable")
		if sentThisProcess {
			return false, false
		}
		client.relayInflight[text] = struct{}{}
		return true, false
	}
	if state.Persisted || state.Suppressed {
		client.completedReports[text] = struct{}{}
		return false, false
	}
	// The backoff gates a record that really did go out. One that was selected
	// and never written carries no stamp, so it lands here eligible again.
	if !state.SentAt.IsZero() && client.nowUTC().Sub(state.SentAt) < relayOutboxBackoff {
		return false, false
	}
	// A row already stamped by a previous process is a restart replay. The hub
	// records the flag; it never lets it change routing.
	replay = !sentThisProcess && !state.SentAt.IsZero()
	client.relayInflight[text] = struct{}{}
	return true, replay
}

// commitRelaySent stamps sent_at for a record whose write has succeeded. Until
// this runs the record is unsent as far as the outbox is concerned, which is
// what puts it back in the very next scan rather than behind the backoff.
func (client *HubClient) commitRelaySent(event hubClientEvent) {
	if !event.relayPending {
		return
	}
	client.outboxMu.Lock()
	defer client.outboxMu.Unlock()
	text := event.relayKey.String()
	delete(client.relayInflight, text)
	if client.completedReports == nil {
		client.completedReports = make(map[string]struct{})
	}
	client.completedReports[text] = struct{}{}
	if client.outbox == nil {
		return
	}
	if err := client.outbox.RecordRelaySent(context.Background(), event.relayKey, client.nowUTC()); err != nil {
		client.warnMessage("relay outbox attempt was not recorded")
	}
}

// releaseRelayEvents returns records that never reached the wire to the pool of
// sendable events. Nothing durable is touched: an event that was not sent must
// not carry a send stamp, and it must be eligible again immediately.
func (client *HubClient) releaseRelayEvents(events ...hubClientEvent) {
	client.outboxMu.Lock()
	defer client.outboxMu.Unlock()
	for _, event := range events {
		if event.relayPending {
			delete(client.relayInflight, event.relayKey.String())
		}
	}
}

// writeRelayEvents stamps each record only after its own write succeeded. A
// mid-batch failure therefore leaves every record behind it unstamped and
// retryable on the next connection, instead of stamped and never sent.
func (client *HubClient) writeRelayEvents(events []hubClientEvent, write func(hubClientEvent) error) error {
	for index, event := range events {
		if err := write(event); err != nil {
			client.releaseRelayEvents(events[index:]...)
			return err
		}
		client.commitRelaySent(event)
	}
	return nil
}

// recordRelayPersisted retires an outbox row once the hub confirms handoffkeep
// owns the record. From here the scan skips it for good.
func (client *HubClient) recordRelayPersisted(message hubOutboundMessage) {
	if client.outbox == nil {
		return
	}
	key := relayOutboxKey{Kind: message.Kind, JobID: message.JobID, Epoch: message.Epoch, ReportPath: message.ReportPath, Reason: message.Reason, Lane: message.Lane, EventID: message.ProducerEventID}
	if err := client.outbox.RecordRelayPersisted(context.Background(), key, time.Now().UTC()); err != nil {
		client.warnMessage("relay outbox persistence was not recorded")
	}
}

// EnqueueRelayEvent is the immediate-send path behind `panewire emit`. It
// bypasses the ten-second scan without bypassing the outbox gate.
func (client *HubClient) EnqueueRelayEvent(job hubScannedRelayEvent) bool {
	event, ok := client.relayEventForSend(job)
	if !ok {
		return false
	}
	select {
	case client.events <- event:
		return true
	default:
		// The event file is still on disk; the next scan picks it up. A drop is
		// not a send, so the record keeps its unstamped outbox row and is
		// eligible again straight away rather than after the retry backoff.
		client.releaseRelayEvents(event)
		client.warnMessage("relay event queue is full")
		return false
	}
}

// EnqueueIdleWakeRouteRequest asks the hub to resolve one already-settled
// observation. It is not a delivery acknowledgement and never touches the R21
// outbox; only the later lane.event does that.
func (client *HubClient) EnqueueIdleWakeRouteRequest(request idleWakeRouteRequest) bool {
	if !validIdleWakeRouteRequest(request) {
		return false
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return false
	}
	select {
	case client.events <- hubClientEvent{Kind: "idle-wake.route.request", Payload: payload}:
		return true
	default:
		client.warnMessage("idle-wake route queue is full")
		return false
	}
}

// warnMessage tolerates the directly-constructed clients used by fixtures,
// which do not go through NewHubClient's defaulting.
func (client *HubClient) warnMessage(message string) {
	if client.warn != nil {
		client.warn(message)
	}
}

// SetRelayOutbox attaches the node's durable outbox. The daemon calls it once
// its SQLite store is open.
func (client *HubClient) SetRelayOutbox(store *Store) {
	client.outboxMu.Lock()
	client.outbox = store
	client.outboxMu.Unlock()
	client.relayBusyManager().restore(context.Background())
}

// SetPanesAlive attaches the pane liveness lookup used to drop jobs whose pane
// is gone. A nil hook keeps the heartbeat's inbox-only active set.
func (client *HubClient) SetPanesAlive(panesAlive panesAliveFunc) {
	client.panesAliveMu.Lock()
	defer client.panesAliveMu.Unlock()
	client.panesAlive = panesAlive
}

// SetSessionSnapshot attaches the local herdr agent.list lookup used by the
// heartbeat. A nil hook preserves the pre-snapshot heartbeat shape.
func (client *HubClient) SetSessionSnapshot(collector func(context.Context) ([]HerdrAgentState, error)) {
	client.sessionMu.Lock()
	defer client.sessionMu.Unlock()
	client.sessionCollector = collector
}

func (client *HubClient) SetIdleWakeManager(manager *idleWakeManager) {
	client.idleWakeMu.Lock()
	client.idleWake = manager
	client.idleWakeMu.Unlock()
}

func (client *HubClient) idleWakeManager() *idleWakeManager {
	client.idleWakeMu.Lock()
	defer client.idleWakeMu.Unlock()
	return client.idleWake
}

func (client *HubClient) panesAliveHook() panesAliveFunc {
	client.panesAliveMu.Lock()
	defer client.panesAliveMu.Unlock()
	return client.panesAlive
}

func (client *HubClient) sessionSnapshotHook() func(context.Context) ([]HerdrAgentState, error) {
	client.sessionMu.Lock()
	defer client.sessionMu.Unlock()
	return client.sessionCollector
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

// parseHubOutbound decodes with the default update repository pin; the node
// read loop uses parseHubOutboundPinned with its configured repository.
func parseHubOutbound(payload []byte) (hubOutboundMessage, bool) {
	return parseHubOutboundPinned(payload, hubUpdateDefaultRepository)
}

// parseHubOutboundPinned drops an update.available whose URL is not the
// pinned repository's release asset for the instructed version, so such an
// instruction is never dispatched (applyHubUpdate checks the pin again).
func parseHubOutboundPinned(payload []byte, updateRepository string) (hubOutboundMessage, bool) {
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
		if json.Unmarshal(fields["job_id"], &message.JobID) != nil || json.Unmarshal(fields["pane"], &message.Pane) != nil || json.Unmarshal(fields["text"], &message.Text) != nil || !hubJobIDPattern.MatchString(message.JobID) || message.Pane == "" || len(message.Pane) > 128 {
			return hubOutboundMessage{}, false
		}
		if len(fields) == 4 {
			if !validHubNoteText(message.Text) {
				return hubOutboundMessage{}, false
			}
		} else if len(fields) == 5 {
			var kind string
			if json.Unmarshal(fields["kind"], &kind) != nil || kind != "lane.event" || !validLaneRelayText(message.Text) {
				return hubOutboundMessage{}, false
			}
		} else if len(fields) == 8 {
			if json.Unmarshal(fields["kind"], &message.Kind) != nil || (message.Kind != "lane.event" && message.Kind != "job.completed" && message.Kind != "job.escalate" && message.Kind != "job.joined") {
				return hubOutboundMessage{}, false
			}
			if json.Unmarshal(fields["lane"], &message.Lane) != nil || json.Unmarshal(fields["event_id"], &message.EventID) != nil || json.Unmarshal(fields["deliver"], &message.DeliverPolicy) != nil || !hubAgentLabelPattern.MatchString(message.Lane) || message.EventID < 1 {
				return hubOutboundMessage{}, false
			}
			if _, valid := parseRelayDeliveryPolicy(message.DeliverPolicy); !valid || !validRelayInjectedText(message.Kind, message.Text) {
				return hubOutboundMessage{}, false
			}
		} else {
			return hubOutboundMessage{}, false
		}
	case "relay.edit":
		if len(fields) != 3 || json.Unmarshal(fields["event_id"], &message.EventID) != nil || json.Unmarshal(fields["text"], &message.Text) != nil || message.EventID < 1 || !validRelayFinalText(message.Text) {
			return hubOutboundMessage{}, false
		}
	case "relay.cancel":
		if len(fields) != 2 || json.Unmarshal(fields["event_id"], &message.EventID) != nil || message.EventID < 1 {
			return hubOutboundMessage{}, false
		}
	case "relay.persisted":
		if json.Unmarshal(fields["job_id"], &message.JobID) != nil || json.Unmarshal(fields["kind"], &message.Kind) != nil || json.Unmarshal(fields["epoch"], &message.Epoch) != nil || json.Unmarshal(fields["report_path"], &message.ReportPath) != nil || json.Unmarshal(fields["reason"], &message.Reason) != nil || json.Unmarshal(fields["event_id"], &message.EventID) != nil || !hubJobIDPattern.MatchString(message.JobID) || !relayPersistedKinds[message.Kind] || message.EventID < 1 {
			return hubOutboundMessage{}, false
		}
		if message.Kind == "lane.event" {
			if len(fields) != 9 || json.Unmarshal(fields["lane"], &message.Lane) != nil || json.Unmarshal(fields["producer_event_id"], &message.ProducerEventID) != nil || !hubAgentLabelPattern.MatchString(message.Lane) || !validLaneEventID(message.ProducerEventID) {
				return hubOutboundMessage{}, false
			}
		} else if len(fields) == 8 {
			// Newer hubs echo the producer's event identity so the
			// acknowledgement retires the exact event's outbox row. An
			// older seven-field acknowledgement leaves it empty and keeps
			// the legacy five-field match.
			if json.Unmarshal(fields["producer_event_id"], &message.ProducerEventID) != nil || len(message.ProducerEventID) > 240 {
				return hubOutboundMessage{}, false
			}
		} else if len(fields) != 7 {
			return hubOutboundMessage{}, false
		}
	case "idle-wake.route":
		for name := range fields {
			if name != "type" && name != "event_id" && name != "pane" && name != "eligible" && name != "lane" && name != "text" && name != "job_id" && name != "reason" {
				return hubOutboundMessage{}, false
			}
		}
		if json.Unmarshal(fields["event_id"], &message.IdleWakeEventID) != nil || json.Unmarshal(fields["pane"], &message.Pane) != nil || json.Unmarshal(fields["eligible"], &message.Eligible) != nil {
			return hubOutboundMessage{}, false
		}
		if raw, exists := fields["lane"]; exists && json.Unmarshal(raw, &message.Lane) != nil {
			return hubOutboundMessage{}, false
		}
		if raw, exists := fields["text"]; exists && json.Unmarshal(raw, &message.Text) != nil {
			return hubOutboundMessage{}, false
		}
		if raw, exists := fields["job_id"]; exists && json.Unmarshal(raw, &message.JobID) != nil {
			return hubOutboundMessage{}, false
		}
		if raw, exists := fields["reason"]; exists && json.Unmarshal(raw, &message.Reason) != nil {
			return hubOutboundMessage{}, false
		}
		if !validHubIdleWakeRouteEvent(hubIdleWakeRouteEvent{Type: message.Type, EventID: message.IdleWakeEventID, Pane: message.Pane, Eligible: message.Eligible, Lane: message.Lane, Text: message.Text, JobID: message.JobID, Reason: message.Reason}) {
			return hubOutboundMessage{}, false
		}
	case "update.available":
		if len(fields) != 4 || json.Unmarshal(fields["version"], &message.Version) != nil || json.Unmarshal(fields["sha256"], &message.SHA256) != nil || json.Unmarshal(fields["url"], &message.URL) != nil || !hubVersionPattern.MatchString(message.Version) || !validHubSHA256(message.SHA256) || !validHubUpdateURLForVersion(message.URL, updateRepository, message.Version) {
			return hubOutboundMessage{}, false
		}
	case "quota.request":
		var tool string
		if len(fields) != 3 || json.Unmarshal(fields["request_id"], &message.RequestID) != nil || json.Unmarshal(fields["tool"], &tool) != nil || !validHubRequestID(message.RequestID) || tool != "scopefuel" {
			return hubOutboundMessage{}, false
		}
	case "job.spawn":
		var brief hubSpawnBrief
		var briefFields map[string]json.RawMessage
		if len(fields) != 6 || json.Unmarshal(fields["request_id"], &message.RequestID) != nil || json.Unmarshal(fields["cwd_key"], &message.CWDKey) != nil || json.Unmarshal(fields["brief"], &brief) != nil || json.Unmarshal(fields["brief"], &briefFields) != nil || len(briefFields) != 1 || json.Unmarshal(briefFields["inline"], &brief.Inline) != nil || json.Unmarshal(fields["args"], &message.Args) != nil || message.Args == nil || json.Unmarshal(fields["wait_seconds"], &message.WaitSeconds) != nil {
			return hubOutboundMessage{}, false
		}
		message.BriefInline = brief.Inline
		if !validHubSpawnRequest(hubSpawnRequest{RequestID: message.RequestID, Machine: "machine-a", CWDKey: message.CWDKey, Brief: brief, Args: message.Args, WaitSeconds: message.WaitSeconds}) {
			return hubOutboundMessage{}, false
		}
	default:
		return hubOutboundMessage{}, false
	}
	return message, true
}

func validRelayInjectedText(kind, text string) bool {
	// The hub's normal lane ingress remains bounded by validLaneRelayText;
	// this receiving-side directive also admits the B3 single-item exception.
	return validRelayFinalText(text)
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

// relayPersistedKinds bounds the hub-originated acknowledgement to the three
// relay record kinds. It is a closed set, not a passthrough.
var relayPersistedKinds = map[string]bool{"job.completed": true, "job.escalate": true, "job.joined": true, "lane.event": true}
