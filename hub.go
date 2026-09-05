package panewire

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	hubOperatorMachineID   = "operator"
	hubMachineIDHeader     = "X-Panewire-Machine-ID"
	hubAuthorizationHeader = "Authorization"
	hubMaxMessageBytes     = 32 << 10
	hubUIEventLimit        = 20
	hubVersion             = "panewire-r13"
)

var hubVersionPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// HubServerConfig contains only local, already-loaded static credentials. The
// operator credential is the HUB_TOKEN_operator entry; agent credentials use
// their own stable machine ID entries.
type HubServerConfig struct {
	Tokens map[string]string
	// AlertNodes is the optional watched-node allowlist. A nil map preserves
	// the original behavior and watches every authenticated node. A non-nil
	// map watches only its entries; all other authenticated nodes remain in
	// presence views but never enter the alert state machine.
	AlertNodes        map[string]struct{}
	Now               func() time.Time
	StaleAfter        time.Duration
	KeepaliveInterval time.Duration
	GracePeriod       time.Duration
	// OrphanGrace is the continuous disconnected period before jobs reported by
	// that node are marked orphaned. A zero value deliberately follows the
	// established presence grace rather than creating a second surprise timer.
	OrphanGrace     time.Duration
	Notifier        HubNotifier
	Logger          *slog.Logger
	BurstPolicyPath string
	// PlacementPolicyPath is an optional operator-owned JSON policy. It is
	// deliberately separate from the R12 burst policy because placement never
	// changes pressure thresholds or dispatches work.
	PlacementPolicyPath string
	PrometheusURL       string
	PrometheusClient    *http.Client
	PrometheusBearer    string
	PrometheusBasicUser string
	PrometheusBasicPass string
	// UIAllowCFOnly deliberately requires an explicit operator opt-in before
	// serving the browser UI. UI requests must then originate on loopback or
	// include the identity header injected by Cloudflare Access.
	UIAllowCFOnly bool
	// ReportRelayPath is an operator-owned route file. It is never sent to nodes.
	ReportRelayPath string
	// UpdateConfirmationTimeout bounds how long a published version may wait
	// for the node's post-restart hello. Zero uses the ten-minute contract.
	UpdateConfirmationTimeout time.Duration
	// RelayAckTimeout bounds the time an injected relay may remain silent.
	RelayAckTimeout time.Duration
	// AcceptingOverridesPath optionally makes operator acceptance choices durable.
	AcceptingOverridesPath string
	// handoffkeep is the durable relay-event store. It is package-private so the
	// hub's public configuration keeps no credential-bearing field.
	handoffkeep *handoffkeepRelayClient
}

// HubNode is the deliberately small presence view returned to authenticated
// operators. RemoteMeta records only protocol metadata, never credentials.
type HubNode struct {
	MachineID          string            `json:"machine_id"`
	AlertClass         string            `json:"alert_class"`
	Accepting          bool              `json:"accepting"`
	AcceptingEffective bool              `json:"accepting_effective"`
	AcceptingOverride  string            `json:"accepting_override"`
	ConnectedSince     time.Time         `json:"connected_since"`
	LastPingMS         int64             `json:"last_ping_ms"`
	LastNote           *HubLastNote      `json:"last_note,omitempty"`
	Load               *HubNodeLoad      `json:"load"`
	Memory             *HubHostMemory    `json:"memory"`
	RemoteMeta         map[string]string `json:"remote_meta"`
	State              string            `json:"state"`
}

// HubNodeLoad is the CPU-only console projection of a heartbeat host_load.
// It deliberately excludes burst-only swap and worker-process measurements.
// Every field is present when Load is non-nil; a nil value is an unavailable
// measurement rather than a fabricated zero.
type HubNodeLoad struct {
	Load1  *float64 `json:"load1"`
	Load5  *float64 `json:"load5"`
	Load15 *float64 `json:"load15"`
	NCPU   *int     `json:"ncpu"`
}

// HubLastNote is the most recent display-only note received from a node. It
// is intentionally in-memory only and has no relationship to alert state.
type HubLastNote struct {
	Text       string    `json:"text"`
	ReceivedAt time.Time `json:"received_at"`
}

type hubNodeRecord struct {
	machineID         string
	accepting         bool
	connectedSince    time.Time
	lastPing          time.Time
	lastKeepaliveSent time.Time
	stateSince        time.Time
	remoteMeta        map[string]string
	state             string
	agent             *hubAgent
	activeJobs        map[string]HubActiveJob
	hostLoad          *HubHostLoad
	hostMemory        *HubHostMemory
}

type hubAgent struct {
	conn        *websocket.Conn
	writeMu     sync.Mutex
	transient   bool
	failovers   chan hubFailoverEvent
	bursts      chan hubBurstEvent
	revocations chan hubJobRevokedEvent
	assignments chan hubJobAssignedEvent
	holds       chan hubBurstHoldsEvent
	relays      chan hubRelayInjectEvent
	persisted   chan hubRelayPersistedEvent
}

// HubActiveJob is deliberately metadata-only. It is copied from a node's
// local jobs/*/events files, never from a brief or pane transcript.
type HubActiveJob struct {
	JobID          string `json:"job_id"`
	AgentLabel     string `json:"agent_label"`
	LastEventSeq   uint64 `json:"last_event_seq"`
	PushSHA        string `json:"push_sha,omitempty"`
	Epoch          uint64 `json:"epoch"`
	OwnerLane      string `json:"owner_lane,omitempty"`
	Label          string `json:"label,omitempty"`
	Host           string `json:"host,omitempty"`
	ReportPath     string `json:"report_path,omitempty"`
	ReportLastLine string `json:"report_last_line,omitempty"`
	Pane           string `json:"pane,omitempty"`
	Tier           string `json:"tier,omitempty"`
	Role           string `json:"role,omitempty"`
	StartedAt      string `json:"started_at,omitempty"`
	LastEventKind  string `json:"last_event_kind,omitempty"`
	LastEventAt    string `json:"last_event_at,omitempty"`
}

type hubJobRecord struct {
	HubActiveJob
	Node          string
	LastSeen      time.Time
	Orphaned      bool
	Completed     bool
	FencedNodes   map[string]uint64
	Reassignments []hubJobReassignment
}

// hubJobReassignment is retained in-memory as an audit trail for every
// predecessor that must remain fenced across later redispatches.
type hubJobReassignment struct {
	From  string
	To    string
	Epoch uint64
	At    time.Time
}

type hubJobEventPayload struct {
	// lane.event uses these relay-only fields. It deliberately never enters
	// the hub's job registration paths despite sharing the delivery envelope.
	EventID        string    `json:"event_id,omitempty"`
	Text           string    `json:"text,omitempty"`
	Truncated      bool      `json:"truncated,omitempty"`
	JobID          string    `json:"job_id"`
	Node           string    `json:"node,omitempty"`
	From           string    `json:"from,omitempty"`
	To             string    `json:"to,omitempty"`
	Epoch          uint64    `json:"epoch"`
	AgentLabel     string    `json:"agent_label,omitempty"`
	LastSeen       time.Time `json:"last_seen,omitempty"`
	ResumeHint     string    `json:"resume_hint,omitempty"`
	OwnerLane      string    `json:"owner_lane,omitempty"`
	Label          string    `json:"label,omitempty"`
	Host           string    `json:"host,omitempty"`
	ReportPath     string    `json:"report_path,omitempty"`
	ReportLastLine string    `json:"report_last_line,omitempty"`
	Reason         string    `json:"reason,omitempty"`
	Question       string    `json:"question,omitempty"`
	PR             string    `json:"pr,omitempty"`
	Head           string    `json:"head,omitempty"`
	PaneID         string    `json:"pane_id,omitempty"`
	// Replay marks a record a node had already sent before it restarted.
	Replay bool `json:"replay,omitempty"`
}

type relayAckPayload struct {
	JobID  string `json:"job_id"`
	Pane   string `json:"pane"`
	Reason string `json:"reason,omitempty"`
}

type hubRelayInjectEvent struct {
	Type  string `json:"type"`
	Kind  string `json:"kind,omitempty"`
	JobID string `json:"job_id"`
	Pane  string `json:"pane"`
	Text  string `json:"text"`
}

type hubJobRevokedEvent struct {
	Type  string `json:"type"`
	JobID string `json:"job_id"`
	Epoch uint64 `json:"epoch"`
}

// hubRelayPersistedEvent tells a node that handoffkeep now owns this record,
// so the node may retire it from its local outbox. It carries the same dedupe
// fields the node keyed the outbox row by.
type hubRelayPersistedEvent struct {
	Type            string `json:"type"`
	JobID           string `json:"job_id"`
	Kind            string `json:"kind"`
	Epoch           uint64 `json:"epoch"`
	ReportPath      string `json:"report_path"`
	Reason          string `json:"reason"`
	EventID         int64  `json:"event_id"`
	Lane            string `json:"lane,omitempty"`
	ProducerEventID string `json:"producer_event_id,omitempty"`
}

// hubJobAssignedEvent is the hub-issued epoch a redispatched owner must use.
// It deliberately carries no work body or execution instruction.
type hubJobAssignedEvent struct {
	Type  string `json:"type"`
	JobID string `json:"job_id"`
	Epoch uint64 `json:"epoch"`
}

type hubBurstHoldsEvent struct {
	Type        string `json:"type"`
	HoldsActive bool   `json:"holds_active"`
}

type hubEvent struct {
	MachineID string          `json:"machine_id"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
	Received  time.Time       `json:"received_at"`
}

type hubEventSubscriber struct {
	conn     *websocket.Conn
	ctx      context.Context
	cancel   context.CancelFunc
	messages chan hubSubscriptionMessage
}

// hubUIEvent is a closed, display-only event record. It never retains event
// payloads, so data intended for an authenticated operator websocket cannot
// accidentally become browser-visible.
type hubUIEvent struct {
	Kind      string    `json:"kind"`
	Phase     string    `json:"phase"`
	MachineID string    `json:"machine_id"`
	JobID     string    `json:"job_id,omitempty"`
	At        time.Time `json:"at"`
}

// hubSubscriptionMessage keeps the operator event stream's vocabulary
// explicit. Agent events retain their existing envelope; server-originated
// failover messages have their own fixed four-field shape.
type hubSubscriptionMessage struct {
	event    *hubEvent
	failover *hubFailoverEvent
}

// HubServer is an in-memory presence and event relay. It deliberately owns no
// file relay, durable queue, command dispatcher, or credentials after startup.
type HubServer struct {
	tokens            map[string]string
	alertNodes        map[string]struct{}
	now               func() time.Time
	staleAfter        time.Duration
	keepaliveInterval time.Duration
	gracePeriod       time.Duration
	orphanGrace       time.Duration
	alertObservations int
	notifier          HubNotifier
	logger            *slog.Logger

	mu                     sync.Mutex
	nodes                  map[string]*hubNodeRecord
	lastNotes              map[string]*HubLastNote
	subscribers            map[*hubEventSubscriber]struct{}
	alerts                 map[string]*hubAlertState
	burstPolicyPath        string
	burstPolicy            BurstPolicy
	burstPolicyModTime     time.Time
	burstState             *hubBurstState
	unknownMessages        uint64
	unfencedCompletions    uint64
	startedAt              time.Time
	uiAllowCFOnly          bool
	uiEvents               []hubUIEvent
	jobs                   map[string]*hubJobRecord
	pendingRevocations     map[string]map[string]hubJobRevokedEvent
	holds                  map[string]*hubBurstHold
	placementPolicyPath    string
	placementPolicy        PlacementPolicy
	placementPolicyModTime time.Time
	prometheusURL          string
	prometheusClient       *http.Client
	prometheusBearer       string
	prometheusBasicUser    string
	prometheusBasicPass    string
	placementCache         placementCache
	r19a                   r19aHubState
	reportRelayPath        string
	// relayDedupe is an active injection claim. lanePersisted keeps the durable
	// row ID while that claim is deliberately released between lane retries.
	relayDedupe                 map[string]int64
	lanePersisted               map[string]int64
	lanePersistedOrder          lruIndex[string]
	replayExhausted             map[int64]struct{}
	replayExhaustedOrder        lruIndex[int64]
	handoffkeep                 *handoffkeepRelayClient
	unpersistedRelayEvents      uint64
	replayExhaustedEvents       uint64
	alreadyDeliveredRelayEvents uint64
	quotaCache                  map[string]hubQuotaCacheEntry
	quotaWaiters                map[string]chan hubQuotaResult
	quotaCacheTTL               time.Duration
	expectedVersion             map[string]hubExpectedVersion
	updateConfirmationTimeout   time.Duration
}

type hubExpectedVersion struct {
	version  string
	deadline time.Time
}

// NewHubServer validates a complete static-token configuration. Tokens remain
// private to the server and are never included in errors or logs.
func NewHubServer(config HubServerConfig) (*HubServer, error) {
	if len(config.Tokens) == 0 {
		return nil, errors.New("hub token configuration is required")
	}
	tokens := make(map[string]string, len(config.Tokens))
	for machineID, token := range config.Tokens {
		if !machineIDPattern.MatchString(machineID) || !validHubToken(token) {
			return nil, errors.New("hub token configuration is invalid")
		}
		tokens[machineID] = token
	}
	if !validHubToken(tokens[hubOperatorMachineID]) {
		return nil, errors.New("hub operator token is required")
	}
	var alertNodes map[string]struct{}
	if config.AlertNodes != nil {
		alertNodes = make(map[string]struct{}, len(config.AlertNodes))
		for machineID := range config.AlertNodes {
			if machineID == hubOperatorMachineID || !machineIDPattern.MatchString(machineID) {
				return nil, errors.New("hub alert node configuration is invalid")
			}
			if _, exists := tokens[machineID]; !exists {
				return nil, errors.New("hub alert node configuration is invalid")
			}
			alertNodes[machineID] = struct{}{}
		}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.StaleAfter <= 0 {
		config.StaleAfter = 30 * time.Second
	}
	if config.KeepaliveInterval <= 0 {
		config.KeepaliveInterval = 10 * time.Second
	}
	if config.GracePeriod <= 0 {
		config.GracePeriod = defaultHubGracePeriod
	}
	if config.OrphanGrace <= 0 {
		config.OrphanGrace = config.GracePeriod
	}
	if config.UpdateConfirmationTimeout <= 0 {
		config.UpdateConfirmationTimeout = 10 * time.Minute
	}
	var burstPolicy BurstPolicy
	var burstPolicyModTime time.Time
	if config.BurstPolicyPath != "" {
		policy, modTime, err := LoadBurstPolicy(config.BurstPolicyPath)
		if err != nil {
			return nil, errors.New("hub burst policy is invalid")
		}
		burstPolicy, burstPolicyModTime = policy, modTime
	}
	var placementPolicy PlacementPolicy
	var placementPolicyModTime time.Time
	if config.PlacementPolicyPath != "" {
		policy, modTime, err := LoadPlacementPolicy(config.PlacementPolicyPath)
		if err != nil {
			return nil, errors.New("hub placement policy is invalid")
		}
		placementPolicy, placementPolicyModTime = policy, modTime
	}
	if config.RelayAckTimeout <= 0 {
		config.RelayAckTimeout = relayAckTimeoutFromEnv()
	}
	overrides, err := loadAcceptingOverrides(config.AcceptingOverridesPath)
	if err != nil {
		return nil, errors.New("hub accepting overrides are invalid")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &HubServer{
		tokens: tokens, alertNodes: alertNodes, r19a: newR19aHubState(config, overrides), now: config.Now, staleAfter: config.StaleAfter, keepaliveInterval: config.KeepaliveInterval,
		gracePeriod: config.GracePeriod, orphanGrace: config.OrphanGrace, alertObservations: defaultHubAlertObservations, notifier: config.Notifier, logger: config.Logger, burstPolicyPath: config.BurstPolicyPath,
		placementPolicyPath: config.PlacementPolicyPath, placementPolicy: placementPolicy, placementPolicyModTime: placementPolicyModTime, prometheusURL: config.PrometheusURL, prometheusClient: config.PrometheusClient, prometheusBearer: config.PrometheusBearer, prometheusBasicUser: config.PrometheusBasicUser, prometheusBasicPass: config.PrometheusBasicPass,
		nodes: make(map[string]*hubNodeRecord), lastNotes: make(map[string]*HubLastNote), subscribers: make(map[*hubEventSubscriber]struct{}), alerts: make(map[string]*hubAlertState), burstPolicy: burstPolicy, burstPolicyModTime: burstPolicyModTime, burstState: &hubBurstState{}, startedAt: config.Now().UTC(), uiAllowCFOnly: config.UIAllowCFOnly, jobs: make(map[string]*hubJobRecord), pendingRevocations: make(map[string]map[string]hubJobRevokedEvent), holds: make(map[string]*hubBurstHold), reportRelayPath: config.ReportRelayPath, relayDedupe: make(map[string]int64), lanePersisted: make(map[string]int64), replayExhausted: make(map[int64]struct{}), handoffkeep: config.handoffkeep, quotaCache: make(map[string]hubQuotaCacheEntry), quotaWaiters: make(map[string]chan hubQuotaResult), quotaCacheTTL: hubQuotaCacheTTL(), expectedVersion: make(map[string]hubExpectedVersion), updateConfirmationTimeout: config.UpdateConfirmationTimeout,
	}, nil
}

func (h *HubServer) alertClass(machineID string) string {
	if h.alertNodes == nil {
		return "watched"
	}
	if _, watched := h.alertNodes[machineID]; watched {
		return "watched"
	}
	return "presence-only"
}

func (h *HubServer) watchesAlerts(machineID string) bool {
	return h.alertClass(machineID) == "watched"
}

func validHubToken(token string) bool {
	return token != "" && len(token) <= 512 && !strings.ContainsAny(token, "\x00\r\n\t ")
}

// Handler exposes the three v1 hub endpoints. The caller is responsible for
// binding it only on loopback; hubListenAddress enforces that CLI invariant.
func (h *HubServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.handleHealthz)
	mux.HandleFunc("GET /ui", h.handleUI)
	mux.HandleFunc("GET /ui/data.json", h.handleUIData)
	mux.HandleFunc("GET /v1/nodes", h.handleNodes)
	mux.HandleFunc("POST /v1/nodes/{machine}/accepting", h.handleAcceptingOverride)
	mux.HandleFunc("GET /v1/burst", h.handleBurst)
	mux.HandleFunc("POST /v1/burst/request", h.handleBurstRequest)
	mux.HandleFunc("POST /v1/burst/release", h.handleBurstRelease)
	mux.HandleFunc("GET /v1/burst/holds", h.handleBurstHolds)
	mux.HandleFunc("GET /v1/placement", h.handlePlacement)
	mux.HandleFunc("GET /v1/jobs", h.handleJobs)
	mux.HandleFunc("GET /v1/jobs/orphaned", h.handleOrphanedJobs)
	mux.HandleFunc("POST /v1/jobs/reassign", h.handleReassignJob)
	mux.HandleFunc("GET /v1/agent", h.handleAgent)
	mux.HandleFunc("GET /v1/events", h.handleEvents)
	mux.HandleFunc("POST /v1/update", h.handleUpdatePublish)
	mux.HandleFunc("GET /v1/quota/{machine}", h.handleQuotaGet)
	mux.HandleFunc("POST /v1/quota/{machine}", h.handleQuotaRequest)
	return mux
}

//go:embed hub_ui.html
var hubUIHTML string

var hubUITemplate = template.Must(template.New("hub-ui").Parse(hubUIHTML))

type hubUIData struct {
	SchemaVersion int          `json:"schema_version"`
	Hub           hubUIHub     `json:"hub"`
	Nodes         []hubUINode  `json:"nodes"`
	Burst         hubUIBurst   `json:"burst"`
	Events        []hubUIEvent `json:"events"`
}

type hubUIHub struct {
	Version             string `json:"version"`
	UptimeMS            int64  `json:"uptime_ms"`
	UnknownMessages     uint64 `json:"unknown_messages"`
	UnfencedCompletions uint64 `json:"unfenced_completions"`
}

type hubUINode struct {
	MachineID          string    `json:"machine_id"`
	State              string    `json:"state"`
	StateSince         time.Time `json:"state_since"`
	AlertClass         string    `json:"alert_class"`
	Accepting          bool      `json:"accepting"`
	AcceptingEffective bool      `json:"accepting_effective"`
	AcceptingOverride  string    `json:"accepting_override"`
	LastPingMS         int64     `json:"last_ping_ms"`
	Version            string    `json:"version"`
}

type hubUIBurstPolicy struct {
	SourceMachine   string  `json:"source_machine"`
	TargetMachine   string  `json:"target_machine"`
	SwapGB          float64 `json:"swap_gb"`
	Load5           float64 `json:"load5"`
	Consecutive     int     `json:"consecutive"`
	IdleMinutes     int     `json:"idle_minutes"`
	CooldownMinutes int     `json:"cooldown_minutes"`
}

type hubUIBurst struct {
	Configured  bool              `json:"configured"`
	Policy      *hubUIBurstPolicy `json:"policy"`
	SourceRuns  int               `json:"source_runs"`
	LastLoad    HubHostLoad       `json:"last_load"`
	LastUp      time.Time         `json:"last_up"`
	LastDown    time.Time         `json:"last_down"`
	UpCompleted bool              `json:"up_completed"`
	IdleSince   time.Time         `json:"idle_since"`
}

func (h *HubServer) handleUI(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeUI(request) {
		http.NotFound(writer, request)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := hubUITemplate.Execute(writer, nil); err != nil {
		h.logger.Error("hub ui template failed")
	}
}

func (h *HubServer) handleUIData(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeUI(request) {
		http.NotFound(writer, request)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(h.uiData())
}

// authorizeUI intentionally does not trust forwarded-address headers. A
// Cloudflare-routed request is accepted only when Access injected an identity;
// loopback is reserved for a truly local request with no Cloudflare headers.
func (h *HubServer) authorizeUI(request *http.Request) bool {
	if !h.uiAllowCFOnly {
		return false
	}
	accessIdentity := strings.TrimSpace(request.Header.Get("Cf-Access-Authenticated-User-Email")) != ""
	cloudflareRouted := strings.TrimSpace(request.Header.Get("Cf-Ray")) != "" || strings.TrimSpace(request.Header.Get("Cf-Connecting-Ip")) != ""
	if cloudflareRouted {
		return accessIdentity
	}
	if accessIdentity {
		return true
	}
	host, _, err := net.SplitHostPort(request.RemoteAddr)
	if err != nil {
		host = request.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (h *HubServer) uiData() hubUIData {
	now := h.now().UTC()
	h.mu.Lock()
	defer h.mu.Unlock()
	nodes := make([]hubUINode, 0, len(h.nodes))
	for _, record := range h.nodes {
		age := now.Sub(record.lastPing)
		if age < 0 {
			age = 0
		}
		alertClass := "unwatched"
		if h.watchesAlerts(record.machineID) {
			alertClass = "watched"
		}
		effective := h.acceptingEffectiveLocked(record.machineID, record.accepting)
		nodes = append(nodes, hubUINode{MachineID: record.machineID, State: record.state, StateSince: record.stateSince, AlertClass: alertClass, Accepting: effective, AcceptingEffective: effective, AcceptingOverride: h.acceptingOverrideLocked(record.machineID), LastPingMS: age.Milliseconds(), Version: record.remoteMeta["version"]})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].MachineID < nodes[j].MachineID })
	var policy *hubUIBurstPolicy
	if h.burstPolicyPath != "" {
		h.reloadBurstPolicyLocked()
		policy = &hubUIBurstPolicy{SourceMachine: h.burstPolicy.SourceMachine, TargetMachine: h.burstPolicy.TargetMachine, SwapGB: h.burstPolicy.SwapGB, Load5: h.burstPolicy.Load5, Consecutive: h.burstPolicy.Consecutive, IdleMinutes: h.burstPolicy.IdleMinutes, CooldownMinutes: h.burstPolicy.CooldownMinutes}
	}
	events := append([]hubUIEvent(nil), h.uiEvents...)
	uptime := now.Sub(h.startedAt)
	if uptime < 0 {
		uptime = 0
	}
	state := *h.burstState
	return hubUIData{SchemaVersion: 1, Hub: hubUIHub{Version: hubVersion, UptimeMS: uptime.Milliseconds(), UnknownMessages: h.unknownMessages, UnfencedCompletions: h.unfencedCompletions}, Nodes: nodes, Burst: hubUIBurst{Configured: h.burstPolicyPath != "", Policy: policy, SourceRuns: state.SourceRuns, LastLoad: state.LastLoad, LastUp: state.LastUp, LastDown: state.LastDown, UpCompleted: state.UpCompleted, IdleSince: state.IdleSince}, Events: events}
}

func (h *HubServer) handleBurst(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	policy, state, configured := h.burstStatus()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(struct {
		Configured  bool        `json:"configured"`
		Policy      BurstPolicy `json:"policy,omitempty"`
		SourceRuns  int         `json:"source_runs"`
		IdleSince   time.Time   `json:"idle_since,omitempty"`
		LastUp      time.Time   `json:"last_up,omitempty"`
		LastDown    time.Time   `json:"last_down,omitempty"`
		UpCompleted bool        `json:"up_completed"`
		LastLoad    HubHostLoad `json:"last_load"`
	}{configured, policy, state.SourceRuns, state.IdleSince, state.LastUp, state.LastDown, state.UpCompleted, state.LastLoad})
}

func (h *HubServer) handleOrphanedJobs(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(struct {
		Jobs []hubJobEventPayload `json:"jobs"`
	}{Jobs: h.orphanedJobs()})
}

func (h *HubServer) handleJobs(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	machine := request.URL.Query().Get("machine")
	if machine != "" && !machineIDPattern.MatchString(machine) {
		http.Error(writer, "invalid machine", http.StatusBadRequest)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(struct {
		Jobs []hubConsoleJob `json:"jobs"`
	}{Jobs: h.activeConsoleJobs(machine)})
}

func (h *HubServer) handleReassignJob(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	var requestBody struct {
		JobID string `json:"job_id"`
		To    string `json:"to"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&requestBody) != nil {
		http.Error(writer, "invalid job reassignment", http.StatusBadRequest)
		return
	}
	result, ok := h.reassignJob(requestBody.JobID, requestBody.To)
	if !ok {
		http.Error(writer, "job reassignment rejected", http.StatusConflict)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(result)
}

func (h *HubServer) handleHealthz(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(`{"status":"ok"}\n`))
}

func (h *HubServer) handleNodes(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(struct {
		Nodes []HubNode `json:"nodes"`
	}{Nodes: h.Nodes()})
}

func (h *HubServer) handleAgent(writer http.ResponseWriter, request *http.Request) {
	machineID, ok := h.authorizeAgent(request)
	if !ok {
		hubUnauthorized(writer)
		return
	}
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	connection.SetReadLimit(hubMaxMessageBytes)
	agent := &hubAgent{conn: connection, failovers: make(chan hubFailoverEvent, 64), bursts: make(chan hubBurstEvent, 64), revocations: make(chan hubJobRevokedEvent, 64), assignments: make(chan hubJobAssignedEvent, 64), holds: make(chan hubBurstHoldsEvent, 4), relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	defer connection.CloseNow()
	agentContext, agentCancel := context.WithCancel(request.Context())
	defer agentCancel()
	go agent.writeFailovers(agentContext)
	go agent.writeBursts(agentContext)
	go agent.writeRevocations(agentContext)
	go agent.writeAssignments(agentContext)
	go agent.writeHolds(agentContext)
	go agent.writeRelays(agentContext)
	go agent.writePersisted(agentContext)
	for {
		messageType, payload, err := connection.Read(request.Context())
		if err != nil {
			h.disconnect(machineID, agent)
			return
		}
		if messageType != websocket.MessageText {
			h.countUnknownMessage()
			continue
		}
		h.handleAgentMessage(machineID, request.RemoteAddr, agent, payload)
	}
}

func (h *HubServer) handleAgentMessage(machineID, remoteAddr string, agent *hubAgent, payload []byte) {
	if report, ok := parseHubQuotaReport(payload); ok {
		h.resolveQuota(machineID, report)
		return
	}
	message, ok := parseHubInbound(payload)
	if !ok {
		h.countUnknownMessage()
		return
	}
	switch message.Type {
	case "hello":
		if message.MachineID != machineID || !machineIDPattern.MatchString(message.MachineID) || !hubVersionPattern.MatchString(message.Version) {
			h.countUnknownMessage()
			return
		}
		if agent.transient {
			h.countUnknownMessage()
			return
		}
		if message.Transient {
			agent.transient = true
			return
		}
		h.connect(machineID, message.Version, remoteAddr, agent, message.Accepting)
	case "ping", "pong":
		if agent.transient {
			h.countUnknownMessage()
			return
		}
		if !h.touch(machineID, agent) {
			h.countUnknownMessage()
			return
		}
		if message.Type == "ping" {
			_ = agent.writeJSON(hubOutbound{Type: "pong"})
		}
	case "update.busy":
		// The node already had a self-update running and declined this one.
		// Recording it keeps the published version's expectation pending until
		// its own deadline rather than silently waiting on a restart that the
		// node was never going to perform for this instruction.
		if agent.transient || !h.touch(machineID, agent) {
			h.countUnknownMessage()
			return
		}
		h.mu.Lock()
		h.recordUIEventLocked("update", "busy", machineID, h.now().UTC())
		h.mu.Unlock()
	case "event":
		if !agent.transient && !h.touch(machineID, agent) {
			h.countUnknownMessage()
			return
		}
		received := h.now().UTC()
		if message.Kind == "heartbeat" {
			heartbeat, valid := decodeHubHeartbeatPayload(message.Payload)
			if !valid {
				h.countUnknownMessage()
				return
			}
			h.mu.Lock()
			if record := h.nodes[machineID]; record != nil && record.agent == agent {
				record.hostLoad = cloneHubHostLoad(heartbeat.HostLoad)
				if !equalHubHostMemory(record.hostMemory, heartbeat.HostMemory) {
					record.hostMemory = cloneHubHostMemory(heartbeat.HostMemory)
					h.placementCache = placementCache{}
				}
			}
			h.mu.Unlock()
			h.observeHeartbeatAlerts(machineID, heartbeat)
			h.observeActiveJobs(machineID, heartbeat.ActiveJobs, received)
			if heartbeat.HostLoad != nil {
				h.mu.Lock()
				bursts := h.observeBurstLocked(received, machineID, *heartbeat.HostLoad)
				h.mu.Unlock()
				for _, burst := range bursts {
					h.dispatchBurst(burst)
				}
			}
			h.sendHeartbeatDirectives(machineID, agent)
		}
		if message.Kind == "job.completed" {
			completion, valid := decodeHubJobCompletionPayload(message.Payload)
			if !valid {
				h.countUnknownMessage()
				return
			}
			// R14 fencing answers "may this job be redispatched"; R18 relay
			// answers "must this report reach its lane". They are different
			// questions, so a completion the hub never registered - a job that
			// finished before any heartbeat carried it, or one outside the
			// node-side active-scan cap - is still relayed. `job.escalate` and
			// `job.joined` below are relayed on the same terms.
			if !h.observeJobCompletion(machineID, completion, received) {
				h.countUnfencedCompletion()
				h.logger.Info("completion relayed without job registration", "job", completion.JobID, "node", machineID)
				h.lateRegisterJobCompletion(machineID, completion, received)
			}
			if completion.Replay {
				h.logger.Info("relay record replayed after node restart", "job", completion.JobID, "kind", message.Kind, "node", machineID)
			}
			h.relayJobCompletion(completion)
		}
		if message.Kind == "job.escalate" || message.Kind == "job.joined" {
			event, truncated, valid := decodeHubJobEscalationPayloadDetailed(message.Payload)
			if !valid {
				h.countUnknownMessage()
				return
			}
			for _, field := range truncated {
				h.logger.Warn("relay payload truncated", "field", field, "job", event.JobID)
			}
			if event.Replay {
				h.logger.Info("relay record replayed after node restart", "job", event.JobID, "kind", message.Kind, "node", machineID)
			}
			h.relayJobEvent(message.Kind, event)
		}
		if message.Kind == "lane.event" {
			event, valid := decodeHubLaneEventPayload(message.Payload)
			if !valid {
				h.countUnknownMessage()
				return
			}
			h.relayLaneEvent(event, agent)
		}
		if message.Kind == "relay.delivered" || message.Kind == "relay.unconfirmed" {
			ack, valid := decodeRelayAckPayload(message.Payload)
			if !valid {
				h.countUnknownMessage()
				return
			}
			pending, acknowledged := h.acknowledgeRelayPending(machineID, ack)
			if !acknowledged {
				h.countUnknownMessage()
				return
			}
			if message.Kind == "relay.delivered" {
				h.markRelayEventDelivered(pending)
			}
		}
		if message.Kind == "job.revocation.ack" {
			ack, valid := decodeHubJobCompletionPayload(message.Payload)
			if !valid || !h.acknowledgeRevocation(machineID, ack) {
				h.countUnknownMessage()
				return
			}
		}
		if message.Kind == "note" {
			// `note` was reserved before R9 and may be consumed by existing
			// subscribers with their own payload. Only the canonical text shape
			// updates the display state; every valid event object still follows
			// the established broadcast path.
			if note, valid := decodeHubNotePayload(message.Payload); valid {
				h.recordNote(machineID, note.Text, received)
			}
		}
		h.broadcast(hubEvent{MachineID: machineID, Kind: message.Kind, Payload: append(json.RawMessage(nil), message.Payload...), Received: received})
	default:
		h.countUnknownMessage()
	}
}

type hubInbound struct {
	Type      string
	MachineID string
	Version   string
	Transient bool
	Accepting bool
	Kind      string
	Payload   json.RawMessage
}

type hubHeartbeatPayload struct {
	Status      string                    `json:"status"`
	Checks      map[string]HubCheckStatus `json:"checks"`
	HostLoad    *HubHostLoad              `json:"host_load,omitempty"`
	HostMemory  *HubHostMemory            `json:"host_memory,omitempty"`
	ActiveJobs  []HubActiveJob            `json:"active_jobs,omitempty"`
	HoldsActive bool                      `json:"holds_active,omitempty"`
}

type hubNotePayload struct {
	Text string `json:"text"`
}

func validHubNoteText(text string) bool {
	return len(text) <= 512 && !strings.ContainsAny(text, "\r\n")
}

func decodeHubNotePayload(payload []byte) (hubNotePayload, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || len(fields) != 1 {
		return hubNotePayload{}, false
	}
	rawText, exists := fields["text"]
	if !exists {
		return hubNotePayload{}, false
	}
	var note hubNotePayload
	if json.Unmarshal(rawText, &note.Text) != nil || !validHubNoteText(note.Text) {
		return hubNotePayload{}, false
	}
	return note, true
}

func decodeHubHeartbeatPayload(payload []byte) (hubHeartbeatPayload, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || fields == nil {
		return hubHeartbeatPayload{}, false
	}
	for name := range fields {
		if name != "status" && name != "checks" && name != "host_load" && name != "host_memory" && name != "active_jobs" && name != "holds_active" {
			return hubHeartbeatPayload{}, false
		}
	}
	rawStatus, exists := fields["status"]
	if !exists {
		return hubHeartbeatPayload{}, false
	}
	var heartbeat hubHeartbeatPayload
	if json.Unmarshal(rawStatus, &heartbeat.Status) != nil || heartbeat.Status != "alive" {
		return hubHeartbeatPayload{}, false
	}
	heartbeat.Checks = make(map[string]HubCheckStatus)
	if rawHolds, exists := fields["holds_active"]; exists && json.Unmarshal(rawHolds, &heartbeat.HoldsActive) != nil {
		return hubHeartbeatPayload{}, false
	}
	if rawChecks, exists := fields["checks"]; exists {
		if json.Unmarshal(rawChecks, &heartbeat.Checks) != nil || heartbeat.Checks == nil {
			return hubHeartbeatPayload{}, false
		}
	}
	if rawLoad, exists := fields["host_load"]; exists {
		var loadFields map[string]json.RawMessage
		if json.Unmarshal(rawLoad, &loadFields) != nil || (len(loadFields) != 4 && len(loadFields) != 6) {
			return hubHeartbeatPayload{}, false
		}
		for name := range loadFields {
			if name != "load1" && name != "load5" && name != "swap_used_gb" && name != "worker_procs" && name != "load15" && name != "ncpu" {
				return hubHeartbeatPayload{}, false
			}
		}
		for _, name := range []string{"load1", "load5", "swap_used_gb", "worker_procs"} {
			if _, exists := loadFields[name]; !exists {
				return hubHeartbeatPayload{}, false
			}
		}
		if len(loadFields) == 6 {
			if _, exists := loadFields["load15"]; !exists {
				return hubHeartbeatPayload{}, false
			}
			if _, exists := loadFields["ncpu"]; !exists {
				return hubHeartbeatPayload{}, false
			}
		}
		var load HubHostLoad
		if json.Unmarshal(rawLoad, &load) != nil || !load.valid() {
			return hubHeartbeatPayload{}, false
		}
		heartbeat.HostLoad = &load
	}
	if rawMemory, exists := fields["host_memory"]; exists {
		var memoryFields map[string]json.RawMessage
		if json.Unmarshal(rawMemory, &memoryFields) != nil || len(memoryFields) != 5 {
			return hubHeartbeatPayload{}, false
		}
		for _, name := range []string{"free_pct", "compressed_mb", "swap_used_mb", "psi_some_avg10", "source"} {
			if _, exists := memoryFields[name]; !exists {
				return hubHeartbeatPayload{}, false
			}
		}
		var memory HubHostMemory
		if json.Unmarshal(rawMemory, &memory) != nil || !memory.valid() {
			return hubHeartbeatPayload{}, false
		}
		heartbeat.HostMemory = &memory
	}
	if rawJobs, exists := fields["active_jobs"]; exists {
		var rawActive []map[string]json.RawMessage
		if json.Unmarshal(rawJobs, &rawActive) != nil || len(rawActive) > 32 {
			return hubHeartbeatPayload{}, false
		}
		heartbeat.ActiveJobs = make([]HubActiveJob, 0, len(rawActive))
		seen := make(map[string]struct{}, len(heartbeat.ActiveJobs))
		for _, rawJob := range rawActive {
			if len(rawJob) < 4 {
				return hubHeartbeatPayload{}, false
			}
			for name := range rawJob {
				if name != "job_id" && name != "agent_label" && name != "last_event_seq" && name != "push_sha" && name != "epoch" && name != "owner_lane" && name != "pane" && name != "tier" && name != "role" && name != "started_at" && name != "last_event_kind" && name != "last_event_at" {
					return hubHeartbeatPayload{}, false
				}
			}
			for _, name := range []string{"job_id", "agent_label", "last_event_seq", "epoch"} {
				if _, present := rawJob[name]; !present {
					return hubHeartbeatPayload{}, false
				}
			}
			encoded, _ := json.Marshal(rawJob)
			var job HubActiveJob
			if json.Unmarshal(encoded, &job) != nil {
				return hubHeartbeatPayload{}, false
			}
			if !validHubActiveJob(job) {
				return hubHeartbeatPayload{}, false
			}
			normalizeHubActiveJobMetadata(&job)
			if _, duplicate := seen[job.JobID]; duplicate {
				return hubHeartbeatPayload{}, false
			}
			seen[job.JobID] = struct{}{}
			heartbeat.ActiveJobs = append(heartbeat.ActiveJobs, job)
		}
	}
	for name, status := range heartbeat.Checks {
		if !hubCheckNamePattern.MatchString(name) || !status.valid() {
			return hubHeartbeatPayload{}, false
		}
	}
	return heartbeat, true
}

type hubOutbound struct {
	Type string `json:"type"`
}

// parseHubInbound accepts only the v1 envelope vocabulary. Unknown fields,
// kinds, malformed JSON, and messages with an invalid payload are ignored by
// the caller rather than terminating an otherwise authenticated connection.
func parseHubInbound(payload []byte) (hubInbound, bool) {
	var fields map[string]json.RawMessage
	if len(payload) == 0 || json.Unmarshal(payload, &fields) != nil || fields == nil {
		return hubInbound{}, false
	}
	var message hubInbound
	if rawType, exists := fields["type"]; !exists || json.Unmarshal(rawType, &message.Type) != nil {
		return hubInbound{}, false
	}
	allowed := func(names ...string) bool {
		for name := range fields {
			known := false
			for _, allowedName := range names {
				if name == allowedName {
					known = true
					break
				}
			}
			if !known {
				return false
			}
		}
		return true
	}
	switch message.Type {
	case "hello":
		if !allowed("type", "machine_id", "version", "transient", "accepting") || json.Unmarshal(fields["machine_id"], &message.MachineID) != nil || json.Unmarshal(fields["version"], &message.Version) != nil || message.MachineID == "" || message.Version == "" {
			return hubInbound{}, false
		}
		if rawTransient, exists := fields["transient"]; exists && json.Unmarshal(rawTransient, &message.Transient) != nil {
			return hubInbound{}, false
		}
		if rawAccepting, exists := fields["accepting"]; exists && json.Unmarshal(rawAccepting, &message.Accepting) != nil {
			return hubInbound{}, false
		}
	case "ping", "pong", "update.busy":
		if !allowed("type") {
			return hubInbound{}, false
		}
	case "event":
		if !allowed("type", "kind", "payload") || json.Unmarshal(fields["kind"], &message.Kind) != nil || message.Kind == "" {
			return hubInbound{}, false
		}
		rawPayload, exists := fields["payload"]
		if !exists || len(rawPayload) == 0 || !json.Valid(rawPayload) {
			return hubInbound{}, false
		}
		var object map[string]json.RawMessage
		if json.Unmarshal(rawPayload, &object) != nil || object == nil {
			return hubInbound{}, false
		}
		if !knownHubEventKind(message.Kind) {
			return hubInbound{}, false
		}
		if message.Kind == "heartbeat" {
			if _, valid := decodeHubHeartbeatPayload(rawPayload); !valid {
				return hubInbound{}, false
			}
		}
		if message.Kind == "job.completed" || message.Kind == "job.revocation.ack" {
			if _, valid := decodeHubJobCompletionPayload(rawPayload); !valid {
				return hubInbound{}, false
			}
		}
		if message.Kind == "job.escalate" || message.Kind == "job.joined" {
			if _, valid := decodeHubJobEscalationPayload(rawPayload); !valid {
				return hubInbound{}, false
			}
		}
		if message.Kind == "lane.event" {
			if _, valid := decodeHubLaneEventPayload(rawPayload); !valid {
				return hubInbound{}, false
			}
		}
		if message.Kind == "relay.delivered" || message.Kind == "relay.unconfirmed" {
			if _, valid := decodeRelayAckPayload(rawPayload); !valid {
				return hubInbound{}, false
			}
		}
		message.Payload = append(json.RawMessage(nil), rawPayload...)
	default:
		return hubInbound{}, false
	}
	return message, true
}

func knownHubEventKind(kind string) bool {
	switch kind {
	case "heartbeat", "note", "job.completed", "job.escalate", "job.joined", "lane.event", "job.revocation.ack", "relay.delivered", "relay.unconfirmed":
		return true
	}
	return false
}

func (h *HubServer) connect(machineID, version, remoteAddr string, agent *hubAgent, accepting bool) {
	// A node registration is the safe no-restart retry trigger for durable
	// lane.event rows whose route was absent or disconnected when produced.
	defer func() { go h.replayUndeliveredLaneEvents(context.Background()) }()
	now := h.now().UTC()
	var previous *hubAgent
	h.mu.Lock()
	if existing := h.nodes[machineID]; existing != nil {
		previous = existing.agent
	}
	h.nodes[machineID] = &hubNodeRecord{
		machineID: machineID, accepting: accepting, connectedSince: now, lastPing: now,
		remoteMeta: map[string]string{"version": version, "remote_addr": remoteAddr},
		state:      "connected", stateSince: now, agent: agent, activeJobs: make(map[string]HubActiveJob),
	}
	h.placementCache = placementCache{}
	if expected, waiting := h.expectedVersion[machineID]; waiting && version == expected.version {
		delete(h.expectedVersion, machineID)
		h.recordUIEventLocked("update", "succeeded", machineID, now)
	}
	h.recordUIEventLocked("presence", "connected", machineID, now)
	if h.burstPolicyPath != "" && machineID == h.burstPolicy.TargetMachine && accepting && !h.burstState.LastUp.IsZero() {
		h.burstState.UpCompleted = true
	}
	h.mu.Unlock()
	h.tryPendingRevocations(machineID)
	if previous != nil && previous != agent {
		_ = previous.conn.Close(websocket.StatusPolicyViolation, "replaced")
	}
}

func (h *HubServer) touch(machineID string, agent *hubAgent) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	record := h.nodes[machineID]
	if record == nil || record.agent != agent {
		return false
	}
	record.lastPing = h.now().UTC()
	if record.state != "connected" {
		record.stateSince = record.lastPing
		h.recordUIEventLocked("presence", "connected", machineID, record.lastPing)
	}
	record.state = "connected"
	return true
}

func (h *HubServer) recordNote(machineID, text string, received time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastNotes[machineID] = &HubLastNote{Text: text, ReceivedAt: received}
}

func (h *HubServer) disconnect(machineID string, agent *hubAgent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if record := h.nodes[machineID]; record != nil && record.agent == agent {
		record.agent = nil
		now := h.now().UTC()
		if record.state == "connected" {
			record.stateSince = now
		}
		record.state = "disconnected"
		h.markBurstHoldsLostLocked(machineID)
		h.recordUIEventLocked("presence", "disconnected", machineID, now)
	}
}

func (h *HubServer) broadcast(event hubEvent) {
	h.broadcastSubscriptionMessage(hubSubscriptionMessage{event: &event})
}

func (h *HubServer) broadcastFailover(event hubFailoverEvent) {
	h.recordUIEvent("failover", event.Phase, event.Machine, event.EmittedAt)
	h.broadcastSubscriptionMessage(hubSubscriptionMessage{failover: &event})
	h.mu.Lock()
	agents := make([]*hubAgent, 0, len(h.nodes))
	for _, record := range h.nodes {
		if record.agent != nil {
			agents = append(agents, record.agent)
		}
	}
	h.mu.Unlock()
	for _, agent := range agents {
		agent.queueFailover(event)
	}
}

func (h *HubServer) recordUIEvent(kind, phase, machineID string, at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recordUIEventLocked(kind, phase, machineID, at)
}

func (h *HubServer) recordUIEventLocked(kind, phase, machineID string, at time.Time) {
	h.recordUIEventWithJobLocked(kind, phase, machineID, "", at)
}

func (h *HubServer) recordUIEventWithJobLocked(kind, phase, machineID, jobID string, at time.Time) {
	if at.IsZero() {
		at = h.now().UTC()
	}
	h.uiEvents = append(h.uiEvents, hubUIEvent{Kind: kind, Phase: phase, MachineID: machineID, JobID: jobID, At: at.UTC()})
	if len(h.uiEvents) > hubUIEventLimit {
		h.uiEvents = append([]hubUIEvent(nil), h.uiEvents[len(h.uiEvents)-hubUIEventLimit:]...)
	}
}

func (h *HubServer) broadcastSubscriptionMessage(message hubSubscriptionMessage) {
	h.mu.Lock()
	var slow []*hubEventSubscriber
	for subscriber := range h.subscribers {
		select {
		case subscriber.messages <- message:
		default:
			slow = append(slow, subscriber)
		}
	}
	h.mu.Unlock()
	for _, subscriber := range slow {
		subscriber.cancel()
	}
}

func (h *HubServer) authorizeAgent(request *http.Request) (string, bool) {
	machineID := strings.TrimSpace(request.Header.Get(hubMachineIDHeader))
	if machineID == hubOperatorMachineID || !machineIDPattern.MatchString(machineID) {
		return "", false
	}
	expected, exists := h.tokens[machineID]
	if !exists || !hubTokenMatches(expected, hubRequestBearerToken(request)) {
		return "", false
	}
	return machineID, true
}

func (h *HubServer) authorizeOperator(request *http.Request) bool {
	return hubTokenMatches(h.tokens[hubOperatorMachineID], hubRequestBearerToken(request))
}

func hubTokenMatches(expected, received string) bool {
	if expected == "" || received == "" || len(expected) != len(received) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(received)) == 1
}

func hubRequestBearerToken(request *http.Request) string {
	value := request.Header.Get(hubAuthorizationHeader)
	scheme, token, found := strings.Cut(value, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || !validHubToken(token) {
		return ""
	}
	return token
}

func hubUnauthorized(writer http.ResponseWriter) {
	writer.Header().Set("WWW-Authenticate", "Bearer")
	http.Error(writer, "unauthorized", http.StatusUnauthorized)
}

func (h *HubServer) countUnknownMessage() {
	h.mu.Lock()
	h.unknownMessages++
	h.mu.Unlock()
}

// countUnfencedCompletion records a terminal record that arrived for a job the
// hub had not registered. It is deliberately not an unknown message: the
// message was well-formed and its report was relayed.
func (h *HubServer) countUnfencedCompletion() {
	h.mu.Lock()
	h.unfencedCompletions++
	h.mu.Unlock()
}

// countUnpersistedRelayEvent records a relay event handoffkeep refused or was
// unreachable for. The record was neither injected nor dropped: the node keeps
// it in its outbox and retries.
func (h *HubServer) countUnpersistedRelayEvent() {
	h.mu.Lock()
	h.unpersistedRelayEvents++
	h.mu.Unlock()
}

// countAlreadyDeliveredRelayEvent records a resend the hub answered without
// re-injecting because handoffkeep already holds the row as delivered.
func (h *HubServer) countAlreadyDeliveredRelayEvent() {
	h.mu.Lock()
	h.alreadyDeliveredRelayEvents++
	h.mu.Unlock()
}

// AlreadyDeliveredRelayEventCount exists for local monitoring and tests.
func (h *HubServer) AlreadyDeliveredRelayEventCount() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.alreadyDeliveredRelayEvents
}

// countReplayExhaustedEvent records a durable row the replay refused to
// re-inject because it had already spent its delivery attempts. The bounded
// remembered set keeps both this metric and the operator broadcast one per row.
func (h *HubServer) countReplayExhaustedEvent(eventID int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, already := h.replayExhausted[eventID]; already {
		h.replayExhaustedOrder.touch(eventID, relayReplayExhaustedMaxEntries)
		return false
	}
	if h.replayExhausted == nil {
		h.replayExhausted = make(map[int64]struct{})
	}
	_, evicted, overflowed := h.replayExhaustedOrder.touch(eventID, relayReplayExhaustedMaxEntries)
	if overflowed {
		// Forgetting an old durable row can only repeat its operator broadcast
		// and count if it is encountered again; replay remains attempt-gated.
		delete(h.replayExhausted, evicted)
	}
	h.replayExhausted[eventID] = struct{}{}
	h.replayExhaustedEvents++
	return true
}

// ReplayExhaustedEventCount exists for local monitoring and tests.
func (h *HubServer) ReplayExhaustedEventCount() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.replayExhaustedEvents
}

// UnpersistedRelayEventCount exists for local monitoring and tests.
func (h *HubServer) UnpersistedRelayEventCount() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.unpersistedRelayEvents
}

// UnfencedCompletionCount exists for local monitoring and tests.
func (h *HubServer) UnfencedCompletionCount() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.unfencedCompletions
}

// UnknownMessageCount exists for local monitoring and tests. It intentionally
// exposes only a count so malformed remote input cannot be reflected in logs.
func (h *HubServer) UnknownMessageCount() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.unknownMessages
}

// Nodes returns a point-in-time sorted presence view. Disconnected entries are
// retained so a recent disconnect is visible until the hub process restarts.
func (h *HubServer) Nodes() []HubNode {
	now := h.now().UTC()
	h.mu.Lock()
	defer h.mu.Unlock()
	nodes := make([]HubNode, 0, len(h.nodes))
	for _, record := range h.nodes {
		age := now.Sub(record.lastPing)
		if age < 0 {
			age = 0
		}
		remoteMeta := make(map[string]string, len(record.remoteMeta))
		for key, value := range record.remoteMeta {
			remoteMeta[key] = value
		}
		var lastNote *HubLastNote
		if note := h.lastNotes[record.machineID]; note != nil {
			lastNote = &HubLastNote{Text: note.Text, ReceivedAt: note.ReceivedAt}
		}
		effective := h.acceptingEffectiveLocked(record.machineID, record.accepting)
		nodes = append(nodes, HubNode{
			MachineID: record.machineID, AlertClass: h.alertClass(record.machineID), Accepting: effective, AcceptingEffective: effective, AcceptingOverride: h.acceptingOverrideLocked(record.machineID), ConnectedSince: record.connectedSince, LastPingMS: age.Milliseconds(), LastNote: lastNote, Load: hubNodeLoadFromHostLoad(record.hostLoad), Memory: cloneHubHostMemory(record.hostMemory), RemoteMeta: remoteMeta, State: record.state,
		})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].MachineID < nodes[j].MachineID })
	return nodes
}

func hubNodeLoadFromHostLoad(load *HubHostLoad) *HubNodeLoad {
	if load == nil {
		return nil
	}
	load1, load5 := load.Load1, load.Load5
	view := &HubNodeLoad{Load1: &load1, Load5: &load5}
	if load.Load15 != nil {
		value := *load.Load15
		view.Load15 = &value
	}
	if load.NCPU != nil {
		value := *load.NCPU
		view.NCPU = &value
	}
	return view
}

// Sweep updates stale state and sends the server side of the application
// keepalive. Tests call it with an injected clock; production invokes it from
// RunMaintenance.
func (h *HubServer) Sweep() {
	now := h.now().UTC()
	type pendingPing struct{ agent *hubAgent }
	var pings []pendingPing
	var notifications []hubNotification
	var failovers []hubFailoverEvent
	h.mu.Lock()
	h.sweepBurstHoldsLocked(now)
	for machineID, expected := range h.expectedVersion {
		if !now.Before(expected.deadline) {
			delete(h.expectedVersion, machineID)
			h.recordUIEventLocked("update", "unconfirmed", machineID, now)
		}
	}
	for _, record := range h.nodes {
		if record.agent != nil && now.Sub(record.lastPing) >= h.staleAfter && record.state != "stale" {
			record.state = "stale"
			record.stateSince = now
			h.markBurstHoldsLostLocked(record.machineID)
			h.recordUIEventLocked("presence", "stale", record.machineID, now)
		}
		recordNotifications, recordFailovers := h.observeNodeAlertLocked(now, record)
		notifications = append(notifications, recordNotifications...)
		failovers = append(failovers, recordFailovers...)
		if record.agent != nil && (record.lastKeepaliveSent.IsZero() || now.Sub(record.lastKeepaliveSent) >= h.keepaliveInterval) {
			record.lastKeepaliveSent = now
			pings = append(pings, pendingPing{agent: record.agent})
		}
	}
	h.mu.Unlock()
	h.sweepOrphanedJobs(now)
	for _, failover := range failovers {
		h.broadcastFailover(failover)
	}
	h.dispatchHubNotifications(notifications)
	for _, ping := range pings {
		_ = ping.agent.writeJSON(hubOutbound{Type: "ping"})
	}
}

// RunMaintenance keeps the testable state transition separate from a real
// ticker. It returns promptly when the containing HTTP server is shutting down.
func (h *HubServer) RunMaintenance(ctx context.Context) {
	// Startup replay runs once, before the first keepalive tick: whatever
	// Postgres still holds as undelivered predates this process.
	h.replayUndeliveredRelayEvents(ctx)
	interval := h.keepaliveInterval
	if halfStale := h.staleAfter / 2; halfStale > 0 && halfStale < interval {
		interval = halfStale
	}
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.Sweep()
		}
	}
}

func (agent *hubAgent) writeJSON(value any) error {
	agent.writeMu.Lock()
	defer agent.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return wsjson.Write(ctx, agent.conn, value)
}

func (agent *hubAgent) queueFailover(event hubFailoverEvent) {
	if agent == nil || agent.failovers == nil {
		return
	}
	select {
	case agent.failovers <- event:
	default:
		if agent.conn != nil {
			_ = agent.conn.Close(websocket.StatusPolicyViolation, "slow")
		}
	}
}

func (agent *hubAgent) writeFailovers(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-agent.failovers:
			if err := agent.writeJSON(event); err != nil {
				return
			}
		}
	}
}

func (agent *hubAgent) queueBurst(event hubBurstEvent) {
	if agent == nil || agent.bursts == nil {
		return
	}
	select {
	case agent.bursts <- event:
	default:
		if agent.conn != nil {
			_ = agent.conn.Close(websocket.StatusPolicyViolation, "slow")
		}
	}
}

func (agent *hubAgent) writeBursts(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-agent.bursts:
			if err := agent.writeJSON(event); err != nil {
				return
			}
		}
	}
}

func (agent *hubAgent) writeRevocations(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-agent.revocations:
			if err := agent.writeJSON(event); err != nil {
				return
			}
		}
	}
}

func (agent *hubAgent) queueAssignment(event hubJobAssignedEvent) {
	if agent == nil || agent.assignments == nil {
		return
	}
	select {
	case agent.assignments <- event:
	default:
		// Directives are idempotent. Preserve a current assignment rather than
		// disconnecting a healthy node because heartbeat writes lag briefly.
		select {
		case <-agent.assignments:
		default:
		}
		select {
		case agent.assignments <- event:
		default:
		}
	}
}

func (agent *hubAgent) writeAssignments(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-agent.assignments:
			if err := agent.writeJSON(event); err != nil {
				return
			}
		}
	}
}

func (agent *hubAgent) queueHolds(event hubBurstHoldsEvent) {
	if agent == nil || agent.holds == nil {
		return
	}
	select {
	case agent.holds <- event:
	default:
		select {
		case <-agent.holds:
		default:
		}
		select {
		case agent.holds <- event:
		default:
		}
	}
}

func (agent *hubAgent) writeHolds(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-agent.holds:
			if err := agent.writeJSON(event); err != nil {
				return
			}
		}
	}
}

func (agent *hubAgent) queueRelay(event hubRelayInjectEvent) bool {
	if agent == nil || agent.relays == nil {
		return false
	}
	select {
	case agent.relays <- event:
		return true
	default:
		return false
	}
}

func (agent *hubAgent) queuePersisted(event hubRelayPersistedEvent) bool {
	if agent == nil || agent.persisted == nil {
		return false
	}
	select {
	case agent.persisted <- event:
		return true
	default:
		return false
	}
}

func (agent *hubAgent) writePersisted(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-agent.persisted:
			if err := agent.writeJSON(event); err != nil {
				return
			}
		}
	}
}

func (agent *hubAgent) writeRelays(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-agent.relays:
			if err := agent.writeJSON(event); err != nil {
				return
			}
		}
	}
}

func (h *HubServer) handleEvents(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	connection.SetReadLimit(hubMaxMessageBytes)
	ctx, cancel := context.WithCancel(request.Context())
	subscriber := &hubEventSubscriber{conn: connection, ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 64)}
	h.mu.Lock()
	h.subscribers[subscriber] = struct{}{}
	h.mu.Unlock()
	defer func() {
		cancel()
		h.mu.Lock()
		delete(h.subscribers, subscriber)
		h.mu.Unlock()
		_ = connection.CloseNow()
	}()
	go subscriber.writeLoop()
	for {
		if _, _, err := connection.Read(ctx); err != nil {
			return
		}
		// Operator clients subscribe only; inbound data is intentionally ignored.
	}
}

func (subscriber *hubEventSubscriber) writeLoop() {
	for {
		select {
		case <-subscriber.ctx.Done():
			return
		case message := <-subscriber.messages:
			var value any
			switch {
			case message.event != nil:
				value = message.event
			case message.failover != nil:
				value = message.failover
			default:
				continue
			}
			writeContext, cancel := context.WithTimeout(subscriber.ctx, 5*time.Second)
			err := wsjson.Write(writeContext, subscriber.conn, value)
			cancel()
			if err != nil {
				subscriber.cancel()
				return
			}
		}
	}
}
