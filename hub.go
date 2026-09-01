package panewire

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
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
	Notifier          HubNotifier
	Logger            *slog.Logger
}

// HubNode is the deliberately small presence view returned to authenticated
// operators. RemoteMeta records only protocol metadata, never credentials.
type HubNode struct {
	MachineID      string            `json:"machine_id"`
	AlertClass     string            `json:"alert_class"`
	Accepting      bool              `json:"accepting"`
	ConnectedSince time.Time         `json:"connected_since"`
	LastPingMS     int64             `json:"last_ping_ms"`
	LastNote       *HubLastNote      `json:"last_note,omitempty"`
	RemoteMeta     map[string]string `json:"remote_meta"`
	State          string            `json:"state"`
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
}

type hubAgent struct {
	conn      *websocket.Conn
	writeMu   sync.Mutex
	transient bool
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

// hubSubscriptionMessage keeps the operator event stream's vocabulary
// explicit. Agent events retain their existing envelope; server-originated
// failover messages have their own fixed three-field shape.
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
	alertObservations int
	notifier          HubNotifier
	logger            *slog.Logger

	mu              sync.Mutex
	nodes           map[string]*hubNodeRecord
	lastNotes       map[string]*HubLastNote
	subscribers     map[*hubEventSubscriber]struct{}
	alerts          map[string]*hubAlertState
	unknownMessages uint64
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
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &HubServer{
		tokens: tokens, alertNodes: alertNodes, now: config.Now, staleAfter: config.StaleAfter, keepaliveInterval: config.KeepaliveInterval,
		gracePeriod: config.GracePeriod, alertObservations: defaultHubAlertObservations, notifier: config.Notifier, logger: config.Logger,
		nodes: make(map[string]*hubNodeRecord), lastNotes: make(map[string]*HubLastNote), subscribers: make(map[*hubEventSubscriber]struct{}), alerts: make(map[string]*hubAlertState),
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
	mux.HandleFunc("GET /v1/nodes", h.handleNodes)
	mux.HandleFunc("GET /v1/agent", h.handleAgent)
	mux.HandleFunc("GET /v1/events", h.handleEvents)
	return mux
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
	agent := &hubAgent{conn: connection}
	defer connection.CloseNow()
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
			h.observeHeartbeatAlerts(machineID, heartbeat)
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
	Status string                    `json:"status"`
	Checks map[string]HubCheckStatus `json:"checks"`
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
		if name != "status" && name != "checks" {
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
	if rawChecks, exists := fields["checks"]; exists {
		if json.Unmarshal(rawChecks, &heartbeat.Checks) != nil || heartbeat.Checks == nil {
			return hubHeartbeatPayload{}, false
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
	case "ping", "pong":
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
		message.Payload = append(json.RawMessage(nil), rawPayload...)
	default:
		return hubInbound{}, false
	}
	return message, true
}

func knownHubEventKind(kind string) bool {
	switch kind {
	case "heartbeat", "note":
		return true
	}
	return false
}

func (h *HubServer) connect(machineID, version, remoteAddr string, agent *hubAgent, accepting bool) {
	now := h.now().UTC()
	var previous *hubAgent
	h.mu.Lock()
	if existing := h.nodes[machineID]; existing != nil {
		previous = existing.agent
	}
	h.nodes[machineID] = &hubNodeRecord{
		machineID: machineID, accepting: accepting, connectedSince: now, lastPing: now,
		remoteMeta: map[string]string{"version": version, "remote_addr": remoteAddr},
		state:      "connected", stateSince: now, agent: agent,
	}
	h.mu.Unlock()
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
		if record.state == "connected" {
			record.stateSince = h.now().UTC()
		}
		record.state = "disconnected"
	}
}

func (h *HubServer) broadcast(event hubEvent) {
	h.broadcastSubscriptionMessage(hubSubscriptionMessage{event: &event})
}

func (h *HubServer) broadcastFailover(event hubFailoverEvent) {
	h.broadcastSubscriptionMessage(hubSubscriptionMessage{failover: &event})
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
		nodes = append(nodes, HubNode{
			MachineID: record.machineID, AlertClass: h.alertClass(record.machineID), Accepting: record.accepting, ConnectedSince: record.connectedSince, LastPingMS: age.Milliseconds(), LastNote: lastNote, RemoteMeta: remoteMeta, State: record.state,
		})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].MachineID < nodes[j].MachineID })
	return nodes
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
	for _, record := range h.nodes {
		if record.agent != nil && now.Sub(record.lastPing) >= h.staleAfter && record.state != "stale" {
			record.state = "stale"
			record.stateSince = now
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
