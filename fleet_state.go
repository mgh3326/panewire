package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Fleet state: every node publishes the state it owns, the hub keeps the
// latest value per (host, kind, key) and serves it at GET /v1/state. The
// contract, including the freshness rule consumers apply, is docs/fleet-state.md.

const (
	fleetStateVersion = 1

	fleetStateKindQuota = "quota"
	fleetStateKindLoad  = "load"
	// fleetStateKindSpawn is retired. An old node still publishes it; the hub
	// accepts the value and drops it. Remove once every node runs this build.
	fleetStateKindSpawn     = "spawn"
	fleetStateKindPublisher = "publisher"

	fleetStateQuotaKeysPerHost = 64
	fleetStateMaxPublishBytes  = 4 << 10
	fleetStateMaxBuckets       = 16

	// fleetStatePublisherBeat bounds how often an unchanged publisher value
	// is re-emitted while heartbeats arrive; consumers allow 90 s.
	fleetStatePublisherBeat = 20 * time.Second
	// fleetStateLoadRepublish bounds how long an unchanged load value goes
	// without a fresh measured_at.
	fleetStateLoadRepublish = 30 * time.Second
	// fleetStateLoadInterval spaces load computations: each one queries the
	// placement metrics source.
	fleetStateLoadInterval = 15 * time.Second
	// fleetStateNodeRepeat drops an identical node value repeated sooner.
	fleetStateNodeRepeat = time.Minute

	// A node's state publishes draw from a token bucket so one node cannot
	// flood the hub. The burst covers a reconnect resync (every quota key); the
	// refill is far above the steady rate of a correct node (one quota per pool
	// per minute). An old node's retired spawn publishes spend it too, so the
	// burst keeps the room they had.
	fleetStateNodeBurst  = 256 + fleetStateQuotaKeysPerHost
	fleetStateNodeRefill = 2.0 // tokens per second
)

var (
	fleetStateAccountKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}:[0-9a-f]{16}$`)
	fleetStateProfilePattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._@:+-]{0,63}$`)
	fleetStateTokenPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,63}$`)
)

// FleetStateMessage is one state value on the wire. The hub sets the envelope
// (v, seq, host, kind, key, received_at); the publisher owns measured_at and
// exactly one kind body.
type FleetStateMessage struct {
	V          int       `json:"v"`
	Seq        uint64    `json:"seq"`
	Host       string    `json:"host"`
	Kind       string    `json:"kind"`
	Key        string    `json:"key"`
	MeasuredAt time.Time `json:"measured_at"`
	// ConfirmedAt is when the shared store last re-confirmed an unchanged
	// quota reading (handoffkeep keeps measured_at and advances confirmed_at).
	ConfirmedAt *time.Time           `json:"confirmed_at,omitempty"`
	ReceivedAt  time.Time            `json:"received_at"`
	Quota       *FleetStateQuota     `json:"quota,omitempty"`
	Load        *FleetStateLoad      `json:"load,omitempty"`
	Publisher   *FleetStatePublisher `json:"publisher,omitempty"`
	// Readiness is the host's launch readiness and backoff (hub_readiness.go).
	Readiness *fleet.ReadinessState `json:"readiness,omitempty"`
}

// FleetStateQuota is one account's shared reading, in the allowlisted bucket
// shape of the shared quota cache. Provider error text never travels here.
type FleetStateQuota struct {
	AccountKey string             `json:"account_key"`
	Pool       string             `json:"pool"`
	Source     string             `json:"source,omitempty"`
	Buckets    []FleetStateBucket `json:"buckets"`
	// Free is a pool's free-tier state (devin's SWE-2 bucket), the shared
	// reading's free_bucket. A free-only reading may carry no paid bucket.
	Free *FleetStateFree `json:"free_bucket,omitempty"`
}

// FleetStateFree is the free-tier state of a pool that has one. State is
// "available", "exhausted" or "unknown"; ResetsAt is set exactly when it is
// "exhausted".
type FleetStateFree struct {
	State    string     `json:"state"`
	ResetsAt *time.Time `json:"resets_at,omitempty"`
}

type FleetStateBucket struct {
	Window   string   `json:"window"`
	UsedPct  *float64 `json:"used_pct"`
	ResetsAt string   `json:"resets_at,omitempty"`
	Scope    string   `json:"scope,omitempty"`
	Model    string   `json:"model,omitempty"`
}

// FleetStateLoad is the hub's placement view of one host, computed from that
// host's own heartbeats and the placement policy.
type FleetStateLoad struct {
	LoadRatio float64 `json:"load_ratio"`
	Score     float64 `json:"score"`
	// Accepting is the node's effective acceptance as placement computes it
	// (connected, operator/override acceptance, no memory pressure); Usable
	// is placement's own verdict for this host (placementUsable).
	Accepting  bool              `json:"accepting"`
	Usable     bool              `json:"usable"`
	Connected  bool              `json:"connected"`
	SlotFull   bool              `json:"slot_full"`
	ActiveJobs int               `json:"active_jobs"`
	Reason     string            `json:"reason"`
	Source     string            `json:"source"`
	Signals    *PlacementSignals `json:"signals,omitempty"`
}

// FleetStateSpawn is what an old node still publishes. It is decoded so the
// publish stays valid, then dropped.
type FleetStateSpawn struct {
	Phase   string    `json:"phase"`
	Job     string    `json:"job"`
	Profile string    `json:"profile,omitempty"`
	Host    string    `json:"host"`
	At      time.Time `json:"at"`
}

// FleetStatePublisher is the publisher heartbeat: it tells "nothing changed"
// apart from "publisher dead".
type FleetStatePublisher struct {
	Connected bool      `json:"connected"`
	LastSeen  time.Time `json:"last_seen"`
}

// FleetStatePublish is the payload of a node's websocket `state` event. Host
// is optional; when present it must name the publishing node itself.
type FleetStatePublish struct {
	Kind       string    `json:"kind"`
	Host       string    `json:"host,omitempty"`
	Key        string    `json:"key"`
	MeasuredAt time.Time `json:"measured_at"`
	// ConfirmedAt is optional and quota-only: see FleetStateMessage.
	ConfirmedAt *time.Time       `json:"confirmed_at,omitempty"`
	Quota       *FleetStateQuota `json:"quota,omitempty"`
	Spawn       *FleetStateSpawn `json:"spawn,omitempty"`
}

// decodeFleetStatePublish validates a node's state event without knowing the
// node: the host binding is checked by the caller, which knows the machine.
func decodeFleetStatePublish(raw []byte) (FleetStatePublish, bool) {
	if len(raw) == 0 || len(raw) > fleetStateMaxPublishBytes {
		return FleetStatePublish{}, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var publish FleetStatePublish
	if decoder.Decode(&publish) != nil || decoder.More() || publish.MeasuredAt.IsZero() {
		return FleetStatePublish{}, false
	}
	if publish.Host != "" && !machineIDPattern.MatchString(publish.Host) {
		return FleetStatePublish{}, false
	}
	if publish.ConfirmedAt != nil && (publish.Kind != fleetStateKindQuota || publish.ConfirmedAt.Before(publish.MeasuredAt)) {
		return FleetStatePublish{}, false
	}
	switch publish.Kind {
	case fleetStateKindQuota:
		return publish, publish.Spawn == nil && validFleetStateQuota(publish.Key, publish.Quota)
	case fleetStateKindSpawn:
		return publish, publish.Quota == nil && validFleetStateSpawn(publish.Key, publish.Spawn)
	}
	return FleetStatePublish{}, false
}

func validFleetStateQuota(key string, quota *FleetStateQuota) bool {
	if quota == nil || !fleetStateAccountKeyPattern.MatchString(key) || quota.AccountKey != key {
		return false
	}
	pool, _, _ := strings.Cut(key, ":")
	if quota.Pool != pool || (quota.Source != "" && !fleetStateTokenPattern.MatchString(quota.Source)) {
		return false
	}
	if len(quota.Buckets) == 0 && quota.Free == nil || len(quota.Buckets) > fleetStateMaxBuckets {
		return false
	}
	if quota.Free != nil {
		switch quota.Free.State {
		case "available", "unknown":
			if quota.Free.ResetsAt != nil {
				return false
			}
		case "exhausted":
			if quota.Free.ResetsAt == nil {
				return false
			}
		default:
			return false
		}
	}
	for _, bucket := range quota.Buckets {
		if !fleetStateTokenPattern.MatchString(bucket.Window) || bucket.UsedPct == nil {
			return false
		}
		if used := *bucket.UsedPct; math.IsNaN(used) || math.IsInf(used, 0) || used < 0 || used > 100 {
			return false
		}
		if bucket.ResetsAt != "" {
			if _, err := time.Parse(time.RFC3339, bucket.ResetsAt); err != nil {
				return false
			}
		}
		if (bucket.Scope != "" && !fleetStateTokenPattern.MatchString(bucket.Scope)) || (bucket.Model != "" && !fleetStateTokenPattern.MatchString(bucket.Model)) {
			return false
		}
	}
	return true
}

func validFleetStateSpawn(key string, spawn *FleetStateSpawn) bool {
	if spawn == nil || spawn.Job != key || !hubJobIDPattern.MatchString(key) || spawn.At.IsZero() {
		return false
	}
	if spawn.Phase != "start" && spawn.Phase != "end" {
		return false
	}
	if spawn.Profile != "" && !fleetStateProfilePattern.MatchString(spawn.Profile) {
		return false
	}
	return spawn.Host == "" || machineIDPattern.MatchString(spawn.Host)
}

type fleetStateKey struct{ host, kind, key string }

// fleetStateStore holds the latest value per key. Its mutex is never held
// across a network write.
type fleetStateStore struct {
	mu      sync.Mutex
	seq     uint64
	latest  map[fleetStateKey]FleetStateMessage
	budgets map[string]*fleetStateBudget
}

type fleetStateBudget struct {
	tokens float64
	at     time.Time
}

// allowNode spends one token of host's publish budget, refilled at
// fleetStateNodeRefill per second up to fleetStateNodeBurst.
func (s *fleetStateStore) allowNode(host string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	budget := s.budgets[host]
	if budget == nil {
		budget = &fleetStateBudget{tokens: fleetStateNodeBurst, at: now}
		s.budgets[host] = budget
	}
	if elapsed := now.Sub(budget.at).Seconds(); elapsed > 0 {
		budget.tokens = math.Min(fleetStateNodeBurst, budget.tokens+elapsed*fleetStateNodeRefill)
	}
	budget.at = now
	if budget.tokens < 1 {
		return false
	}
	budget.tokens--
	return true
}

func newFleetStateStore() *fleetStateStore {
	return &fleetStateStore{
		latest:  make(map[fleetStateKey]FleetStateMessage),
		budgets: make(map[string]*fleetStateBudget),
	}
}

// stateStore answers the fleet state store. NewHubServer creates it; a hub
// built any other way gets one on first use instead of a nil dereference on
// the heartbeat path.
func (h *HubServer) stateStore() *fleetStateStore {
	h.fleetStateOnce.Do(func() {
		if h.fleetState == nil {
			h.fleetState = newFleetStateStore()
		}
	})
	return h.fleetState
}

// fleetStateBody is what "unchanged" compares: the kind body plus, for node
// values, the publisher's measured_at.
func fleetStateBody(message FleetStateMessage) string {
	body := struct {
		MeasuredAt *time.Time            `json:"m,omitempty"`
		Confirmed  *time.Time            `json:"c,omitempty"`
		Quota      *FleetStateQuota      `json:"q,omitempty"`
		Load       *FleetStateLoad       `json:"l,omitempty"`
		Publisher  *FleetStatePublisher  `json:"p,omitempty"`
		Readiness  *fleet.ReadinessState `json:"r,omitempty"`
	}{Quota: message.Quota, Load: message.Load, Readiness: message.Readiness}
	if message.Kind == fleetStateKindQuota {
		body.MeasuredAt = &message.MeasuredAt
		body.Confirmed = message.ConfirmedAt
	}
	if message.Publisher != nil {
		// last_seen advances every heartbeat; only connectivity is a change.
		body.Publisher = &FleetStatePublisher{Connected: message.Publisher.Connected}
	}
	encoded, _ := json.Marshal(body)
	return string(encoded)
}

// publish records message unless an identical value was recorded less than
// repeat ago. It answers whether a new value (and seq) was recorded.
func (s *fleetStateStore) publish(message FleetStateMessage, repeat time.Duration) bool {
	key := fleetStateKey{message.Host, message.Kind, message.Key}
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, exists := s.latest[key]; exists {
		if fleetStateBody(previous) == fleetStateBody(message) && message.ReceivedAt.Sub(previous.ReceivedAt) < repeat {
			return false
		}
		// The store keeps one value per host and account, so a delayed reading
		// must not replace a newer one: the supersession check reads exactly
		// this value, and an old exhausted publish arriving after a newer
		// available one would otherwise be the only thing it sees.
		if fleetStateObservedAt(message).Before(fleetStateObservedAt(previous)) {
			return false
		}
	}
	s.seq++
	message.V, message.Seq = fleetStateVersion, s.seq
	s.latest[key] = message
	s.boundHostLocked(message.Host, message.Kind)
	return true
}

// fleetStateObservedAt is when a reading was taken: its measurement, or a later
// confirmation of it.
func fleetStateObservedAt(message FleetStateMessage) time.Time {
	at := message.MeasuredAt
	if message.ConfirmedAt != nil && message.ConfirmedAt.After(at) {
		at = *message.ConfirmedAt
	}
	return at
}

// boundHostLocked keeps one host's quota keys within their cap, dropping the
// oldest first.
func (s *fleetStateStore) boundHostLocked(host, kind string) {
	if kind != fleetStateKindQuota {
		return
	}
	var keys []fleetStateKey
	for key := range s.latest {
		if key.host == host && key.kind == kind {
			keys = append(keys, key)
		}
	}
	if len(keys) <= fleetStateQuotaKeysPerHost {
		return
	}
	slices.SortFunc(keys, func(a, b fleetStateKey) int {
		return s.latest[a].ReceivedAt.Compare(s.latest[b].ReceivedAt)
	})
	for _, key := range keys[:len(keys)-fleetStateQuotaKeysPerHost] {
		delete(s.latest, key)
	}
}

func (s *fleetStateStore) snapshotLocked() []FleetStateMessage {
	states := make([]FleetStateMessage, 0, len(s.latest))
	for _, message := range s.latest {
		states = append(states, message)
	}
	slices.SortFunc(states, func(a, b FleetStateMessage) int {
		switch {
		case a.Seq < b.Seq:
			return -1
		case a.Seq > b.Seq:
			return 1
		}
		return 0
	})
	return states
}

type fleetStateSnapshot struct {
	V       int                  `json:"v"`
	Seq     uint64               `json:"seq"`
	At      time.Time            `json:"at"`
	States  []FleetStateMessage  `json:"states"`
	Cooling map[string]time.Time `json:"pool_cooling,omitempty"`
}

func (s *fleetStateStore) snapshot(now time.Time) fleetStateSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fleetStateSnapshot{V: fleetStateVersion, Seq: s.seq, At: now, States: s.snapshotLocked()}
}

// publishFleetStateFromNode stores one node `state` event under the node's
// own machine id. A value naming any other host is refused.
func (h *HubServer) publishFleetStateFromNode(machineID string, publish FleetStatePublish, received time.Time) bool {
	if publish.Host != "" && publish.Host != machineID {
		h.logger.Warn("fleet state for another host refused", "machine", machineID, "kind", publish.Kind)
		return false
	}
	if !h.stateStore().allowNode(machineID, received) {
		h.logger.Warn("fleet state publish over budget dropped", "machine", machineID, "kind", publish.Kind)
		return false
	}
	if publish.Kind == fleetStateKindSpawn {
		if publish.Spawn.Host != "" && publish.Spawn.Host != machineID {
			h.logger.Warn("fleet state for another host refused", "machine", machineID, "kind", publish.Kind)
			return false
		}
		return true
	}
	recorded := h.stateStore().publish(FleetStateMessage{Host: machineID, Kind: publish.Kind, Key: publish.Key, MeasuredAt: publish.MeasuredAt.UTC(), ConfirmedAt: utcPtr(publish.ConfirmedAt), ReceivedAt: received, Quota: publish.Quota}, fleetStateNodeRepeat)
	// A quota value that shows a pool used up is the trigger of the
	// exhausted-pool sweep (hub_pool_exhausted.go); an identical value
	// repeated within fleetStateNodeRepeat is not recorded and not a new look.
	if recorded && publish.Kind == fleetStateKindQuota {
		h.observePoolExhaustion(publish.Quota, received)
	}
	return true
}

// publishFleetPublisher emits the publisher heartbeat for one machine: at once
// when connectivity changes, otherwise at most every fleetStatePublisherBeat.
func (h *HubServer) publishFleetPublisher(machineID string, now time.Time) {
	h.mu.Lock()
	record := h.nodes[machineID]
	if record == nil {
		h.mu.Unlock()
		return
	}
	publisher := FleetStatePublisher{Connected: record.agent != nil && record.state == "connected", LastSeen: record.lastHeartbeat.UTC()}
	h.mu.Unlock()
	repeat := fleetStatePublisherBeat
	if !publisher.Connected {
		// A dead publisher is recorded once, on the transition; its last_seen
		// stays the moment it was last heard.
		repeat = time.Duration(math.MaxInt64)
	}
	h.stateStore().publish(FleetStateMessage{Host: machineID, Kind: fleetStateKindPublisher, MeasuredAt: publisher.LastSeen, ReceivedAt: now, Publisher: &publisher}, repeat)
}

// sweepFleetState re-emits publisher state, so a stale node is recorded. Sweep
// calls it with the maintenance clock.
func (h *HubServer) sweepFleetState(now time.Time) {
	h.mu.Lock()
	machines := make([]string, 0, len(h.nodes))
	for machine := range h.nodes {
		machines = append(machines, machine)
	}
	h.mu.Unlock()
	for _, machine := range machines {
		h.publishFleetPublisher(machine, now)
		h.publishFleetReadiness(machine, now)
	}
}

// startFleetLoadPublish runs publishFleetLoad off the sweep: the placement
// metrics fetch is a network call. At most one run is in flight, and runs
// start at most every fleetStateLoadInterval.
func (h *HubServer) startFleetLoadPublish(now time.Time) {
	if !h.fleetLoadMu.TryLock() {
		return
	}
	if !h.fleetLoadAt.IsZero() && now.Sub(h.fleetLoadAt) < fleetStateLoadInterval {
		h.fleetLoadMu.Unlock()
		return
	}
	h.fleetLoadAt = now
	go func() {
		defer h.fleetLoadMu.Unlock()
		parent := h.closeCtx
		if parent == nil {
			parent = context.Background()
		}
		ctx, cancel := context.WithTimeout(parent, 10*time.Second)
		defer cancel()
		h.publishFleetLoad(ctx, now)
	}()
}

// publishFleetLoad publishes one `load` value per placement machine from the
// same computation as GET /v1/placement, minus its wake-on-spill side effect
// and its cache. Slot-full machines, which /v1/placement drops from its
// candidates, are published with slot_full set.
func (h *HubServer) publishFleetLoad(ctx context.Context, now time.Time) {
	h.mu.Lock()
	h.reloadPlacementPolicyLocked()
	policy := h.placementPolicy
	h.mu.Unlock()
	if !policy.valid() {
		policy = DefaultPlacementPolicy()
	}
	if policy.LocalMachine == "" {
		return
	}
	metrics, err := h.fetchPlacementMetrics(ctx)
	source := "prometheus"
	if err != nil {
		source, metrics = "hub-only", placementMetrics{}
	}
	for _, candidate := range h.placementCandidates(policy, metrics, source, now) {
		reason := candidate.Reason
		load := FleetStateLoad{
			LoadRatio:  candidate.LoadRatio,
			Score:      math.Round(candidate.Score*1000) / 1000,
			Connected:  candidate.Connected,
			Accepting:  candidate.accepting,
			Usable:     placementUsable(candidate, policy, source),
			SlotFull:   candidate.slotFull,
			ActiveJobs: candidate.ActiveJobs,
			Reason:     reason,
			Source:     source,
			Signals:    candidate.Signals,
		}
		h.stateStore().publish(FleetStateMessage{Host: candidate.Machine, Kind: fleetStateKindLoad, MeasuredAt: now, ReceivedAt: now, Load: &load}, fleetStateLoadRepublish)
	}
}

func (h *HubServer) authorizeFleetStateRead(writer http.ResponseWriter, request *http.Request) bool {
	if access := h.authorize(request, hubScopeStateRead, ""); access != hubAccessGranted {
		writeHubAccessDenied(writer, access)
		return false
	}
	return true
}

func (h *HubServer) handleFleetStateSnapshot(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeFleetStateRead(writer, request) {
		return
	}
	writeJSON(writer, http.StatusOK, h.fleetSnapshot(h.now().UTC()))
}

// utcPtr copies an optional time in UTC.
func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
