package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Fleet state: every node publishes the state it owns, the hub keeps the
// latest value per (host, kind, key) and fans it out over SSE. The contract,
// including the freshness rule consumers apply, is docs/fleet-state.md.

const (
	fleetStateVersion = 1

	fleetStateKindQuota     = "quota"
	fleetStateKindLoad      = "load"
	fleetStateKindSpawn     = "spawn"
	fleetStateKindPublisher = "publisher"

	fleetStateRingSize         = 2048
	fleetStateQuotaKeysPerHost = 64
	fleetStateSpawnKeysPerHost = 256
	fleetStateEndedRetention   = time.Hour
	fleetStateMaxReaders       = 32
	fleetStateReaderBuffer     = 256
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
	fleetStateKeepalive  = 15 * time.Second
)

var (
	fleetStateAccountKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}:[0-9a-f]{16}$`)
	fleetStateProfilePattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._@:+-]{0,63}$`)
	fleetStateTokenPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,63}$`)
)

// FleetStateMessage is one state value on the wire. The hub sets the envelope
// (v, id, seq, host, kind, key, received_at); the publisher owns measured_at
// and exactly one kind body.
type FleetStateMessage struct {
	V          int                  `json:"v"`
	ID         string               `json:"id"`
	Seq        uint64               `json:"seq"`
	Host       string               `json:"host"`
	Kind       string               `json:"kind"`
	Key        string               `json:"key"`
	MeasuredAt time.Time            `json:"measured_at"`
	ReceivedAt time.Time            `json:"received_at"`
	Quota      *FleetStateQuota     `json:"quota,omitempty"`
	Load       *FleetStateLoad      `json:"load,omitempty"`
	Spawn      *FleetStateSpawn     `json:"spawn,omitempty"`
	Publisher  *FleetStatePublisher `json:"publisher,omitempty"`
}

// FleetStateQuota is one account's shared reading, in the allowlisted bucket
// shape of the shared quota cache. Provider error text never travels here.
type FleetStateQuota struct {
	AccountKey string             `json:"account_key"`
	Pool       string             `json:"pool"`
	Source     string             `json:"source,omitempty"`
	Buckets    []FleetStateBucket `json:"buckets"`
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
	LoadRatio      float64 `json:"load_ratio"`
	Score          float64 `json:"score"`
	CapacityWeight float64 `json:"capacity_weight"`
	Accepting      bool    `json:"accepting"`
	Connected      bool    `json:"connected"`
	SlotFull       bool    `json:"slot_full"`
	ActiveJobs     int     `json:"active_jobs"`
	Reason         string  `json:"reason"`
	Source         string  `json:"source"`
}

// FleetStateSpawn says a job started or ended on a host.
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
	Kind       string           `json:"kind"`
	Host       string           `json:"host,omitempty"`
	Key        string           `json:"key"`
	MeasuredAt time.Time        `json:"measured_at"`
	Quota      *FleetStateQuota `json:"quota,omitempty"`
	Spawn      *FleetStateSpawn `json:"spawn,omitempty"`
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
	if len(quota.Buckets) == 0 || len(quota.Buckets) > fleetStateMaxBuckets {
		return false
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

// fleetStateReader is one SSE subscriber. overflow is closed when the reader
// falls a full buffer behind; it then reconnects with its last id.
type fleetStateReader struct {
	messages chan FleetStateMessage
	overflow chan struct{}
}

// fleetStateStore holds the latest value per key, a bounded replay ring and
// the live readers. Its mutex is never held across a network write.
type fleetStateStore struct {
	mu      sync.Mutex
	epoch   string
	seq     uint64
	latest  map[fleetStateKey]FleetStateMessage
	ring    []FleetStateMessage
	readers map[*fleetStateReader]struct{}
}

func newFleetStateStore(started time.Time) *fleetStateStore {
	return &fleetStateStore{
		epoch:   strconv.FormatInt(started.UnixNano(), 36),
		latest:  make(map[fleetStateKey]FleetStateMessage),
		readers: make(map[*fleetStateReader]struct{}),
	}
}

// fleetStateBody is what "unchanged" compares: the kind body plus, for node
// values, the publisher's measured_at.
func fleetStateBody(message FleetStateMessage) string {
	body := struct {
		MeasuredAt *time.Time           `json:"m,omitempty"`
		Quota      *FleetStateQuota     `json:"q,omitempty"`
		Load       *FleetStateLoad      `json:"l,omitempty"`
		Spawn      *FleetStateSpawn     `json:"s,omitempty"`
		Publisher  *FleetStatePublisher `json:"p,omitempty"`
	}{Quota: message.Quota, Load: message.Load, Spawn: message.Spawn}
	if message.Kind == fleetStateKindQuota || message.Kind == fleetStateKindSpawn {
		body.MeasuredAt = &message.MeasuredAt
	}
	if message.Publisher != nil {
		// last_seen advances every heartbeat; only connectivity is a change.
		body.Publisher = &FleetStatePublisher{Connected: message.Publisher.Connected}
	}
	encoded, _ := json.Marshal(body)
	return string(encoded)
}

// publish records message unless an identical value was recorded less than
// repeat ago. It answers whether a new value (and seq) was emitted.
func (s *fleetStateStore) publish(message FleetStateMessage, repeat time.Duration) bool {
	key := fleetStateKey{message.Host, message.Kind, message.Key}
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, exists := s.latest[key]; exists && fleetStateBody(previous) == fleetStateBody(message) && message.ReceivedAt.Sub(previous.ReceivedAt) < repeat {
		return false
	}
	s.seq++
	message.V, message.Seq = fleetStateVersion, s.seq
	message.ID = s.epoch + "-" + strconv.FormatUint(s.seq, 10)
	s.latest[key] = message
	s.boundHostLocked(message.Host, message.Kind, message.ReceivedAt)
	s.ring = append(s.ring, message)
	if len(s.ring) >= 2*fleetStateRingSize {
		s.ring = append([]FleetStateMessage(nil), s.ring[len(s.ring)-fleetStateRingSize:]...)
	}
	for reader := range s.readers {
		select {
		case reader.messages <- message:
		default:
			delete(s.readers, reader)
			close(reader.overflow)
		}
	}
	return true
}

// boundHostLocked keeps one host's quota and spawn keys within their caps.
// An ended spawn is evicted before a running one, the oldest first.
func (s *fleetStateStore) boundHostLocked(host, kind string, now time.Time) {
	limit := 0
	switch kind {
	case fleetStateKindQuota:
		limit = fleetStateQuotaKeysPerHost
	case fleetStateKindSpawn:
		limit = fleetStateSpawnKeysPerHost
	default:
		return
	}
	var keys []fleetStateKey
	for key := range s.latest {
		if key.host == host && key.kind == kind {
			keys = append(keys, key)
		}
	}
	if len(keys) <= limit {
		return
	}
	slices.SortFunc(keys, func(a, b fleetStateKey) int {
		x, y := s.latest[a], s.latest[b]
		if xEnded, yEnded := x.Spawn != nil && x.Spawn.Phase == "end", y.Spawn != nil && y.Spawn.Phase == "end"; xEnded != yEnded {
			if xEnded {
				return -1
			}
			return 1
		}
		return x.ReceivedAt.Compare(y.ReceivedAt)
	})
	for _, key := range keys[:len(keys)-limit] {
		delete(s.latest, key)
	}
}

// expire drops ended spawns older than the retention. It emits nothing: an
// ended job has already said so on the stream.
func (s *fleetStateStore) expire(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, message := range s.latest {
		if message.Spawn != nil && message.Spawn.Phase == "end" && now.Sub(message.ReceivedAt) > fleetStateEndedRetention {
			delete(s.latest, key)
		}
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
	V      int                 `json:"v"`
	Epoch  string              `json:"epoch"`
	Seq    uint64              `json:"seq"`
	At     time.Time           `json:"at"`
	States []FleetStateMessage `json:"states"`
}

func (s *fleetStateStore) snapshot(now time.Time) fleetStateSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fleetStateSnapshot{V: fleetStateVersion, Epoch: s.epoch, Seq: s.seq, At: now, States: s.snapshotLocked()}
}

// subscribe registers a reader and answers what it must be sent first: the
// ring tail after lastID when lastID is from this epoch and still covered, or
// a reset plus the full snapshot otherwise. Registration and the replay are
// one critical section, so nothing published meanwhile is lost or repeated.
func (s *fleetStateStore) subscribe(lastID string) (reader *fleetStateReader, replay []FleetStateMessage, reset bool, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.readers) >= fleetStateMaxReaders {
		return nil, nil, false, false
	}
	reader = &fleetStateReader{messages: make(chan FleetStateMessage, fleetStateReaderBuffer), overflow: make(chan struct{})}
	s.readers[reader] = struct{}{}
	if lastID == "" {
		return reader, s.snapshotLocked(), false, true
	}
	if after, covered := s.ringAfterLocked(lastID); covered {
		return reader, after, false, true
	}
	return reader, s.snapshotLocked(), true, true
}

func (s *fleetStateStore) ringAfterLocked(lastID string) ([]FleetStateMessage, bool) {
	epoch, rawSeq, found := strings.Cut(lastID, "-")
	if !found || epoch != s.epoch {
		return nil, false
	}
	last, err := strconv.ParseUint(rawSeq, 10, 64)
	if err != nil || last > s.seq {
		return nil, false
	}
	if last == s.seq {
		return nil, true
	}
	ring := s.ring
	if len(ring) > fleetStateRingSize {
		ring = ring[len(ring)-fleetStateRingSize:]
	}
	// The ring covers lastID only when the message right after it is still
	// held; anything older has been trimmed and needs a reset.
	if len(ring) == 0 || ring[0].Seq > last+1 {
		return nil, false
	}
	index, _ := slices.BinarySearchFunc(ring, last+1, func(message FleetStateMessage, seq uint64) int {
		switch {
		case message.Seq < seq:
			return -1
		case message.Seq > seq:
			return 1
		}
		return 0
	})
	return append([]FleetStateMessage(nil), ring[index:]...), true
}

func (s *fleetStateStore) unsubscribe(reader *fleetStateReader) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.readers[reader]; exists {
		delete(s.readers, reader)
		close(reader.overflow)
	}
}

// publishFleetStateFromNode stores one node `state` event under the node's
// own machine id. A value naming any other host is refused.
func (h *HubServer) publishFleetStateFromNode(machineID string, publish FleetStatePublish, received time.Time) bool {
	if publish.Host != "" && publish.Host != machineID {
		h.logger.Warn("fleet state for another host refused", "machine", machineID, "kind", publish.Kind)
		return false
	}
	message := FleetStateMessage{Host: machineID, Kind: publish.Kind, Key: publish.Key, MeasuredAt: publish.MeasuredAt.UTC(), ReceivedAt: received, Quota: publish.Quota}
	if publish.Spawn != nil {
		if publish.Spawn.Host != "" && publish.Spawn.Host != machineID {
			h.logger.Warn("fleet state for another host refused", "machine", machineID, "kind", publish.Kind)
			return false
		}
		spawn := *publish.Spawn
		spawn.Host, spawn.At = machineID, spawn.At.UTC()
		message.Spawn = &spawn
	}
	h.fleetState.publish(message, fleetStateNodeRepeat)
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
	h.fleetState.publish(FleetStateMessage{Host: machineID, Kind: fleetStateKindPublisher, MeasuredAt: publisher.LastSeen, ReceivedAt: now, Publisher: &publisher}, fleetStatePublisherBeat)
}

// sweepFleetState re-emits publisher state (so a stale node is announced) and
// drops expired ended spawns. Sweep calls it with the maintenance clock.
func (h *HubServer) sweepFleetState(now time.Time) {
	h.mu.Lock()
	machines := make([]string, 0, len(h.nodes))
	for machine := range h.nodes {
		machines = append(machines, machine)
	}
	h.mu.Unlock()
	for _, machine := range machines {
		h.publishFleetPublisher(machine, now)
	}
	h.fleetState.expire(now)
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
		ctx, cancel := context.WithTimeout(h.closeCtx, 10*time.Second)
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
			LoadRatio:      candidate.LoadRatio,
			Score:          math.Round(candidate.Score*1000) / 1000,
			CapacityWeight: policy.capacityWeight(candidate.Machine),
			Connected:      candidate.Connected,
			Accepting:      candidate.Connected && !strings.Contains(reason, "not_accepting"),
			SlotFull:       candidate.slotFull,
			ActiveJobs:     candidate.ActiveJobs,
			Reason:         reason,
			Source:         source,
		}
		h.fleetState.publish(FleetStateMessage{Host: candidate.Machine, Kind: fleetStateKindLoad, MeasuredAt: now, ReceivedAt: now, Load: &load}, fleetStateLoadRepublish)
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
	writeJSON(writer, http.StatusOK, h.fleetState.snapshot(h.now().UTC()))
}

// handleFleetStateStream serves the state stream. The id of each event is the
// message id, so a reconnecting EventSource resumes with Last-Event-ID.
func (h *HubServer) handleFleetStateStream(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeFleetStateRead(writer, request) {
		return
	}
	lastID := strings.TrimSpace(request.Header.Get("Last-Event-ID"))
	if lastID == "" {
		lastID = strings.TrimSpace(request.URL.Query().Get("last_event_id"))
	}
	if len(lastID) > 64 {
		lastID = "invalid"
	}
	reader, replay, reset, ok := h.fleetState.subscribe(lastID)
	if !ok {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "state_readers_full"})
		return
	}
	defer h.fleetState.unsubscribe(reader)
	controller := http.NewResponseController(writer)
	header := writer.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-store")
	header.Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)
	write := func(chunk string) bool {
		if _, err := writer.Write([]byte(chunk)); err != nil {
			return false
		}
		return controller.Flush() == nil
	}
	head := "retry: 3000\n\n"
	if reset {
		head += fmt.Sprintf("event: reset\ndata: {\"epoch\":%q}\n\n", h.fleetState.epoch)
	}
	for _, message := range replay {
		head += fleetStateEvent(message)
	}
	if !write(head) {
		return
	}
	keepalive := time.NewTicker(fleetStateKeepalive)
	defer keepalive.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case <-h.closeCtx.Done():
			return
		case <-reader.overflow:
			return
		case message := <-reader.messages:
			if !write(fleetStateEvent(message)) {
				return
			}
		case <-keepalive.C:
			if !write(": keepalive\n\n") {
				return
			}
		}
	}
}

func fleetStateEvent(message FleetStateMessage) string {
	encoded, _ := json.Marshal(message)
	return "id: " + message.ID + "\nevent: state\ndata: " + string(encoded) + "\n\n"
}
