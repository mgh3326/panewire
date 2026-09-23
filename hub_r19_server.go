package panewire

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"
)

type hubQuotaResult struct {
	Payload string `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
}
type hubQuotaCacheEntry struct {
	result  hubQuotaResult
	expires time.Time
}
type hubQuotaReport struct {
	RequestID string
	Payload   string
	Error     string
}

func hubQuotaCacheTTL() time.Duration {
	if value, err := time.ParseDuration(os.Getenv("QUOTA_CACHE_TTL")); err == nil && value > 0 {
		return value
	}
	return 5 * time.Minute
}

func parseHubQuotaReport(raw []byte) (hubQuotaReport, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) < 3 || len(fields) > 3 {
		return hubQuotaReport{}, false
	}
	var report hubQuotaReport
	var kind string
	if json.Unmarshal(fields["type"], &kind) != nil || kind != "quota.report" || json.Unmarshal(fields["request_id"], &report.RequestID) != nil || !validHubRequestID(report.RequestID) {
		return hubQuotaReport{}, false
	}
	if value, found := fields["payload"]; found {
		if json.Unmarshal(value, &report.Payload) != nil || report.Payload == "" {
			return hubQuotaReport{}, false
		}
	}
	if value, found := fields["error"]; found {
		if json.Unmarshal(value, &report.Error) != nil || (report.Error != "unsupported" && report.Error != "scopefuel failed" && report.Error != "output_too_large" && report.Error != "timeout") {
			return hubQuotaReport{}, false
		}
	}
	return report, (report.Payload == "") != (report.Error == "")
}

func (h *HubServer) resolveQuota(machine string, report hubQuotaReport) {
	h.mu.Lock()
	waiter := h.quotaWaiters[report.RequestID]
	if waiter != nil {
		delete(h.quotaWaiters, report.RequestID)
	}
	h.mu.Unlock()
	if waiter != nil {
		select {
		case waiter <- hubQuotaResult{Payload: report.Payload, Error: report.Error}:
		default:
		}
	}
}

func hubQuotaRequestID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "quota-" + hex.EncodeToString(b)
}

func (h *HubServer) handleQuotaGet(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	machine := r.PathValue("machine")
	if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID {
		http.Error(w, "invalid machine", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	cache, found := h.quotaCache[machine]
	h.mu.Unlock()
	if !found || !h.now().Before(cache.expires) {
		http.Error(w, "quota unavailable", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Cached bool `json:"cached"`
		hubQuotaResult
	}{Cached: true, hubQuotaResult: cache.result})
}

func (h *HubServer) handleQuotaRequest(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	machine := r.PathValue("machine")
	if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID {
		http.Error(w, "invalid machine", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	if cache, found := h.quotaCache[machine]; found && h.now().Before(cache.expires) {
		h.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Cached bool `json:"cached"`
			hubQuotaResult
		}{Cached: true, hubQuotaResult: cache.result})
		return
	}
	record := h.nodes[machine]
	if record == nil || record.agent == nil || record.state != "connected" {
		h.mu.Unlock()
		http.Error(w, "node unavailable", http.StatusServiceUnavailable)
		return
	}
	id, waiter := hubQuotaRequestID(), make(chan hubQuotaResult, 1)
	h.quotaWaiters[id] = waiter
	agent := record.agent
	h.mu.Unlock()
	if err := agent.writeJSON(struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Tool      string `json:"tool"`
	}{"quota.request", id, "scopefuel"}); err != nil {
		h.mu.Lock()
		delete(h.quotaWaiters, id)
		h.mu.Unlock()
		http.Error(w, "node unavailable", http.StatusServiceUnavailable)
		return
	}
	select {
	case result := <-waiter:
		h.mu.Lock()
		h.quotaCache[machine] = hubQuotaCacheEntry{result: result, expires: h.now().Add(h.quotaCacheTTL)}
		h.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Cached bool `json:"cached"`
			hubQuotaResult
		}{Cached: false, hubQuotaResult: result})
	case <-time.After(20 * time.Second):
		h.mu.Lock()
		delete(h.quotaWaiters, id)
		h.mu.Unlock()
		http.Error(w, "quota timeout", http.StatusGatewayTimeout)
	}
}

func (h *HubServer) handleUpdatePublish(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	var request struct {
		Version  string   `json:"version"`
		SHA256   string   `json:"sha256"`
		URL      string   `json:"url"`
		Machines []string `json:"machines"`
	}
	// The URL must be this repository's release asset for exactly the
	// published version (hubUpdateRepository pin, not merely a GitHub host).
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request) != nil || !hubVersionPattern.MatchString(request.Version) || !validHubSHA256(request.SHA256) || !validHubUpdateURLForVersion(request.URL, h.updateRepository, request.Version) || len(request.Machines) == 0 {
		http.Error(w, "invalid update", http.StatusBadRequest)
		return
	}
	// BLOCK-3: the NCP root node shares its executable with the hub. The whole
	// request is refused before any node lookup, so no target list that names
	// it in any spelling is partially published.
	seen := make(map[string]bool, len(request.Machines))
	for _, machine := range request.Machines {
		folded := strings.ToLower(strings.TrimSpace(machine))
		if hubUpdateExcludedMachine(machine) {
			http.Error(w, "update excluded machine", http.StatusBadRequest)
			return
		}
		if seen[folded] {
			http.Error(w, "duplicate machine", http.StatusBadRequest)
			return
		}
		seen[folded] = true
	}
	h.mu.Lock()
	agents := make([]*hubAgent, 0, len(request.Machines))
	for _, machine := range request.Machines {
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || h.nodes[machine] == nil || h.nodes[machine].agent == nil {
			h.mu.Unlock()
			http.Error(w, "node unavailable", http.StatusConflict)
			return
		}
		agents = append(agents, h.nodes[machine].agent)
	}
	deadline := h.now().UTC().Add(h.updateConfirmationTimeout)
	for _, machine := range request.Machines {
		h.expectedVersion[machine] = hubExpectedVersion{version: request.Version, deadline: deadline}
	}
	h.mu.Unlock()
	message := struct {
		Type    string `json:"type"`
		Version string `json:"version"`
		SHA256  string `json:"sha256"`
		URL     string `json:"url"`
	}{"update.available", request.Version, request.SHA256, request.URL}
	for _, agent := range agents {
		if agent.writeJSON(message) != nil {
			h.mu.Lock()
			for _, machine := range request.Machines {
				delete(h.expectedVersion, machine)
			}
			h.mu.Unlock()
			http.Error(w, "node unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{"published": request.Machines})
}

type hubUpdateOverdue struct {
	machine  string
	version  string
	deadline time.Time
}

// updateOverdueLocked judges one expired expectation. A node that is not on
// the published version is overdue; each (machine, version) pair is reported
// once, however often that version is republished.
func (h *HubServer) updateOverdueLocked(machineID string, expected hubExpectedVersion, now time.Time) (hubUpdateOverdue, bool) {
	if record := h.nodes[machineID]; record != nil && record.remoteMeta["version"] == expected.version {
		return hubUpdateOverdue{}, false
	}
	if h.updateOverdueNotified[machineID] == expected.version {
		return hubUpdateOverdue{}, false
	}
	h.updateOverdueNotified[machineID] = expected.version
	h.recordUIEventLocked("update", "overdue", machineID, now)
	h.lastNotes[machineID] = &HubLastNote{Text: "update overdue " + expected.version, ReceivedAt: now}
	return hubUpdateOverdue{machine: machineID, version: expected.version, deadline: expected.deadline}, true
}

// emitUpdateOverdue writes the update.overdue row to the configured operator
// sink lane. It never uses the Telegram notifier and never reaches a pane: a
// lane that is not a sink is refused inside relayLaneEvent. The event id is
// derived from (machine, version), so handoffkeep's first-writer-wins row also
// dedupes across hub restarts.
func (h *HubServer) emitUpdateOverdue(notice hubUpdateOverdue) {
	lane := h.updateOverdueLane
	if lane == "" {
		return
	}
	eventID := "update.overdue:" + notice.machine + ":" + notice.version
	event := hubJobEventPayload{
		JobID:     laneEventTransportID(lane, eventID),
		Epoch:     1,
		OwnerLane: lane,
		EventID:   eventID,
		Text:      "update.overdue machine=" + notice.machine + " version=" + notice.version + " deadline=" + notice.deadline.UTC().Format(time.RFC3339),
		Label:     "hub-update",
		Host:      "hub",
		Reason:    "update_overdue",
		sinkOnly:  true,
	}
	result := h.relayLaneEvent(event, nil)
	if !result.Routed && !result.Duplicate && !result.AlreadyDelivered {
		h.logger.Warn("update.overdue was not recorded in its sink lane", "lane", lane, "machine", notice.machine)
	}
}
