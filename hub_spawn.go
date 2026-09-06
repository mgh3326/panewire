package panewire

import (
	"encoding/json"
	"net/http"
	"regexp"
	"time"
	"unicode/utf8"
)

const (
	hubSpawnMaxBriefBytes = 64 << 10
	// A JSON string may use a six-byte escape for each byte of an otherwise
	// valid brief. Keep the HTTP allowance above that representation while the
	// decoded brief itself remains strictly capped at 64 KiB.
	hubSpawnMaxBodyBytes = 512 << 10
	hubSpawnResultTail   = 4 << 10
	hubSpawnRetention    = time.Hour
)

var (
	hubSpawnRequestIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	hubSpawnMachinePattern   = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
	hubSpawnCWDKeyPattern    = regexp.MustCompile(`^[a-z0-9._-]{1,64}$`)
	hubSpawnArgValuePattern  = regexp.MustCompile(`^[A-Za-z0-9._:/@-]{1,200}$`)
)

type hubSpawnBrief struct {
	Inline string `json:"inline"`
}

type hubSpawnRequest struct {
	RequestID   string        `json:"request_id"`
	Machine     string        `json:"machine"`
	CWDKey      string        `json:"cwd_key"`
	Brief       hubSpawnBrief `json:"brief"`
	Args        []string      `json:"args"`
	WaitSeconds int           `json:"wait_seconds"`
}

type hubSpawnResult struct {
	RequestID   string     `json:"request_id"`
	Machine     string     `json:"machine"`
	RC          int        `json:"rc"`
	JobID       string     `json:"job_id"`
	Pane        string     `json:"pane"`
	StdoutTail  string     `json:"stdout_tail"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Error       string     `json:"error,omitempty"`
}

type hubSpawnRecord struct {
	request hubSpawnRequest
	result  hubSpawnResult
	status  string
	created time.Time
	waiter  chan hubSpawnResult
}

func validHubSpawnRequestID(value string) bool { return hubSpawnRequestIDPattern.MatchString(value) }

func validHubSpawnRequest(request hubSpawnRequest) bool {
	if request.Args == nil || !validHubSpawnRequestID(request.RequestID) || !hubSpawnMachinePattern.MatchString(request.Machine) || !hubSpawnCWDKeyPattern.MatchString(request.CWDKey) || request.WaitSeconds < 1 || request.WaitSeconds > 300 || request.Brief.Inline == "" || !utf8.ValidString(request.Brief.Inline) || len([]byte(request.Brief.Inline)) > hubSpawnMaxBriefBytes {
		return false
	}
	return validHubSpawnArgs(request.Args)
}

func validHubSpawnArgs(args []string) bool {
	if len(args)%2 != 0 {
		return false
	}
	allowed := map[string]struct{}{
		"-m": {}, "-w": {}, "-l": {}, "--t": {}, "--effort": {}, "--job": {}, "--owner": {}, "--report": {}, "--role": {}, "--lane": {}, "--parent": {}, "-L": {},
	}
	for index := 0; index < len(args); index += 2 {
		if _, found := allowed[args[index]]; !found || !hubSpawnArgValuePattern.MatchString(args[index+1]) {
			return false
		}
	}
	return true
}

// parseHubSpawnResult is deliberately separate from parseHubInbound: a spawn
// result is a node-to-hub control response, not an event envelope.
func parseHubSpawnResult(raw []byte) (hubSpawnResult, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil || (len(fields) != 6 && len(fields) != 7) {
		return hubSpawnResult{}, false
	}
	var kind string
	var result hubSpawnResult
	if json.Unmarshal(fields["type"], &kind) != nil || kind != "job.spawn.result" ||
		json.Unmarshal(fields["request_id"], &result.RequestID) != nil || !validHubSpawnRequestID(result.RequestID) ||
		json.Unmarshal(fields["rc"], &result.RC) != nil || result.RC < 0 ||
		json.Unmarshal(fields["job_id"], &result.JobID) != nil || len(result.JobID) > 512 ||
		json.Unmarshal(fields["pane"], &result.Pane) != nil || len(result.Pane) > 512 ||
		json.Unmarshal(fields["stdout_tail"], &result.StdoutTail) != nil || len([]byte(result.StdoutTail)) > hubSpawnResultTail {
		return hubSpawnResult{}, false
	}
	if rawError, found := fields["error"]; found {
		if json.Unmarshal(rawError, &result.Error) != nil || !validHubSpawnResultError(result.Error) {
			return hubSpawnResult{}, false
		}
	} else if len(fields) != 6 {
		return hubSpawnResult{}, false
	}
	return result, true
}

func validHubSpawnResultError(value string) bool {
	switch value {
	case "spawn_disabled", "cwd_unmapped", "spawn_failed", "timeout":
		return true
	default:
		return false
	}
}

func (h *HubServer) pruneSpawnRecordsLocked(now time.Time) {
	for requestID, record := range h.spawnRecords {
		if now.Sub(record.created) >= hubSpawnRetention {
			delete(h.spawnRecords, requestID)
		}
	}
}

func (h *HubServer) spawnRecordResponse(record *hubSpawnRecord) any {
	if record.status == "completed" {
		return record.result
	}
	return struct {
		RequestID string `json:"request_id"`
		Status    string `json:"status"`
	}{RequestID: record.request.RequestID, Status: record.status}
}

func (h *HubServer) spawnUnavailableResponse(machine string) any {
	h.mu.Lock()
	defer h.mu.Unlock()
	state := "disconnected"
	if record := h.nodes[machine]; record != nil {
		state = record.state
		if record.agent != nil && record.state == "connected" && !h.acceptingEffectiveLocked(machine, record.accepting) {
			state = "not_accepting"
		}
	}
	return struct {
		Error string `json:"error"`
		State string `json:"state"`
	}{Error: "node_unavailable", State: state}
}

func (h *HubServer) handleSpawn(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	var body hubSpawnRequest
	if !decodeHubJSON(writer, request, hubSpawnMaxBodyBytes, &body) || !validHubSpawnRequest(body) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}

	now := h.now().UTC()
	h.mu.Lock()
	h.pruneSpawnRecordsLocked(now)
	if existing := h.spawnRecords[body.RequestID]; existing != nil {
		response := h.spawnRecordResponse(existing)
		h.mu.Unlock()
		writeHubJSON(writer, http.StatusConflict, response)
		return
	}
	record := h.nodes[body.Machine]
	if record == nil || record.agent == nil || record.state != "connected" || !h.acceptingEffectiveLocked(body.Machine, record.accepting) {
		h.mu.Unlock()
		writeHubJSON(writer, http.StatusServiceUnavailable, h.spawnUnavailableResponse(body.Machine))
		return
	}
	entry := &hubSpawnRecord{request: body, status: "pending", created: now, waiter: make(chan hubSpawnResult, 1)}
	h.spawnRecords[body.RequestID] = entry
	agent := record.agent
	h.mu.Unlock()

	message := struct {
		Type        string        `json:"type"`
		RequestID   string        `json:"request_id"`
		CWDKey      string        `json:"cwd_key"`
		Brief       hubSpawnBrief `json:"brief"`
		Args        []string      `json:"args"`
		WaitSeconds int           `json:"wait_seconds"`
	}{Type: "job.spawn", RequestID: body.RequestID, CWDKey: body.CWDKey, Brief: body.Brief, Args: body.Args, WaitSeconds: body.WaitSeconds}
	if err := agent.writeJSON(message); err != nil {
		h.mu.Lock()
		delete(h.spawnRecords, body.RequestID)
		h.mu.Unlock()
		writeHubJSON(writer, http.StatusServiceUnavailable, h.spawnUnavailableResponse(body.Machine))
		return
	}
	h.broadcastSpawnRequested(body)

	select {
	case result := <-entry.waiter:
		if result.CompletedAt != nil {
			writeHubJSON(writer, http.StatusOK, result)
			return
		}
		writeHubJSON(writer, http.StatusOK, struct {
			RequestID string `json:"request_id"`
			Status    string `json:"status"`
		}{RequestID: body.RequestID, Status: "lost"})
	case <-time.After(time.Duration(body.WaitSeconds) * time.Second):
		writeHubJSON(writer, http.StatusGatewayTimeout, struct {
			RequestID string `json:"request_id"`
			Status    string `json:"status"`
		}{RequestID: body.RequestID, Status: "pending"})
	}
}

func (h *HubServer) handleSpawnGet(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	requestID := request.PathValue("request_id")
	if !validHubSpawnRequestID(requestID) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	h.mu.Lock()
	h.pruneSpawnRecordsLocked(h.now().UTC())
	record := h.spawnRecords[requestID]
	if record == nil {
		h.mu.Unlock()
		writeHubJSON(writer, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	response := h.spawnRecordResponse(record)
	h.mu.Unlock()
	writeHubJSON(writer, http.StatusOK, response)
}

func (h *HubServer) resolveSpawn(machine string, result hubSpawnResult) {
	h.mu.Lock()
	record := h.spawnRecords[result.RequestID]
	if record == nil || record.status != "pending" || record.request.Machine != machine {
		h.mu.Unlock()
		return
	}
	completed := h.now().UTC()
	result.Machine = machine
	result.CompletedAt = &completed
	record.result = result
	record.status = "completed"
	waiter := record.waiter
	h.mu.Unlock()
	h.broadcastSpawnResult(result)
	select {
	case waiter <- result:
	default:
	}
}

func (h *HubServer) loseSpawns(machine string, agent *hubAgent) {
	h.mu.Lock()
	if record := h.nodes[machine]; record == nil || record.agent != agent {
		h.mu.Unlock()
		return
	}
	var waiters []chan hubSpawnResult
	for _, record := range h.spawnRecords {
		if record.status == "pending" && record.request.Machine == machine {
			record.status = "lost"
			waiters = append(waiters, record.waiter)
		}
	}
	h.mu.Unlock()
	for _, waiter := range waiters {
		select {
		case waiter <- hubSpawnResult{}:
		default:
		}
	}
}

func (h *HubServer) broadcastSpawnRequested(request hubSpawnRequest) {
	payload, _ := json.Marshal(struct {
		RequestID string `json:"request_id"`
		Machine   string `json:"machine"`
	}{RequestID: request.RequestID, Machine: request.Machine})
	h.broadcast(hubEvent{Kind: "spawn.requested", Payload: payload, Received: h.now().UTC()})
}

func (h *HubServer) broadcastSpawnResult(result hubSpawnResult) {
	payload, _ := json.Marshal(struct {
		RequestID string `json:"request_id"`
		Machine   string `json:"machine"`
		RC        int    `json:"rc"`
	}{RequestID: result.RequestID, Machine: result.Machine, RC: result.RC})
	h.broadcast(hubEvent{Kind: "spawn.result", Payload: payload, Received: h.now().UTC()})
}
