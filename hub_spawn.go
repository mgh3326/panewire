package panewire

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/spawnargs"
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
)

type hubSpawnBrief struct {
	Inline    string `json:"inline,omitempty"`
	Doc       string `json:"doc,omitempty"`
	hasInline bool
	hasDoc    bool
}

func (brief *hubSpawnBrief) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if fields == nil {
		return errors.New("invalid brief")
	}
	for key := range fields {
		if key != "inline" && key != "doc" {
			return errors.New("invalid brief")
		}
	}
	if value, ok := fields["inline"]; ok {
		brief.hasInline = true
		if err := json.Unmarshal(value, &brief.Inline); err != nil {
			return err
		}
	}
	if value, ok := fields["doc"]; ok {
		brief.hasDoc = true
		if err := json.Unmarshal(value, &brief.Doc); err != nil {
			return err
		}
	}
	return nil
}

type hubSpawnCheckout struct {
	Origin string `json:"origin"`
	Mode   string `json:"mode"`
	Branch string `json:"branch,omitempty"`
	Sha    string `json:"sha,omitempty"`
	Fetch  string `json:"fetch,omitempty"`
	Dir    string `json:"dir"`
}

type hubSpawnRequest struct {
	DeadlineMS  int64             `json:"deadline_ms,omitempty"`
	PlacementMS int64             `json:"hub_placement_ms,omitempty"`
	RequestID   string            `json:"request_id"`
	Machine     string            `json:"machine"`
	CWDKey      string            `json:"cwd_key"`
	Brief       hubSpawnBrief     `json:"brief"`
	Checkout    *hubSpawnCheckout `json:"checkout,omitempty"`
	Args        []string          `json:"args"`
	WaitSeconds int               `json:"wait_seconds"`
}

type hubSpawnResult struct {
	Receipt     *fleet.ExecutionReceipt `json:"receipt,omitempty"`
	Stage       string                  `json:"stage,omitempty"`
	StageMS     map[string]int64        `json:"stage_ms,omitempty"`
	Retryable   bool                    `json:"retryable,omitempty"`
	RequestID   string                  `json:"request_id"`
	Machine     string                  `json:"machine"`
	RC          int                     `json:"rc"`
	JobID       string                  `json:"job_id"`
	Pane        string                  `json:"pane"`
	StdoutTail  string                  `json:"stdout_tail"`
	CompletedAt *time.Time              `json:"completed_at,omitempty"`
	Error       string                  `json:"error,omitempty"`
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
	return validHubSpawnRequestForMode(request, false)
}

func validHubSpawnRequestForMode(request hubSpawnRequest, operator bool) bool {
	maxWait := 300
	if operator {
		maxWait = 3600
		if request.DeadlineMS <= 0 {
			return false
		}
	} else if request.DeadlineMS != 0 || request.PlacementMS != 0 {
		return false
	}
	inline, doc := request.Brief.hasInline || request.Brief.Inline != "", request.Brief.hasDoc || request.Brief.Doc != ""
	if request.Args == nil || !validHubSpawnRequestID(request.RequestID) || !hubSpawnMachinePattern.MatchString(request.Machine) || !hubSpawnCWDKeyPattern.MatchString(request.CWDKey) || request.WaitSeconds < 1 || request.WaitSeconds > maxWait || inline == doc || inline && (request.Brief.Inline == "" || !utf8.ValidString(request.Brief.Inline) || len([]byte(request.Brief.Inline)) > hubSpawnMaxBriefBytes) || doc && (request.Brief.Doc == "" || !validHubSpawnDocKey(request.Brief.Doc)) {
		return false
	}
	return validHubSpawnArgsForMode(request.Args, operator) && (request.Checkout == nil || validHubSpawnCheckout(*request.Checkout))
}

func validHubSpawnDocKey(key string) bool {
	return spawnargs.ValidDocKey(key)
}

func validHubSpawnCheckout(checkout hubSpawnCheckout) bool {
	ownerRepo, ok := nodeOriginRepo(checkout.Origin)
	if !ok || !validNodeJobName(checkout.Dir) {
		return false
	}
	repo := ownerRepo[strings.IndexByte(ownerRepo, '/')+1:]
	if strings.EqualFold(checkout.Dir, repo) {
		return false
	}
	switch checkout.Mode {
	case "branch":
		return validNodeRefName(checkout.Branch) && (checkout.Sha == "" || nodeShaPattern.MatchString(checkout.Sha)) && checkout.Fetch == ""
	case "detached":
		return checkout.Branch == "" && nodeShaPattern.MatchString(checkout.Sha) && validNodeFetchRef(checkout.Fetch)
	default:
		return false
	}
}

func validHubSpawnArgs(args []string) bool {
	return validHubSpawnArgsForMode(args, false)
}

// validHubSpawnArgsForMode is the node's argument rule; spawnargs states it
// once for the node and for the submitter that builds the same args.
func validHubSpawnArgsForMode(args []string, operator bool) bool {
	if spawnargs.Refusal(args, operator) != "" {
		return false
	}
	// The arm names the spawn's own canon (-m), the rule submission applies.
	arm := hubSpawnArg(args, "--e6-arm")
	return arm == "" || fleet.ValidE6Arm(arm, hubSpawnArg(args, "-m"))
}

// parseHubSpawnResult is deliberately separate from parseHubInbound: a spawn
// result is a node-to-hub control response, not an event envelope.
func parseHubSpawnResult(raw []byte) (hubSpawnResult, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil || (len(fields) < 6 || len(fields) > 11) {
		return hubSpawnResult{}, false
	}
	for key := range fields {
		switch key {
		case "type", "request_id", "rc", "job_id", "pane", "stdout_tail", "error", "stage", "stage_ms", "retryable", "receipt":
		default:
			return hubSpawnResult{}, false
		}
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
	}
	if raw, ok := fields["stage"]; ok {
		if json.Unmarshal(raw, &result.Stage) != nil || !validSpawnStage(result.Stage) {
			return hubSpawnResult{}, false
		}
	}
	if raw, ok := fields["stage_ms"]; ok {
		if json.Unmarshal(raw, &result.StageMS) != nil || !validSpawnStages(result.StageMS) {
			return hubSpawnResult{}, false
		}
	}
	if raw, ok := fields["retryable"]; ok {
		if json.Unmarshal(raw, &result.Retryable) != nil {
			return hubSpawnResult{}, false
		}
	}
	if raw, ok := fields["receipt"]; ok {
		var receipt fleet.ExecutionReceipt
		if json.Unmarshal(raw, &receipt) != nil || len(raw) > 4096 {
			return hubSpawnResult{}, false
		}
		result.Receipt = &receipt
	}
	return result, true
}

func validHubSpawnResultError(value string) bool {
	switch value {
	case "spawn_disabled", "cwd_unmapped", "spawn_failed", "launch_proof_failed", "timeout", "brief_doc_unavailable", "journal_unavailable", "journal_conflict", "reconciling", "unknown", "deadline_not_started", "invalid_args", "invalid_request":
		return true
	default:
		return strings.HasPrefix(value, "checkout_refused:") && len(value) <= 80 && regexp.MustCompile(`^checkout_refused:[a-z0-9_.-]+$`).MatchString(value)
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
	if !h.authenticateScoped(request) {
		hubUnauthorized(writer)
		return
	}
	var body hubSpawnRequest
	if !decodeHubJSON(writer, request, hubSpawnMaxBodyBytes, &body) || !validHubSpawnRequest(body) {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	if access := h.authorize(request, hubScopeSpawn, body.Machine); access != hubAccessGranted {
		writeHubAccessDenied(writer, access)
		return
	}

	now := h.now().UTC()
	h.mu.Lock()
	h.pruneSpawnRecordsLocked(now)
	if existing := h.spawnRecords[body.RequestID]; existing != nil {
		response := h.spawnRecordResponse(existing)
		h.mu.Unlock()
		writeJSON(writer, http.StatusConflict, response)
		return
	}
	record := h.nodes[body.Machine]
	if record != nil && record.agent != nil && record.state == "connected" && (body.Checkout != nil || body.Brief.Doc != "") && !record.capabilities["spawn_v2"] {
		h.mu.Unlock()
		writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "spawn_capability_required", "capability": "spawn_v2"})
		return
	}
	if record == nil || record.agent == nil || record.state != "connected" || !h.acceptingEffectiveLocked(body.Machine, record.accepting) {
		h.mu.Unlock()
		writeJSON(writer, http.StatusServiceUnavailable, h.spawnUnavailableResponse(body.Machine))
		return
	}
	entry := &hubSpawnRecord{request: body, status: "pending", created: now, waiter: make(chan hubSpawnResult, 1)}
	h.spawnRecords[body.RequestID] = entry
	agent := record.agent
	h.mu.Unlock()

	message := struct {
		Type        string            `json:"type"`
		RequestID   string            `json:"request_id"`
		CWDKey      string            `json:"cwd_key"`
		Brief       hubSpawnBrief     `json:"brief"`
		Checkout    *hubSpawnCheckout `json:"checkout,omitempty"`
		Args        []string          `json:"args"`
		WaitSeconds int               `json:"wait_seconds"`
	}{Type: "job.spawn", RequestID: body.RequestID, CWDKey: body.CWDKey, Brief: body.Brief, Checkout: body.Checkout, Args: body.Args, WaitSeconds: body.WaitSeconds}
	if err := agent.writeJSON(message); err != nil {
		h.mu.Lock()
		delete(h.spawnRecords, body.RequestID)
		h.mu.Unlock()
		writeJSON(writer, http.StatusServiceUnavailable, h.spawnUnavailableResponse(body.Machine))
		return
	}
	h.broadcastSpawnRequested(body)

	select {
	case result := <-entry.waiter:
		if result.CompletedAt != nil {
			// A request id is accepted once (a repeat is a conflict), so this
			// answer counts once toward the launch-failure backoff.
			h.observeLaunchOutcome(body.Machine, hubSpawnArg(body.Args, "-m"), result.RC, result.Error, h.now().UTC())
			writeJSON(writer, http.StatusOK, result)
			return
		}
		writeJSON(writer, http.StatusOK, struct {
			RequestID string `json:"request_id"`
			Status    string `json:"status"`
		}{RequestID: body.RequestID, Status: "lost"})
	case <-time.After(time.Duration(body.WaitSeconds) * time.Second):
		writeJSON(writer, http.StatusGatewayTimeout, struct {
			RequestID string `json:"request_id"`
			Status    string `json:"status"`
		}{RequestID: body.RequestID, Status: "pending"})
	}
}

func (h *HubServer) handleSpawnGet(writer http.ResponseWriter, request *http.Request) {
	if !h.authenticateScoped(request) {
		hubUnauthorized(writer)
		return
	}
	requestID := request.PathValue("request_id")
	if !validHubSpawnRequestID(requestID) {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	h.mu.Lock()
	h.pruneSpawnRecordsLocked(h.now().UTC())
	record := h.spawnRecords[requestID]
	if record == nil {
		h.mu.Unlock()
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	response, machine := h.spawnRecordResponse(record), record.request.Machine
	h.mu.Unlock()
	// The record's machine is the resource, so it is checked after the
	// lookup; an unknown id answers 404 to any operator or service.
	if access := h.authorize(request, hubScopeSpawn, machine); access != hubAccessGranted {
		writeHubAccessDenied(writer, access)
		return
	}
	writeJSON(writer, http.StatusOK, response)
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

// loseSpawnsLocked marks machine's pending spawns lost and returns their
// waiters for wakeLostSpawns once h.mu is released. The caller holds h.mu and
// has checked the disconnecting agent is still the machine's current one.
func (h *HubServer) loseSpawnsLocked(machine string) []chan hubSpawnResult {
	var waiters []chan hubSpawnResult
	for _, record := range h.spawnRecords {
		if record.status == "pending" && record.request.Machine == machine {
			record.status = "lost"
			waiters = append(waiters, record.waiter)
		}
	}
	return waiters
}

func wakeLostSpawns(waiters []chan hubSpawnResult) {
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
