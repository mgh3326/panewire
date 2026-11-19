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
	// hubCheckoutRefusalPattern is the closed refusal-error shape: an outcome
	// alone, or the bounded detail a successor refusal names for a hub that
	// listed checkout_branch_v1 — the wanted and origin commits, or the
	// worktree that holds the branch (hk 5118).
	hubCheckoutRefusalPattern       = regexp.MustCompile(`^checkout_refused:[a-z0-9_.-]{1,64}$`)
	hubCheckoutRefusalDetailPattern = regexp.MustCompile(`^checkout_refused:[a-z0-9_.-]{1,64}: (?:want=[0-9a-f]{40} origin=(?:[0-9a-f]{40}|none)|worktree=[^\x00-\x1f\x7f]{1,512})$`)
)

// hubCheckoutRefusalMaxBytes bounds a detailed checkout refusal the hub reads.
const hubCheckoutRefusalMaxBytes = 640

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
	Origin      string `json:"origin"`
	Mode        string `json:"mode"`
	Branch      string `json:"branch,omitempty"`
	Sha         string `json:"sha,omitempty"`
	Fetch       string `json:"fetch,omitempty"`
	Dir         string `json:"dir"`
	VerifyReuse bool   `json:"verify_reuse,omitempty"`
	// Exclude and Include are the sparse checkout lists the node turns into
	// its own pattern list (hk 5098). They travel only to a node that
	// advertises checkout_sparse_v1; every other node gets the full checkout.
	Exclude []string `json:"exclude,omitempty"`
	Include []string `json:"include,omitempty"`
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
	// GrantRequired is fixed at bind: the attempt goes only to a node that
	// starts it after the hub's durable grant, and only such an attempt may
	// be fenced on silence (hub_spawn_grant.go).
	GrantRequired bool `json:"grant_required,omitempty"`
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
	// CheckoutMode is the node's answer for a checkout spec: "sparse" or
	// "full" (hk 5098), or "branch" for a successor checkout (hk 5118).
	// Empty for a spawn that names no spec.
	CheckoutMode string `json:"checkout_mode,omitempty"`
}

type hubSpawnRecord struct {
	request hubSpawnRequest
	result  hubSpawnResult
	status  string
	created time.Time
	waiter  chan hubSpawnResult
	// checkoutStripped records that the hub dropped this request's sparse
	// lists because the node does not advertise checkout_sparse_v1 (hk 5098).
	// A node from before the feature answers no checkout_mode at all, so the
	// hub then reports "full" itself rather than leaving the answer silent.
	checkoutStripped bool
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
	// The sparse lists belong to a detached spec only (hk 5098): the same rule
	// the node applies, so a branch-mode request with lists is refused here
	// instead of being accepted and refused at the node or silently run full.
	if spawnargs.SparseSpecRefusal(checkout.Mode, checkout.Exclude, checkout.Include) != "" {
		return false
	}
	switch checkout.Mode {
	case "branch":
		return validNodeRefName(checkout.Branch) && (checkout.Sha == "" || nodeShaPattern.MatchString(checkout.Sha)) && (!checkout.VerifyReuse || checkout.Sha != "") && checkout.Fetch == ""
	case "detached":
		return !checkout.VerifyReuse && checkout.Branch == "" && nodeShaPattern.MatchString(checkout.Sha) && validNodeFetchRef(checkout.Fetch)
	case fleet.HubCheckoutSuccessor:
		// An implementer spec on refs/heads/<branch> (hk 5118): the node
		// prepares a local branch at the pinned commit, so both are required
		// and it fetches the whole remote itself. A snapshot hand-over's
		// fetch is the hand-over ref the commit lives under (hk 5324).
		return !checkout.VerifyReuse && validNodeRefName(checkout.Branch) && nodeShaPattern.MatchString(checkout.Sha) && (checkout.Fetch == "" || validHandoverFetchRef(checkout.Fetch))
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

// withoutSpawnFlag returns args without flag and the one value it takes. The
// hub drops a flag an older node's argument rule would refuse before it binds
// the attempt; every flag that rule admits a value for takes exactly one.
func withoutSpawnFlag(args []string, flag string) []string {
	for i, arg := range args {
		if arg != flag {
			continue
		}
		out := make([]string, 0, len(args))
		out = append(out, args[:i]...)
		if i+1 < len(args) {
			out = append(out, args[i+2:]...)
		}
		return out
	}
	return args
}

// parseHubSpawnResult is deliberately separate from parseHubInbound: a spawn
// result is a node-to-hub control response, not an event envelope.
func parseHubSpawnResult(raw []byte) (hubSpawnResult, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil || (len(fields) < 6 || len(fields) > 12) {
		return hubSpawnResult{}, false
	}
	for key := range fields {
		switch key {
		case "type", "request_id", "rc", "job_id", "pane", "stdout_tail", "error", "stage", "stage_ms", "retryable", "receipt", "checkout_mode":
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
	if raw, ok := fields["checkout_mode"]; ok {
		if json.Unmarshal(raw, &result.CheckoutMode) != nil || !validSpawnCheckoutMode(result.CheckoutMode) {
			return hubSpawnResult{}, false
		}
	}
	return result, true
}

// validSpawnCheckoutMode is the closed set a node may answer for a spec
// checkout (hk 5098, hk 5118).
func validSpawnCheckoutMode(value string) bool {
	return value == "" || value == nodeCheckoutSparse || value == nodeCheckoutFull || value == nodeCheckoutBranch
}

func validHubSpawnResultError(value string) bool {
	switch value {
	case "spawn_disabled", "cwd_unmapped", "spawn_failed", "launch_proof_failed", "timeout", "brief_doc_unavailable", "journal_unavailable", "journal_conflict", "reconciling", "unknown", "deadline_not_started", "invalid_args", "invalid_request":
		return true
	default:
		if !strings.HasPrefix(value, "checkout_refused:") || len(value) > hubCheckoutRefusalMaxBytes {
			return false
		}
		return hubCheckoutRefusalPattern.MatchString(value) || hubCheckoutRefusalDetailPattern.MatchString(value)
	}
}

// hubCheckoutRefusalDetail returns the ": <reason>" a successor refusal may
// append to its error, or "" when the reason cannot be carried inside the
// bounded shape the hub parses. A node checks the same pattern before it puts
// the detail on the wire, so a long worktree path or a control byte falls back
// to the outcome alone instead of making the hub drop the whole answer (hk
// 5118).
func hubCheckoutRefusalDetail(outcome, reason string) string {
	if reason == "" {
		return ""
	}
	detail := ": " + reason
	if !hubCheckoutRefusalDetailPattern.MatchString("checkout_refused:" + outcome + detail) {
		return ""
	}
	return detail
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
	// An older node's argument rule refuses --consult-stamp and drops the whole
	// spawn; the stamp is an extra, so the spawn goes without it (hk 4875).
	if record == nil || !record.capabilities[hubCapabilityConsultStamp] {
		body.Args = withoutSpawnFlag(body.Args, fleet.ConsultStampArg)
	}
	if record == nil || !record.capabilities[hubCapabilityOwnerNotify] {
		body.Args = withoutSpawnFlag(body.Args, fleet.OwnerNotifyArg)
	}
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
	// Only a node that advertises checkout_sparse_v1 reads exclude/include;
	// every other node gets the full checkout and says so (hk 5098). The
	// record keeps the request as sent, so a replay to a capable node keeps
	// the lists.
	checkout := sparseCheckoutForNode(body.Checkout, record.capabilities)
	entry.checkoutStripped = checkout != body.Checkout
	h.mu.Unlock()

	message := struct {
		Type        string            `json:"type"`
		RequestID   string            `json:"request_id"`
		CWDKey      string            `json:"cwd_key"`
		Brief       hubSpawnBrief     `json:"brief"`
		Checkout    *hubSpawnCheckout `json:"checkout,omitempty"`
		Args        []string          `json:"args"`
		WaitSeconds int               `json:"wait_seconds"`
	}{Type: "job.spawn", RequestID: body.RequestID, CWDKey: body.CWDKey, Brief: body.Brief, Checkout: checkout, Args: body.Args, WaitSeconds: body.WaitSeconds}
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
	// A pre-feature node cannot answer checkout_mode; the hub knows the
	// request asked for a sparse checkout and the node got no lists, so the
	// checkout it prepared is the full one (hk 5098 review).
	if result.CheckoutMode == "" && record.checkoutStripped {
		result.CheckoutMode = nodeCheckoutFull
	}
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
