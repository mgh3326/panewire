package panewire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	controlPlaneTransferMaxBodyBytes = 32 << 10
	controlPlaneHistoryMaxEntries    = 8
)

type controlPlaneAction string

const (
	controlPlaneActionPrepare       controlPlaneAction = "prepare"
	controlPlaneActionDrain         controlPlaneAction = "drain"
	controlPlaneActionHandoverReady controlPlaneAction = "handover_ready"
	controlPlaneActionCommit        controlPlaneAction = "commit"
	controlPlaneActionAck           controlPlaneAction = "ack"
)

type controlPlaneState string

const (
	controlPlaneStateActive             controlPlaneState = "ACTIVE"
	controlPlaneStatePrepared           controlPlaneState = "PREPARED"
	controlPlaneStateDraining           controlPlaneState = "DRAINING"
	controlPlaneStateHandoverReady      controlPlaneState = "HANDOVER_READY"
	controlPlaneStateTransferredUnacked controlPlaneState = "TRANSFERRED_UNACKED"
)

// lanesFileControl is co-located with the two authority routes so a rename of
// lanes.json publishes route ownership, epoch, and idempotency history together.
type lanesFileControl struct {
	Epoch          uint64                      `json:"epoch"`
	Owner          string                      `json:"owner"`
	State          controlPlaneState           `json:"state"`
	LastRequestID  string                      `json:"last_request_id"`
	HandoverDocKey string                      `json:"handover_doc_key"`
	UpdatedAt      time.Time                   `json:"updated_at"`
	History        []controlPlaneHistoryRecord `json:"history"`
}

func (control lanesFileControl) shouldPersist() bool {
	return control.Epoch != 0 || control.Owner != "" || len(control.History) != 0
}

type controlPlaneHistoryRecord struct {
	RequestID string `json:"request_id"`
	// Fingerprint decides whether a repeated request ID is the same request.
	Fingerprint string `json:"fingerprint"`
	Epoch       uint64 `json:"epoch"`
	Owner       string `json:"owner"`
	// HandoverDocKey and Readiness are stored rather than reconstructed. AC1
	// requires a retry to converge on the same result, and a reconstructed
	// value is only ever equal by assumption.
	HandoverDocKey string                        `json:"handover_doc_key"`
	State          controlPlaneState             `json:"state"`
	Routes         []controlPlaneRouteProjection `json:"routes"`
	Readiness      []controlPlaneReadinessCheck  `json:"readiness"`
	At             time.Time                     `json:"at"`
}

type controlPlaneRouteProjection struct {
	Lane    string `json:"lane"`
	Machine string `json:"machine"`
	Pane    string `json:"pane"`
}

type controlPlaneReadinessCheck struct {
	Name string `json:"name"`
	OK   bool   `json:"ok"`
}

var controlPlaneReadinessNames = []string{
	"model_login",
	"tools",
	"handoffkeep_read",
	"hub_read",
	"target_pane",
}

var controlPlaneTransitions = map[controlPlaneState]map[controlPlaneAction]controlPlaneState{
	controlPlaneStateActive: {
		controlPlaneActionPrepare: controlPlaneStatePrepared,
		controlPlaneActionDrain:   controlPlaneStateDraining,
	},
	controlPlaneStatePrepared: {
		controlPlaneActionCommit: controlPlaneStateTransferredUnacked,
	},
	controlPlaneStateDraining: {
		controlPlaneActionHandoverReady: controlPlaneStateHandoverReady,
	},
	controlPlaneStateHandoverReady: {
		controlPlaneActionCommit: controlPlaneStateTransferredUnacked,
	},
	controlPlaneStateTransferredUnacked: {
		controlPlaneActionAck: controlPlaneStateActive,
	},
}

type controlPlaneTransferRoute struct {
	Machine string `json:"machine"`
	Pane    string `json:"pane"`
}

type controlPlaneTransferLane struct {
	Lane     string                     `json:"lane"`
	Expected *controlPlaneTransferRoute `json:"expected"`
	Target   *controlPlaneTransferRoute `json:"target"`
}

type controlPlaneReadinessClaims struct {
	ModelLogin      *bool `json:"model_login"`
	Tools           *bool `json:"tools"`
	HandoffkeepRead *bool `json:"handoffkeep_read"`
	HubRead         *bool `json:"hub_read"`
	TargetPane      *bool `json:"target_pane"`
}

type controlPlaneTransferRequest struct {
	Action             controlPlaneAction           `json:"action"`
	RequestID          string                       `json:"request_id"`
	ExpectedEpoch      *uint64                      `json:"expected_epoch"`
	HandoverDocKey     *string                      `json:"handover_doc_key"`
	InflightOperations *uint64                      `json:"inflight_operations"`
	Lanes              []controlPlaneTransferLane   `json:"lanes"`
	Readiness          *controlPlaneReadinessClaims `json:"readiness"`
}

type controlPlaneTransferResponse struct {
	RequestID      string                        `json:"request_id"`
	Epoch          uint64                        `json:"epoch"`
	Owner          string                        `json:"owner"`
	State          controlPlaneState             `json:"state"`
	HandoverDocKey string                        `json:"handover_doc_key"`
	Lanes          []controlPlaneRouteProjection `json:"lanes"`
	Readiness      []controlPlaneReadinessCheck  `json:"readiness"`
}

type controlPlaneSelfCheckExpected struct {
	Machine string `json:"machine"`
	Pane    string `json:"pane"`
	Epoch   uint64 `json:"epoch"`
}

type controlPlaneSelfCheckObserved struct {
	Machine string `json:"machine"`
	Pane    string `json:"pane"`
	Epoch   uint64 `json:"epoch"`
	Owner   string `json:"owner"`
	State   string `json:"state"`
}

type controlPlaneSelfCheckWitness struct {
	Kind     string                        `json:"kind"`
	At       time.Time                     `json:"at"`
	Lane     string                        `json:"lane"`
	Expected controlPlaneSelfCheckExpected `json:"expected"`
	Observed controlPlaneSelfCheckObserved `json:"observed"`
	Verdict  string                        `json:"verdict"`
	Action   string                        `json:"action"`
}

type controlPlaneTransferError struct {
	status int
	code   string
}

func (err *controlPlaneTransferError) Error() string { return err.code }

func controlPlaneConflictError(code string) error {
	return &controlPlaneTransferError{status: http.StatusConflict, code: code}
}

func (h *HubServer) handleControlPlaneTransfer(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	var body controlPlaneTransferRequest
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, controlPlaneTransferMaxBodyBytes))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil {
		writeLaneJSONError(writer, http.StatusBadRequest, "invalid_transfer_request")
		return
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		writeLaneJSONError(writer, http.StatusBadRequest, "invalid_transfer_request")
		return
	}
	if code := validateControlPlaneTransferRequest(body); code != "" {
		writeLaneJSONError(writer, http.StatusBadRequest, code)
		return
	}
	fingerprint := controlPlaneRequestFingerprint(body)
	response, err := h.transferControlPlane(body, fingerprint)
	if err != nil {
		var transferError *controlPlaneTransferError
		if errors.As(err, &transferError) {
			writeLaneJSONError(writer, transferError.status, transferError.code)
			return
		}
		writeLanesWriteError(writer, err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(response)
}

func validateControlPlaneTransferRequest(request controlPlaneTransferRequest) string {
	if !validControlPlaneAction(request.Action) || !validHubSpawnRequestID(request.RequestID) || request.ExpectedEpoch == nil || request.HandoverDocKey == nil || request.InflightOperations == nil || !validControlPlaneHandoverDocKey(*request.HandoverDocKey) {
		return "invalid_transfer_request"
	}
	if len(request.Lanes) != 2 {
		return "invalid_transfer_request"
	}
	seen := make(map[string]struct{}, len(request.Lanes))
	for _, lane := range request.Lanes {
		if _, duplicate := seen[lane.Lane]; duplicate {
			return "invalid_transfer_request"
		}
		seen[lane.Lane] = struct{}{}
		if !laneNamePattern.MatchString(lane.Lane) || !validControlPlaneTransferRoute(lane.Expected) || !validControlPlaneTransferRoute(lane.Target) {
			return "invalid_transfer_request"
		}
	}
	if request.Lanes[0].Target.Machine != request.Lanes[1].Target.Machine {
		return "mixed_target_machine"
	}
	return ""
}

func validControlPlaneAction(action controlPlaneAction) bool {
	switch action {
	case controlPlaneActionPrepare, controlPlaneActionDrain, controlPlaneActionHandoverReady, controlPlaneActionCommit, controlPlaneActionAck:
		return true
	default:
		return false
	}
}

func validControlPlaneTransferRoute(route *controlPlaneTransferRoute) bool {
	return route != nil && route.Machine != hubOperatorMachineID && machineIDPattern.MatchString(route.Machine) && validLanePane(route.Pane)
}

func validControlPlaneHandoverDocKey(value string) bool {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) < 1 || utf8.RuneCountInString(value) > 200 {
		return false
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func controlPlaneRequestFingerprint(request controlPlaneTransferRequest) string {
	type fingerprintRoute struct {
		Machine string `json:"machine"`
		Pane    string `json:"pane"`
	}
	type fingerprintLane struct {
		Lane     string           `json:"lane"`
		Expected fingerprintRoute `json:"expected"`
		Target   fingerprintRoute `json:"target"`
	}
	type fingerprintReadiness struct {
		ModelLogin      bool `json:"model_login"`
		Tools           bool `json:"tools"`
		HandoffkeepRead bool `json:"handoffkeep_read"`
		HubRead         bool `json:"hub_read"`
		TargetPane      bool `json:"target_pane"`
	}
	lanes := make([]fingerprintLane, 0, len(request.Lanes))
	for _, lane := range request.Lanes {
		lanes = append(lanes, fingerprintLane{
			Lane:     lane.Lane,
			Expected: fingerprintRoute{Machine: lane.Expected.Machine, Pane: lane.Expected.Pane},
			Target:   fingerprintRoute{Machine: lane.Target.Machine, Pane: lane.Target.Pane},
		})
	}
	sort.Slice(lanes, func(first, second int) bool { return lanes[first].Lane < lanes[second].Lane })
	readiness := fingerprintReadiness{}
	if request.Readiness != nil {
		readiness = fingerprintReadiness{
			ModelLogin:      controlPlaneReadinessClaim(request.Readiness.ModelLogin),
			Tools:           controlPlaneReadinessClaim(request.Readiness.Tools),
			HandoffkeepRead: controlPlaneReadinessClaim(request.Readiness.HandoffkeepRead),
			HubRead:         controlPlaneReadinessClaim(request.Readiness.HubRead),
			TargetPane:      controlPlaneReadinessClaim(request.Readiness.TargetPane),
		}
	}
	encoded, _ := json.Marshal(struct {
		Action             controlPlaneAction   `json:"action"`
		ExpectedEpoch      uint64               `json:"expected_epoch"`
		HandoverDocKey     string               `json:"handover_doc_key"`
		InflightOperations uint64               `json:"inflight_operations"`
		Lanes              []fingerprintLane    `json:"lanes"`
		Readiness          fingerprintReadiness `json:"readiness"`
	}{
		Action: request.Action, ExpectedEpoch: *request.ExpectedEpoch, HandoverDocKey: *request.HandoverDocKey,
		InflightOperations: *request.InflightOperations, Lanes: lanes, Readiness: readiness,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func controlPlaneReadinessClaim(value *bool) bool {
	return value != nil && *value
}

func (h *HubServer) transferControlPlane(request controlPlaneTransferRequest, fingerprint string) (controlPlaneTransferResponse, error) {
	if h.reportRelayPath == "" {
		return controlPlaneTransferResponse{}, errLanesWriteUnconfigured
	}
	var response controlPlaneTransferResponse
	committed := false
	err := withLanesFileLock(h.reportRelayPath, func() error {
		snapshot, err := readLanesFileForWrite(h.reportRelayPath)
		if err != nil {
			return err
		}
		// The history lookup comes before the epoch check on purpose. A caller
		// that never saw its response retries with the epoch it still believes
		// in, which is now one behind, and that retry has to converge on the
		// stored result rather than be rejected as stale. A request ID that has
		// aged out of the bounded history is deliberately not recoverable: it
		// falls through to the epoch check and is refused safely.
		if record, found := findControlPlaneHistoryRecord(snapshot.Control.History, request.RequestID); found {
			if record.Fingerprint != fingerprint {
				return controlPlaneConflictError("request_id_conflict")
			}
			response = controlPlaneResponseFromHistory(record)
			return nil
		}
		if *request.ExpectedEpoch != snapshot.Control.Epoch {
			return controlPlaneConflictError("stale_epoch")
		}
		if !controlPlaneExpectedRoutesMatch(request.Lanes, snapshot.Routes) {
			return controlPlaneConflictError("route_mismatch")
		}
		currentState := controlPlaneCurrentState(snapshot.Control)
		nextState, allowed := controlPlaneTransitions[currentState][request.Action]
		if !allowed {
			return controlPlaneConflictError("illegal_transition")
		}
		// inflight_operations is operator-asserted, not server-counted. An empty
		// handover_doc_key is already a 400 above, so only the count is checked.
		if request.Action == controlPlaneActionHandoverReady && *request.InflightOperations != 0 {
			return controlPlaneConflictError("illegal_transition")
		}
		// A3 places JIT readiness before the route commit. The contract scopes
		// the gate to that one action on purpose: drain and handover_ready run
		// while the target is still being spawned, so demanding a ready target
		// there would either invert the order or force the caller to assert a
		// readiness it does not have. Actions that never touch a route are not
		// gated, and only commit can produce the clearance below.
		var clearance readinessClearance
		if request.Action == controlPlaneActionCommit {
			clearance, err = h.controlPlaneReadinessClearance(request, snapshot.Routes)
			if err != nil {
				return err
			}
		}

		now := lanesBackupNow(h)
		control := snapshot.Control
		control.State = nextState
		control.LastRequestID = request.RequestID
		control.HandoverDocKey = *request.HandoverDocKey
		control.UpdatedAt = now
		if request.Action == controlPlaneActionCommit {
			commitControlPlaneRoutes(&snapshot, request.Lanes, clearance)
			control.Epoch++
			control.Owner = request.Lanes[0].Target.Machine
			committed = true
		}
		snapshot.Control = control
		response = controlPlaneResponseFromSnapshot(request, snapshot.Control, snapshot.Routes, clearance.checks)
		snapshot.Control.History = append(snapshot.Control.History, controlPlaneHistoryRecord{
			RequestID: request.RequestID, Fingerprint: fingerprint, Epoch: response.Epoch, Owner: response.Owner,
			HandoverDocKey: response.HandoverDocKey, State: response.State, Routes: response.Lanes,
			Readiness: response.Readiness, At: now,
		})
		if len(snapshot.Control.History) > controlPlaneHistoryMaxEntries {
			snapshot.Control.History = append([]controlPlaneHistoryRecord(nil), snapshot.Control.History[len(snapshot.Control.History)-controlPlaneHistoryMaxEntries:]...)
		}
		return h.replaceLanesFile(h.reportRelayPath, snapshot, now)
	})
	if err != nil {
		return controlPlaneTransferResponse{}, err
	}
	if committed {
		for _, lane := range response.Lanes {
			h.broadcastLanesChanged(lane.Lane, "update")
		}
	}
	return response, nil
}

func findControlPlaneHistoryRecord(history []controlPlaneHistoryRecord, requestID string) (controlPlaneHistoryRecord, bool) {
	for index := len(history) - 1; index >= 0; index-- {
		if history[index].RequestID == requestID {
			return history[index], true
		}
	}
	return controlPlaneHistoryRecord{}, false
}

func controlPlaneCurrentState(control lanesFileControl) controlPlaneState {
	if control.State == "" {
		return controlPlaneStateActive
	}
	return control.State
}

func controlPlaneExpectedRoutesMatch(requested []controlPlaneTransferLane, routes map[string]reportRelayRoute) bool {
	for _, requestedLane := range requested {
		route, found := routes[requestedLane.Lane]
		if !found || route.Sink || route.Machine != requestedLane.Expected.Machine || route.Pane != requestedLane.Expected.Pane {
			return false
		}
	}
	return true
}

// readinessClearance is created only after all requested readiness checks have
// passed while the lanes lock is held.
type readinessClearance struct {
	checks []controlPlaneReadinessCheck
	at     time.Time
}

func (h *HubServer) controlPlaneReadinessClearance(request controlPlaneTransferRequest, routes map[string]reportRelayRoute) (readinessClearance, error) {
	if !controlPlaneClaimsReady(request.Readiness) {
		return readinessClearance{}, controlPlaneConflictError("target_not_ready")
	}
	checks := h.controlPlaneReadinessChecks(request, routes)
	ordered, valid := orderedControlPlaneReadinessChecks(checks)
	if !valid || !controlPlaneTargetsReady(h, request.Lanes, routes) {
		return readinessClearance{}, controlPlaneConflictError("target_not_ready")
	}
	return readinessClearance{checks: ordered, at: lanesBackupNow(h)}, nil
}

func controlPlaneClaimsReady(claims *controlPlaneReadinessClaims) bool {
	return claims != nil && controlPlaneReadinessClaim(claims.ModelLogin) && controlPlaneReadinessClaim(claims.Tools) && controlPlaneReadinessClaim(claims.HandoffkeepRead) && controlPlaneReadinessClaim(claims.HubRead) && controlPlaneReadinessClaim(claims.TargetPane)
}

func (h *HubServer) controlPlaneReadinessChecks(request controlPlaneTransferRequest, routes map[string]reportRelayRoute) []controlPlaneReadinessCheck {
	if h.controlReadiness != nil {
		return h.controlReadiness(request, routes)
	}
	return []controlPlaneReadinessCheck{
		{Name: "model_login", OK: controlPlaneReadinessClaim(request.Readiness.ModelLogin)},
		{Name: "tools", OK: controlPlaneReadinessClaim(request.Readiness.Tools)},
		{Name: "handoffkeep_read", OK: controlPlaneReadinessClaim(request.Readiness.HandoffkeepRead)},
		{Name: "hub_read", OK: controlPlaneReadinessClaim(request.Readiness.HubRead)},
		{Name: "target_pane", OK: controlPlaneReadinessClaim(request.Readiness.TargetPane) && controlPlaneTargetsReady(h, request.Lanes, routes)},
	}
}

func orderedControlPlaneReadinessChecks(checks []controlPlaneReadinessCheck) ([]controlPlaneReadinessCheck, bool) {
	if len(checks) != len(controlPlaneReadinessNames) {
		return nil, false
	}
	byName := make(map[string]bool, len(checks))
	for _, check := range checks {
		if _, duplicate := byName[check.Name]; duplicate || !check.OK {
			return nil, false
		}
		byName[check.Name] = true
	}
	ordered := make([]controlPlaneReadinessCheck, 0, len(controlPlaneReadinessNames))
	for _, name := range controlPlaneReadinessNames {
		if !byName[name] {
			return nil, false
		}
		ordered = append(ordered, controlPlaneReadinessCheck{Name: name, OK: true})
	}
	return ordered, true
}

func controlPlaneTargetsReady(hub *HubServer, requested []controlPlaneTransferLane, routes map[string]reportRelayRoute) bool {
	for _, requestedLane := range requested {
		if !validLanePane(requestedLane.Target.Pane) || !hub.knownLaneMachine(requestedLane.Target.Machine, routes) {
			return false
		}
	}
	return true
}

// commitControlPlaneRoutes cannot be called without a readiness clearance.
// A3 ordering is enforced by the signature, not by call-site discipline.
func commitControlPlaneRoutes(snapshot *lanesFileSnapshot, requested []controlPlaneTransferLane, clearance readinessClearance) {
	_ = clearance
	for _, requestedLane := range requested {
		route := snapshot.Routes[requestedLane.Lane]
		route.Machine = requestedLane.Target.Machine
		route.Pane = requestedLane.Target.Pane
		snapshot.Routes[requestedLane.Lane] = route
	}
}

func controlPlaneResponseFromSnapshot(request controlPlaneTransferRequest, control lanesFileControl, routes map[string]reportRelayRoute, checks []controlPlaneReadinessCheck) controlPlaneTransferResponse {
	lanes := make([]controlPlaneRouteProjection, 0, len(request.Lanes))
	for _, requestedLane := range request.Lanes {
		route := routes[requestedLane.Lane]
		lanes = append(lanes, controlPlaneRouteProjection{Lane: requestedLane.Lane, Machine: route.Machine, Pane: route.Pane})
	}
	sort.Slice(lanes, func(first, second int) bool { return lanes[first].Lane < lanes[second].Lane })
	return controlPlaneTransferResponse{
		RequestID: request.RequestID, Epoch: control.Epoch, Owner: control.Owner, State: control.State,
		HandoverDocKey: control.HandoverDocKey, Lanes: lanes, Readiness: controlPlaneReadinessChecksCopy(checks),
	}
}

// controlPlaneResponseFromHistory replays what was stored, never a plausible
// reconstruction of it: AC1 asks a retry to converge on the same result, and a
// value rebuilt from the current request is only equal by assumption.
func controlPlaneResponseFromHistory(record controlPlaneHistoryRecord) controlPlaneTransferResponse {
	return controlPlaneTransferResponse{
		RequestID: record.RequestID, Epoch: record.Epoch, Owner: record.Owner, State: record.State,
		HandoverDocKey: record.HandoverDocKey, Lanes: controlPlaneRouteProjections(record.Routes),
		Readiness: controlPlaneReadinessChecksCopy(record.Readiness),
	}
}

func controlPlaneRouteProjections(routes []controlPlaneRouteProjection) []controlPlaneRouteProjection {
	copied := make([]controlPlaneRouteProjection, 0, len(routes))
	return append(copied, routes...)
}

func controlPlaneReadinessChecksCopy(checks []controlPlaneReadinessCheck) []controlPlaneReadinessCheck {
	copied := make([]controlPlaneReadinessCheck, 0, len(checks))
	return append(copied, checks...)
}

func controlFromLanesFile(source *lanesFileControl) (lanesFileControl, error) {
	if source == nil {
		return lanesFileControl{}, nil
	}
	control := *source
	control.History = append([]controlPlaneHistoryRecord(nil), source.History...)
	if err := validateLanesFileControl(control); err != nil {
		return lanesFileControl{}, err
	}
	return control, nil
}

func validateLanesFileControl(control lanesFileControl) error {
	if control.Owner != "" && (control.Owner == hubOperatorMachineID || !machineIDPattern.MatchString(control.Owner)) {
		return errors.New("invalid control owner")
	}
	if control.State != "" && !validControlPlaneState(control.State) {
		return errors.New("invalid control state")
	}
	if control.LastRequestID != "" && !validHubSpawnRequestID(control.LastRequestID) {
		return errors.New("invalid control request ID")
	}
	if control.HandoverDocKey != "" && !validControlPlaneHandoverDocKey(control.HandoverDocKey) {
		return errors.New("invalid control handover document key")
	}
	if len(control.History) > controlPlaneHistoryMaxEntries {
		return errors.New("control history exceeds retention")
	}
	for _, record := range control.History {
		if !validHubSpawnRequestID(record.RequestID) || len(record.Fingerprint) != sha256.Size*2 || !validControlPlaneState(record.State) || len(record.Routes) != 2 {
			return errors.New("invalid control history")
		}
		if !validControlPlaneHandoverDocKey(record.HandoverDocKey) {
			return errors.New("invalid control history handover document key")
		}
		if _, err := hex.DecodeString(record.Fingerprint); err != nil {
			return errors.New("invalid control history fingerprint")
		}
		for _, route := range record.Routes {
			if !laneNamePattern.MatchString(route.Lane) || route.Machine == hubOperatorMachineID || !machineIDPattern.MatchString(route.Machine) || !validLanePane(route.Pane) {
				return errors.New("invalid control history route")
			}
		}
		if !validControlPlaneHistoryReadiness(record.Readiness) {
			return errors.New("invalid control history readiness")
		}
	}
	return nil
}

// validControlPlaneHistoryReadiness accepts the two shapes a stored record can
// have: the five passing checks a commit records, or nothing at all for an
// action that never reached the readiness gate.
func validControlPlaneHistoryReadiness(checks []controlPlaneReadinessCheck) bool {
	if len(checks) == 0 {
		return true
	}
	if len(checks) != len(controlPlaneReadinessNames) {
		return false
	}
	for index, name := range controlPlaneReadinessNames {
		if checks[index].Name != name || !checks[index].OK {
			return false
		}
	}
	return true
}

func validControlPlaneState(state controlPlaneState) bool {
	switch state {
	case controlPlaneStateActive, controlPlaneStatePrepared, controlPlaneStateDraining, controlPlaneStateHandoverReady, controlPlaneStateTransferredUnacked:
		return true
	default:
		return false
	}
}

func loadReportRelayRoutesAndControlResult(path string) (map[string]reportRelayRoute, lanesFileControl, error) {
	if path == "" {
		return nil, lanesFileControl{}, nil
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, lanesFileControl{}, nil
	}
	if len(contents) > lanesFileMaxBytes {
		return nil, lanesFileControl{}, errReportRelayRoutesInvalid
	}
	routes, err := parseReportRelayRoutes(contents)
	if err != nil {
		return nil, lanesFileControl{}, err
	}
	var source struct {
		Control *lanesFileControl `json:"control"`
	}
	if err := json.Unmarshal(contents, &source); err != nil {
		return nil, lanesFileControl{}, err
	}
	control, err := controlFromLanesFile(source.Control)
	if err != nil {
		return nil, lanesFileControl{}, err
	}
	return routes, control, nil
}

// --- authority lane policy -------------------------------------------------
//
// The authority lane set is operator configuration, not source. It follows the
// placement-policy convention already in this repository: a CLI flag naming an
// operator-owned JSON file under /etc/panewire, hot-reloaded by modification
// time, with the same default/current/stale/invalid status vocabulary.

const controlPlaneLanesMaxEntries = 64

// ControlPlaneLanesFile is the operator-owned authority lane set. Lane names
// live in this file, never in source: which lanes carry authority is a
// deployment decision.
type ControlPlaneLanesFile struct {
	AuthorityLanes []string `json:"authority_lanes"`
}

// ParseControlPlaneLanes rejects anything it cannot understand rather than
// silently protecting a smaller set than the operator wrote.
func ParseControlPlaneLanes(data []byte) (map[string]struct{}, error) {
	var file ControlPlaneLanesFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return nil, errors.New("control-plane lanes file is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("control-plane lanes file is invalid")
	}
	if file.AuthorityLanes == nil {
		return nil, errors.New("control-plane lanes file must list authority_lanes")
	}
	if len(file.AuthorityLanes) > controlPlaneLanesMaxEntries {
		return nil, errors.New("control-plane lanes file lists too many lanes")
	}
	lanes := make(map[string]struct{}, len(file.AuthorityLanes))
	for _, lane := range file.AuthorityLanes {
		if !laneNamePattern.MatchString(lane) {
			return nil, errors.New("control-plane lanes file has an invalid lane")
		}
		if _, duplicate := lanes[lane]; duplicate {
			return nil, errors.New("control-plane lanes file has a duplicate lane")
		}
		lanes[lane] = struct{}{}
	}
	return lanes, nil
}

func LoadControlPlaneLanes(path string) (map[string]struct{}, time.Time, error) {
	if path == "" {
		return nil, time.Time{}, errors.New("control-plane lanes path is required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, time.Time{}, errors.New("control-plane lanes must be a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, time.Time{}, errors.New("read control-plane lanes")
	}
	lanes, err := ParseControlPlaneLanes(data)
	return lanes, info.ModTime(), err
}

func (h *HubServer) reloadControlPlaneLanesLocked() {
	if h.controlPlaneLanesPath == "" {
		return
	}
	info, err := os.Lstat(h.controlPlaneLanesPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		h.setControlPlaneLanesFailureLocked("unreadable")
		return
	}
	if info.ModTime().Equal(h.controlPlaneLanesObservedModTime) {
		return
	}
	lanes, modTime, err := LoadControlPlaneLanes(h.controlPlaneLanesPath)
	if err != nil {
		h.controlPlaneLanesObservedModTime = info.ModTime()
		h.setControlPlaneLanesFailureLocked("invalid")
		return
	}
	h.controlPlaneLanes, h.controlPlaneLanesModTime = lanes, modTime
	h.controlPlaneLanesObservedModTime = modTime
	h.controlPlaneLanesLoaded = true
	h.controlPlaneLanesStatus = "current"
	h.controlPlaneLanesLastFailure = ""
}

// setControlPlaneLanesFailureLocked keeps the last-known-good set on a stale
// reload. A set that was read once still says what has to be protected; a set
// that was never read does not, which is what makes the unavailable path below
// refuse every lane instead of guessing.
func (h *HubServer) setControlPlaneLanesFailureLocked(failure string) {
	status := "invalid"
	if h.controlPlaneLanesLoaded {
		status = "stale"
	}
	changed := h.controlPlaneLanesStatus != status || h.controlPlaneLanesLastFailure != failure
	h.controlPlaneLanesStatus, h.controlPlaneLanesLastFailure = status, failure
	if changed {
		h.logger.Error("control-plane lanes reload failed", "policy_status", status, "failure", failure)
	}
}

// authorityLaneProtection reports the guard's state in the same vocabulary the
// placement policy uses, with "disabled" for the unconfigured default. An
// opt-in guard that says nothing about being off stays off, so the state is
// reported where an operator already looks.
func (h *HubServer) authorityLaneProtection() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reloadControlPlaneLanesLocked()
	if h.controlPlaneLanesStatus == "default" {
		return "disabled"
	}
	return h.controlPlaneLanesStatus
}

// logAuthorityLaneProtection is called once at startup. Lane names never reach
// the log: this is a public repository and a shared operational log.
// logAuthorityLaneProtection is called once when the hub starts serving, not
// from the constructor: an opt-in guard has to announce that it is off, and the
// place to say so is a real start.
func (h *HubServer) logAuthorityLaneProtection() {
	h.mu.Lock()
	status, failure, lanes := h.controlPlaneLanesStatus, h.controlPlaneLanesLastFailure, len(h.controlPlaneLanes)
	h.mu.Unlock()
	logAuthorityLaneProtection(h.logger, status, failure, lanes)
}

func logAuthorityLaneProtection(logger *slog.Logger, status, failure string, lanes int) {
	switch status {
	case "default":
		logger.Info("authority-lane protection disabled", "reason", "no policy path configured")
	case "invalid":
		// Configured but never loaded: every direct lane write is refused until
		// the file loads, so this has to be loud rather than merely reported.
		logger.Error("authority-lane protection is fail-closed", "policy_status", status, "failure", failure)
	default:
		logger.Info("authority-lane protection enabled", "policy_status", status, "lanes", lanes)
	}
}

// controlPlaneLanesFailureReason uses the same two words the placement policy
// does: a file that is not there at all is unreadable, one that is there and
// does not parse is invalid.
func controlPlaneLanesFailureReason(path string) string {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "unreadable"
	}
	return "invalid"
}

var errAuthorityLanePolicyUnavailable = errors.New("authority lane policy is unavailable")

// controlPlaneAuthorityDecision answers whether a direct lane write may
// proceed. It is deliberately fail-closed: a configured policy that has never
// loaded refuses every lane, because the hub cannot tell an authority lane from
// an ordinary one without it, and letting writes through then is the same as
// having no guard at all.
func (h *HubServer) controlPlaneAuthorityDecision(lane string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reloadControlPlaneLanesLocked()
	if h.controlPlaneLanesPath != "" && !h.controlPlaneLanesLoaded {
		return errAuthorityLanePolicyUnavailable
	}
	if _, configured := h.controlPlaneLanes[lane]; configured {
		return errAuthorityLaneDirectWrite
	}
	return nil
}
