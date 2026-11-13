package panewire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"time"
	"unicode"
	"unicode/utf8"
)

// The authority-lane guard. Lanes named in the operator's --control-plane-lanes
// file cannot be written through the lanes API: a direct PUT or DELETE on one
// of them is refused, and the lanes file on the hub host is the only way to
// change them. This file also holds the lanes file's control block, which the
// lanes write path preserves and validates, and the lane self-check witness.

const (
	controlPlaneHistoryMaxEntries = 8
)

type controlPlaneState string

const (
	controlPlaneStateActive controlPlaneState = "ACTIVE"

	controlPlaneStatePrepared controlPlaneState = "PREPARED"

	controlPlaneStateDraining controlPlaneState = "DRAINING"

	controlPlaneStateHandoverReady controlPlaneState = "HANDOVER_READY"

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

func controlFromLanesFile(source json.RawMessage) (lanesFileControl, error) {
	if len(source) == 0 || bytes.Equal(bytes.TrimSpace(source), []byte("null")) {
		return lanesFileControl{}, nil
	}
	var control lanesFileControl
	if err := json.Unmarshal(source, &control); err != nil {
		return lanesFileControl{}, errors.New("control block is not an object")
	}
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
			if !LaneNamePattern.MatchString(route.Lane) || route.Machine == hubOperatorMachineID || !machineIDPattern.MatchString(route.Machine) || !validLanePane(route.Pane) {
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

// loadReportRelayRoutesAndControlResult reads the lanes projection for GET.
// A control block it cannot make sense of is reported as unreadable rather
// than failing the read: the guard exists to stop unauthorized writes, not
// reads, and a hand-edited file that the hub refuses to show is a file the
// operator cannot diagnose. The self-check witness reads this same projection,
// so failing here would take the observation hook down with the thing it
// observes. The write path stays strict — nothing overwrites a control block
// it did not understand.
func loadReportRelayRoutesAndControlResult(path string) (map[string]reportRelayRoute, lanesFileControl, bool, error) {
	if path == "" {
		return nil, lanesFileControl{}, true, nil
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, lanesFileControl{}, true, nil
	}
	if len(contents) > lanesFileMaxBytes {
		return nil, lanesFileControl{}, false, errReportRelayRoutesInvalid
	}
	routes, err := parseReportRelayRoutes(contents)
	if err != nil {
		return nil, lanesFileControl{}, false, err
	}
	var source struct {
		Control json.RawMessage `json:"control"`
	}
	if err := json.Unmarshal(contents, &source); err != nil {
		return routes, lanesFileControl{}, false, nil
	}
	control, err := controlFromLanesFile(source.Control)
	if err != nil {
		// Safe defaults, never the damaged values: epoch 0 and empty strings
		// read as "no control state", which is what an unreadable block means.
		return routes, lanesFileControl{}, false, nil
	}
	return routes, control, true, nil
}

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
		if !LaneNamePattern.MatchString(lane) {
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
func (h *HubServer) authorityLaneProtection(controlReadable bool) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reloadControlPlaneLanesLocked()
	if !controlReadable {
		// The control block the guard depends on does not parse. That is the
		// same "configured but not understood" condition the policy file has,
		// and it reads the same way to an operator.
		//
		// This deliberately outranks "disabled", including on a hub with no
		// policy configured. The field is also what self-check trusts: if an
		// unreadable control block reported "disabled" here, the witness would
		// confirm ownership from the epoch 0 the hub substituted for values it
		// could not validate — and it would do so on the default configuration,
		// which is most of them.
		return "invalid"
	}
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
