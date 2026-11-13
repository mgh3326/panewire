package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"regexp"
	"strconv"
)

const acceptingOverrideAuto = "auto"

type acceptingOverride struct {
	Mode                string `json:"mode"`
	Containment         string `json:"containment,omitempty"`
	ContainmentVerified bool   `json:"containment_verified"`
}

func (o *acceptingOverride) UnmarshalJSON(b []byte) error {
	var legacy string
	if json.Unmarshal(b, &legacy) == nil {
		o.Mode = legacy
		return nil
	}
	type plain acceptingOverride
	return json.Unmarshal(b, (*plain)(o))
}

type acceptingOverrideFile struct {
	Overrides    map[string]acceptingOverride    `json:"overrides"`
	Containments map[string]acceptingContainment `json:"containments,omitempty"`
}

type acceptingContainment struct {
	Containment         string `json:"containment"`
	ContainmentVerified bool   `json:"containment_verified"`
}

var containmentPattern = regexp.MustCompile(`^hk:[1-9][0-9]{0,8}$`)

func (h *HubServer) verifyContainment(ctx context.Context, containment string) (bool, int) {
	if h.handoffkeep == nil {
		return false, 0
	}
	id, _ := strconv.ParseInt(containment[3:], 10, 64)
	status, payload, err := h.handoffkeep.do(ctx, http.MethodGet, h.handoffkeep.endpoint("/v1/tasks/"+strconv.FormatInt(id, 10)), nil)
	if err != nil {
		return false, 0
	}
	if status == http.StatusNotFound {
		return false, http.StatusNotFound
	}
	var task acceptingTask
	if status != http.StatusOK || json.Unmarshal(payload, &task) != nil || task.ID != id || task.State == "" {
		return false, 0
	}
	switch task.State {
	case "merged", "deployed", "dropped", "duplicate":
		return false, http.StatusConflict
	}
	return true, 0
}

// acceptingTask is the part of an hk task the accepting override reads: its id
// and its state.
type acceptingTask struct {
	ID    int64  `json:"id"`
	State string `json:"state"`
}

func validAcceptingOverride(mode string) bool {
	return mode == "on" || mode == "off" || mode == acceptingOverrideAuto
}

func loadAcceptingOverrides(path string) (map[string]acceptingOverride, error) {
	overrides := make(map[string]acceptingOverride)
	if path == "" {
		return overrides, nil
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return overrides, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid override file")
	}
	b, err := os.ReadFile(path)
	if err != nil || len(b) > 64<<10 {
		return nil, errors.New("invalid override file")
	}
	var wire acceptingOverrideFile
	if json.Unmarshal(b, &wire) != nil {
		return nil, errors.New("invalid override file")
	}
	for machine, override := range wire.Overrides {
		if details, ok := wire.Containments[machine]; ok {
			override.Containment, override.ContainmentVerified = details.Containment, details.ContainmentVerified
		}
		if override.Mode != "off" {
			override.Containment, override.ContainmentVerified = "", false
		}
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || !validAcceptingOverride(override.Mode) || (override.Containment != "" && !containmentPattern.MatchString(override.Containment)) {
			return nil, errors.New("invalid override file")
		}
		overrides[machine] = override
	}
	return overrides, nil
}

func (h *HubServer) acceptingOverrideLocked(machine string) string {
	if mode := h.relayControl.acceptingOverride[machine].Mode; mode != "" {
		return mode
	}
	return acceptingOverrideAuto
}

func (h *HubServer) acceptingEffectiveLocked(machine string, advertised bool) bool {
	switch h.acceptingOverrideLocked(machine) {
	case "on":
		return true
	case "off":
		return false
	default:
		return advertised
	}
}

func (h *HubServer) persistAcceptingOverridesLocked(next map[string]acceptingOverride) error {
	if h.relayControl.acceptingOverridesPath == "" {
		return nil
	}
	modes := make(map[string]string, len(next))
	details := make(map[string]acceptingContainment)
	for machine, override := range next {
		modes[machine] = override.Mode
		if override.Mode == "off" && override.Containment != "" {
			details[machine] = acceptingContainment{override.Containment, override.ContainmentVerified}
		}
	}
	b, err := json.Marshal(struct {
		Overrides    map[string]string               `json:"overrides"`
		Containments map[string]acceptingContainment `json:"containments,omitempty"`
	}{modes, details})
	if err != nil {
		return err
	}
	return replaceFileAtomically(h.relayControl.acceptingOverridesPath, ".accepting-overrides-*", append(b, '\n'), 0600)
}

func (h *HubServer) handleAcceptingOverride(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	machine := request.PathValue("machine")
	var body struct {
		Mode        string `json:"mode"`
		Containment string `json:"containment"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1024))
	decoder.DisallowUnknownFields()
	if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || decoder.Decode(&body) != nil || !validAcceptingOverride(body.Mode) {
		http.Error(writer, "invalid accepting override", http.StatusBadRequest)
		return
	}

	if body.Mode == "off" && !containmentPattern.MatchString(body.Containment) {
		http.Error(writer, "containment_required", http.StatusBadRequest)
		return
	}
	verified := false
	if body.Mode == "off" {
		var status int
		verified, status = h.verifyContainment(request.Context(), body.Containment)
		if status == http.StatusConflict {
			http.Error(writer, "containment_closed", status)
			return
		}
		if status == http.StatusNotFound {
			http.Error(writer, "containment_not_found", http.StatusBadRequest)
			return
		}
	} else {
		body.Containment = ""
	}
	h.mu.Lock()
	if _, known := h.auth.Load().tokens[machine]; !known {
		h.mu.Unlock()
		http.Error(writer, "unknown node", http.StatusNotFound)
		return
	}
	next := make(map[string]acceptingOverride, len(h.relayControl.acceptingOverride)+1)
	for key, value := range h.relayControl.acceptingOverride {
		next[key] = value
	}
	next[machine] = acceptingOverride{Mode: body.Mode, Containment: body.Containment, ContainmentVerified: verified}
	if err := h.persistAcceptingOverridesLocked(next); err != nil {
		h.mu.Unlock()
		http.Error(writer, "accepting override unavailable", http.StatusInternalServerError)
		return
	}
	h.relayControl.acceptingOverride = next
	effective := false
	if record := h.nodes[machine]; record != nil {
		effective = h.acceptingEffectiveLocked(machine, record.accepting)
	}
	h.placementCache = placementCache{}
	h.mu.Unlock()
	writeJSON(writer, http.StatusOK, struct {
		Machine             string `json:"machine"`
		Mode                string `json:"mode"`
		Effective           bool   `json:"accepting_effective"`
		Containment         string `json:"containment,omitempty"`
		ContainmentVerified bool   `json:"containment_verified"`
	}{machine, body.Mode, effective, body.Containment, verified})
}
