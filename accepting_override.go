package panewire

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
)

const acceptingOverrideAuto = "auto"

type acceptingOverrideFile struct {
	Overrides map[string]string `json:"overrides"`
}

func validAcceptingOverride(mode string) bool {
	return mode == "on" || mode == "off" || mode == acceptingOverrideAuto
}

func loadAcceptingOverrides(path string) (map[string]string, error) {
	overrides := make(map[string]string)
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
	for machine, mode := range wire.Overrides {
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || !validAcceptingOverride(mode) {
			return nil, errors.New("invalid override file")
		}
		overrides[machine] = mode
	}
	return overrides, nil
}

func (h *HubServer) acceptingOverrideLocked(machine string) string {
	if mode := h.acceptingOverride[machine]; mode != "" {
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

func (h *HubServer) persistAcceptingOverridesLocked(next map[string]string) error {
	if h.acceptingOverridesPath == "" {
		return nil
	}
	b, err := json.Marshal(acceptingOverrideFile{Overrides: next})
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(h.acceptingOverridesPath), ".accepting-overrides-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(append(b, '\n')); err != nil || temporary.Close() != nil {
		return errors.New("write override file")
	}
	return os.Rename(name, h.acceptingOverridesPath)
}

func (h *HubServer) handleAcceptingOverride(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	machine := request.PathValue("machine")
	var body struct {
		Mode string `json:"mode"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1024))
	decoder.DisallowUnknownFields()
	if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || decoder.Decode(&body) != nil || !validAcceptingOverride(body.Mode) {
		http.Error(writer, "invalid accepting override", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	if _, known := h.tokens[machine]; !known {
		h.mu.Unlock()
		http.Error(writer, "unknown node", http.StatusNotFound)
		return
	}
	next := make(map[string]string, len(h.acceptingOverride)+1)
	for key, value := range h.acceptingOverride {
		next[key] = value
	}
	next[machine] = body.Mode
	if err := h.persistAcceptingOverridesLocked(next); err != nil {
		h.mu.Unlock()
		http.Error(writer, "accepting override unavailable", http.StatusInternalServerError)
		return
	}
	h.acceptingOverride = next
	effective := false
	if record := h.nodes[machine]; record != nil {
		effective = h.acceptingEffectiveLocked(machine, record.accepting)
	}
	h.placementCache = placementCache{}
	h.mu.Unlock()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(struct {
		Machine   string `json:"machine"`
		Mode      string `json:"mode"`
		Effective bool   `json:"accepting_effective"`
	}{machine, body.Mode, effective})
}
