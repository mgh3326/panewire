package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/mgh3326/panewire/internal/configkeys"
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

func loadAcceptingOverrides(path string) (map[string]acceptingOverride, []string, error) {
	overrides := make(map[string]acceptingOverride)
	if path == "" {
		return overrides, nil, nil
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return overrides, nil, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, nil, errors.New("invalid override file")
	}
	b, err := os.ReadFile(path)
	if err != nil || len(b) > 64<<10 {
		return nil, nil, errors.New("invalid override file")
	}
	var wire acceptingOverrideFile
	if json.Unmarshal(b, &wire) != nil {
		return nil, nil, errors.New("invalid override file")
	}
	ignored, err := configkeys.JSON(b, &wire)
	if err != nil {
		return nil, nil, errors.New("invalid override file")
	}
	ignored = append(ignored, unknownOverrideEntryKeys(b)...)
	slices.Sort(ignored)
	for machine, override := range wire.Overrides {
		if details, ok := wire.Containments[machine]; ok {
			override.Containment, override.ContainmentVerified = details.Containment, details.ContainmentVerified
		}
		if override.Mode != "off" {
			override.Containment, override.ContainmentVerified = "", false
		}
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || !validAcceptingOverride(override.Mode) || (override.Containment != "" && !containmentPattern.MatchString(override.Containment)) {
			return nil, nil, errors.New("invalid override file")
		}
		overrides[machine] = override
	}
	return overrides, ignored, nil
}

// acceptingOverrideShape is acceptingOverride without its legacy-string
// unmarshaler, so configkeys can walk the keys of each override entry.
type acceptingOverrideShape acceptingOverride

// unknownOverrideEntryKeys lists the keys inside each override entry that
// acceptingOverride does not read, as overrides.<machine>.<key>.
func unknownOverrideEntryKeys(b []byte) []string {
	var doc struct {
		Overrides map[string]json.RawMessage `json:"overrides"`
	}
	if json.Unmarshal(b, &doc) != nil {
		return nil
	}
	var out []string
	for machine, raw := range doc.Overrides {
		var entry any
		if json.Unmarshal(raw, &entry) != nil {
			continue
		}
		for _, key := range configkeys.Unknown(entry, acceptingOverrideShape{}, "json") {
			out = append(out, "overrides."+machine+"."+key)
		}
	}
	return out
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

// persistAcceptingOverridesLocked rewrites the override file from next. Keys
// the loader ignores are carried over, so a rewrite does not erase data a
// newer release wrote. An entry with such keys is written as an object; the
// others keep the legacy string form that pre-change hubs decode.
func (h *HubServer) persistAcceptingOverridesLocked(next map[string]acceptingOverride) error {
	path := h.relayControl.acceptingOverridesPath
	if path == "" {
		return nil
	}
	rootExtras, entryExtras := acceptingOverrideExtras(path)
	overrides := make(map[string]any, len(next))
	details := make(map[string]acceptingContainment)
	for machine, override := range next {
		if extras := entryExtras[machine]; len(extras) > 0 {
			fields := make(map[string]any, len(extras)+1)
			for key, value := range extras {
				fields[key] = value
			}
			fields["mode"] = override.Mode
			overrides[machine] = fields
		} else {
			overrides[machine] = override.Mode
		}
		if override.Mode == "off" && override.Containment != "" {
			details[machine] = acceptingContainment{override.Containment, override.ContainmentVerified}
		}
	}
	doc := make(map[string]any, len(rootExtras)+2)
	for key, value := range rootExtras {
		doc[key] = value
	}
	doc["overrides"] = overrides
	if len(details) > 0 {
		doc["containments"] = details
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return replaceFileAtomically(path, ".accepting-overrides-*", append(b, '\n'), 0600)
}

// acceptingOverrideReadKeys are the entry keys the loader reads. Matching is
// case-insensitive, as encoding/json and configkeys both match.
var acceptingOverrideReadKeys = []string{"mode", "containment", "containment_verified"}

// acceptingOverrideExtras returns the keys in the override file that the
// loader does not read: root-level keys, and per machine the entry keys. A
// file that cannot be read or decoded yields none.
func acceptingOverrideExtras(path string) (map[string]json.RawMessage, map[string]map[string]json.RawMessage) {
	root := make(map[string]json.RawMessage)
	entries := make(map[string]map[string]json.RawMessage)
	b, err := os.ReadFile(path)
	if err != nil {
		return root, entries
	}
	var doc map[string]json.RawMessage
	if json.Unmarshal(b, &doc) != nil {
		return root, entries
	}
	for key, value := range doc {
		if !keyNamedIn(key, []string{"overrides", "containments"}) {
			root[key] = value
		}
	}
	var overrides map[string]json.RawMessage
	if json.Unmarshal(doc["overrides"], &overrides) != nil {
		return root, entries
	}
	for machine, value := range overrides {
		var fields map[string]json.RawMessage
		if json.Unmarshal(value, &fields) != nil {
			continue // legacy string form: no entry keys
		}
		for key, field := range fields {
			if keyNamedIn(key, acceptingOverrideReadKeys) {
				continue
			}
			if entries[machine] == nil {
				entries[machine] = make(map[string]json.RawMessage)
			}
			entries[machine][key] = field
		}
	}
	return root, entries
}

func keyNamedIn(key string, names []string) bool {
	return slices.ContainsFunc(names, func(name string) bool { return strings.EqualFold(name, key) })
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
