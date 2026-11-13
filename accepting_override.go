package panewire

import (
	"bytes"
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
	if err != nil || len(b) > acceptingOverrideMaxBytes {
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

// acceptingOverrideMaxBytes is the largest override file the loader reads. A
// rewrite that would exceed it is refused, so a later restart can still load it.
const acceptingOverrideMaxBytes = 64 << 10

var errAcceptingOverrideTooLarge = errors.New("accepting override file would exceed the loader limit")

// persistAcceptingOverridesLocked rewrites the override file from next. Keys
// the loader ignores are carried over at the root, in each override entry, and
// in each containment entry, so a rewrite does not erase data a newer release
// wrote. An entry with such keys is written as an object; the others keep the
// legacy string form that pre-change hubs decode.
func (h *HubServer) persistAcceptingOverridesLocked(next map[string]acceptingOverride) error {
	path := h.relayControl.acceptingOverridesPath
	if path == "" {
		return nil
	}
	left := readAcceptingOverrideLeftovers(path)
	overrides := make(map[string]any, len(next))
	containments := make(map[string]any)
	for machine, override := range next {
		if extras := left.overrides[machine]; len(extras) > 0 {
			overrides[machine] = withLeftovers(extras, map[string]any{"mode": override.Mode})
		} else {
			overrides[machine] = override.Mode
		}
		extras := left.containments[machine]
		typed := override.Mode == "off" && override.Containment != ""
		switch {
		case typed && len(extras) == 0:
			containments[machine] = acceptingContainment{override.Containment, override.ContainmentVerified}
		case typed:
			containments[machine] = withLeftovers(extras, map[string]any{"containment": override.Containment, "containment_verified": override.ContainmentVerified})
		case len(extras) > 0:
			containments[machine] = withLeftovers(extras, nil)
		}
	}
	doc := make(map[string]any, len(left.root)+2)
	for key, value := range left.root {
		doc[key] = value
	}
	doc["overrides"] = overrides
	if len(containments) > 0 {
		doc["containments"] = containments
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	out := append(b, '\n')
	if len(out) > acceptingOverrideMaxBytes {
		return errAcceptingOverrideTooLarge
	}
	return replaceFileAtomically(path, ".accepting-overrides-*", out, 0600)
}

// acceptingOverrideLeftovers holds the keys of the override file that the
// loader does not read.
type acceptingOverrideLeftovers struct {
	root         map[string]json.RawMessage
	overrides    map[string]map[string]json.RawMessage
	containments map[string]map[string]json.RawMessage
}

var (
	acceptingOverrideReadKeys    = []string{"mode", "containment", "containment_verified"}
	acceptingContainmentReadKeys = []string{"containment", "containment_verified"}
)

// readAcceptingOverrideLeftovers collects the keys the loader does not read:
// root-level keys, and per machine the keys of each override and containment
// entry. Member names match case-insensitively, as the loader's decode does.
// Members are visited in source order, so when two case variants of one member
// both define a leftover key, the later one is kept, as the loader's decode
// keeps the later machine entry. A file that cannot be read or decoded yields
// none.
func readAcceptingOverrideLeftovers(path string) acceptingOverrideLeftovers {
	left := acceptingOverrideLeftovers{
		root:         make(map[string]json.RawMessage),
		overrides:    make(map[string]map[string]json.RawMessage),
		containments: make(map[string]map[string]json.RawMessage),
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return left
	}
	members, err := topLevelMembers(b)
	if err != nil {
		return left
	}
	for _, member := range members {
		switch {
		case strings.EqualFold(member.key, "overrides"):
			collectEntryLeftovers(member.value, acceptingOverrideReadKeys, left.overrides)
		case strings.EqualFold(member.key, "containments"):
			collectEntryLeftovers(member.value, acceptingContainmentReadKeys, left.containments)
		default:
			left.root[member.key] = member.value
		}
	}
	return left
}

// jsonMember is one member of a JSON object, with its raw value.
type jsonMember struct {
	key   string
	value json.RawMessage
}

// topLevelMembers returns the members of the JSON object in b in source order.
// A key repeated in the object appears once per occurrence.
func topLevelMembers(b []byte) ([]jsonMember, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, errors.New("not a JSON object")
	}
	var members []jsonMember
	for dec.More() {
		tok, err := dec.Token()
		key, ok := tok.(string)
		if err != nil || !ok {
			return nil, errors.New("invalid object key")
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		members = append(members, jsonMember{key, value})
	}
	return members, nil
}

// collectEntryLeftovers adds, per machine, the keys of each object in members
// that are not in read.
func collectEntryLeftovers(members json.RawMessage, read []string, into map[string]map[string]json.RawMessage) {
	var entries map[string]json.RawMessage
	if json.Unmarshal(members, &entries) != nil {
		return
	}
	for machine, raw := range entries {
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			continue // legacy string form: no leftover keys
		}
		for key, value := range fields {
			if keyNamedIn(key, read) {
				continue
			}
			if into[machine] == nil {
				into[machine] = make(map[string]json.RawMessage)
			}
			into[machine][key] = value
		}
	}
}

// withLeftovers returns the leftover keys merged with fields. Typed fields win,
// so a leftover cannot shadow a field this release writes.
func withLeftovers(leftovers map[string]json.RawMessage, fields map[string]any) map[string]any {
	out := make(map[string]any, len(leftovers)+len(fields))
	for key, value := range leftovers {
		out[key] = value
	}
	for key, value := range fields {
		out[key] = value
	}
	return out
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
