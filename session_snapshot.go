package panewire

import (
	"bytes"
	"encoding/json"
	"sort"
	"time"
)

const (
	hubSnapshotStatusOK          = "ok"
	hubSnapshotStatusUnavailable = "unavailable"
	// hubMaxHeartbeatSessions bounds both node work and hub allocations. The
	// byte-budget check below is still authoritative because labels and other
	// existing heartbeat fields have variable encoded sizes.
	hubMaxHeartbeatSessions = 64

	// hubSessionSnapshotTimeout bounds the once-per-heartbeat agent.list
	// lookup. The collection runs inline in the heartbeat write loop, so a
	// stalled herdr must surface as snapshot_status=unavailable instead of
	// holding the loop — and every ping/relay behind it — open. Mirrors
	// hubPanesAliveTimeout on the pane liveness hook.
	hubSessionSnapshotTimeout = 2 * time.Second
)

// Closed label-provenance vocabulary. agent.list records carry mixed label
// provenance — some panes report the herdr agent name, others a tab/display
// or stale label — so the wire marks which class produced each session's
// label instead of letting consumers guess.
const (
	hubLabelSourceAgentName = "agent_name"
	hubLabelSourceTabLabel  = "tab_label"
	hubLabelSourceMissing   = "missing"
)

// HubSession is the allowlisted, metadata-only view of one local herdr agent.
// It intentionally has no prompt, transcript, terminal, path, or UUID field.
// AgentName is the only canonical agent identity: it is copied byte-for-byte
// from a non-empty agent.list name and is never inferred from any display
// field. Label keeps its legacy display role with its provenance declared by
// LabelSource; DisplayLabel carries only the tab.list join. Both are display
// context, never identity.
type HubSession struct {
	PaneID           string `json:"pane_id"`
	WorkspaceID      string `json:"workspace_id"`
	Label            string `json:"label"`
	AgentName        string `json:"agent_name,omitempty"`
	LabelSource      string `json:"label_source,omitempty"`
	DisplayLabel     string `json:"display_label,omitempty"`
	Status           string `json:"status"`
	InteractiveReady *bool  `json:"interactive_ready,omitempty"`
	Revision         int64  `json:"revision"`
	StateChangeSeq   int64  `json:"state_change_seq"`
}

// HubSessionSnapshot is the nested /v1/nodes projection. ReceivedAt is always
// assigned from the hub clock, and Stale is derived from the node presence
// state when Nodes builds this view.
type HubSessionSnapshot struct {
	Sessions       []HubSession `json:"sessions"`
	SnapshotStatus string       `json:"snapshot_status"`
	Truncated      bool         `json:"truncated"`
	ReceivedAt     time.Time    `json:"received_at"`
	Stale          bool         `json:"stale"`
}

type hubSessionSnapshotRecord struct {
	sessions       []HubSession
	snapshotStatus string
	truncated      bool
	receivedAt     time.Time
}

func hubSessionSnapshotsFromStates(states []HerdrAgentState) []HubSession {
	sessions := make([]HubSession, 0, len(states))
	for _, state := range states {
		if !validIdleWakePane(state.PaneID) || !validObservedAgentStatus(state.Status) || state.Revision < 0 || state.SourceStateChangeSeq < 0 {
			continue
		}
		session := HubSession{
			PaneID:           state.PaneID,
			WorkspaceID:      normalizedIdleWakeMetadata(state.WorkspaceID),
			Label:            normalizedIdleWakeMetadata(state.Label),
			AgentName:        normalizedIdleWakeMetadata(state.AgentName),
			DisplayLabel:     normalizedIdleWakeMetadata(state.DisplayLabel),
			Status:           state.Status,
			InteractiveReady: cloneBool(state.InteractiveReady),
			Revision:         state.Revision,
			StateChangeSeq:   state.SourceStateChangeSeq,
		}
		session.LabelSource = hubSessionLabelSource(session.Label, session.AgentName)
		sessions = append(sessions, session)
	}
	sort.Slice(sessions, func(i, j int) bool {
		left, right := sessions[i], sessions[j]
		if left.PaneID != right.PaneID {
			return left.PaneID < right.PaneID
		}
		if left.WorkspaceID != right.WorkspaceID {
			return left.WorkspaceID < right.WorkspaceID
		}
		if left.Label != right.Label {
			return left.Label < right.Label
		}
		if left.Status != right.Status {
			return left.Status < right.Status
		}
		if left.Revision != right.Revision {
			return left.Revision < right.Revision
		}
		if left.StateChangeSeq != right.StateChangeSeq {
			return left.StateChangeSeq < right.StateChangeSeq
		}
		return boolValue(left.InteractiveReady) < boolValue(right.InteractiveReady)
	})
	return sessions
}

// hubSessionLabelSource classifies what the populated label value actually
// is. A label equal to the agent name is the name itself; any other non-empty
// label is display-class (tab/display or stale metadata); an empty label has
// no provenance. The check runs on normalized values so a dropped-overlong
// name cannot flip the class.
func hubSessionLabelSource(label, agentName string) string {
	switch {
	case label == "":
		return hubLabelSourceMissing
	case agentName != "" && label == agentName:
		return hubLabelSourceAgentName
	default:
		return hubLabelSourceTabLabel
	}
}

// validHubSessionLabelSource gates the provenance marker. Empty stays valid
// for pre-additive payloads; a present value must be inside the closed
// vocabulary and consistent with the label/agent_name pair it describes.
func validHubSessionLabelSource(session HubSession) bool {
	switch session.LabelSource {
	case "":
		return true
	case hubLabelSourceMissing:
		return session.Label == ""
	case hubLabelSourceAgentName:
		return session.AgentName != "" && session.Label == session.AgentName
	case hubLabelSourceTabLabel:
		return session.Label != "" && session.Label != session.AgentName
	default:
		return false
	}
}

func boolValue(value *bool) int {
	if value == nil {
		return 0
	}
	if *value {
		return 2
	}
	return 1
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneHubSessions(sessions []HubSession) []HubSession {
	if sessions == nil {
		return nil
	}
	copy := make([]HubSession, len(sessions))
	for index, session := range sessions {
		copy[index] = session
		copy[index].InteractiveReady = cloneBool(session.InteractiveReady)
	}
	return copy
}

func cloneHubSessionSnapshotRecord(snapshot *hubSessionSnapshotRecord) *hubSessionSnapshotRecord {
	if snapshot == nil {
		return nil
	}
	return &hubSessionSnapshotRecord{
		sessions:       cloneHubSessions(snapshot.sessions),
		snapshotStatus: snapshot.snapshotStatus,
		truncated:      snapshot.truncated,
		receivedAt:     snapshot.receivedAt,
	}
}

func isJSONNull(raw []byte) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func hubSessionSnapshotRecordFromHeartbeat(heartbeat hubHeartbeatPayload, receivedAt time.Time) *hubSessionSnapshotRecord {
	var sessions []HubSession
	if heartbeat.Sessions != nil {
		sessions = cloneHubSessions(*heartbeat.Sessions)
	}
	return &hubSessionSnapshotRecord{
		sessions:       sessions,
		snapshotStatus: heartbeat.SnapshotStatus,
		truncated:      heartbeat.Truncated,
		receivedAt:     receivedAt,
	}
}

func hubSessionSnapshotView(snapshot *hubSessionSnapshotRecord, stale bool) *HubSessionSnapshot {
	if snapshot == nil {
		return nil
	}
	return &HubSessionSnapshot{
		Sessions:       cloneHubSessions(snapshot.sessions),
		SnapshotStatus: snapshot.snapshotStatus,
		Truncated:      snapshot.truncated,
		ReceivedAt:     snapshot.receivedAt,
		Stale:          stale,
	}
}

func decodeHubSessionSnapshots(raw []byte) ([]HubSession, bool) {
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return nil, true
	}
	var rows []map[string]json.RawMessage
	if json.Unmarshal(trimmed, &rows) != nil || rows == nil || len(rows) > hubMaxHeartbeatSessions {
		return nil, false
	}
	sessions := make([]HubSession, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, fields := range rows {
		if len(fields) < 6 || len(fields) > 10 {
			return nil, false
		}
		for name := range fields {
			switch name {
			case "pane_id", "workspace_id", "label", "status", "interactive_ready", "revision", "state_change_seq",
				"agent_name", "label_source", "display_label":
			default:
				return nil, false
			}
		}
		for _, name := range []string{"pane_id", "workspace_id", "label", "status", "revision", "state_change_seq"} {
			rawValue, exists := fields[name]
			if !exists || bytes.Equal(bytes.TrimSpace(rawValue), []byte("null")) {
				return nil, false
			}
		}
		for _, name := range []string{"interactive_ready", "agent_name", "label_source", "display_label"} {
			if rawValue, exists := fields[name]; exists && bytes.Equal(bytes.TrimSpace(rawValue), []byte("null")) {
				return nil, false
			}
		}
		encoded, err := json.Marshal(fields)
		if err != nil {
			return nil, false
		}
		var session HubSession
		if json.Unmarshal(encoded, &session) != nil || !validHubSession(session) {
			return nil, false
		}
		if _, duplicate := seen[session.PaneID]; duplicate {
			return nil, false
		}
		seen[session.PaneID] = struct{}{}
		sessions = append(sessions, session)
	}
	return sessions, true
}

func validHubSession(session HubSession) bool {
	return validIdleWakePane(session.PaneID) && validIdleWakeMetadata(session.WorkspaceID) && validIdleWakeMetadata(session.Label) && validIdleWakeMetadata(session.AgentName) && validIdleWakeMetadata(session.DisplayLabel) && validHubSessionLabelSource(session) && validObservedAgentStatus(session.Status) && session.Revision >= 0 && session.StateChangeSeq >= 0
}

func marshalHubHeartbeatForWire(heartbeat hubHeartbeatPayload) ([]byte, bool) {
	payload, err := json.Marshal(heartbeat)
	if err != nil {
		return nil, false
	}
	envelope, err := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: "heartbeat", Payload: payload}))
	if err != nil {
		return payload, false
	}
	return payload, len(payload) < hubMaxMessageBytes && len(envelope) < hubMaxMessageBytes
}

func marshalHubHeartbeatWithSessions(heartbeat hubHeartbeatPayload, states []HerdrAgentState) []byte {
	candidates := hubSessionSnapshotsFromStates(states)
	selected := make([]HubSession, 0, minInt(len(candidates), hubMaxHeartbeatSessions))
	heartbeat.Sessions = &selected
	truncated := len(candidates) > hubMaxHeartbeatSessions
	heartbeat.Truncated = truncated
	for len(selected) < len(candidates) && len(selected) < hubMaxHeartbeatSessions {
		selected = append(selected, candidates[len(selected)])
		heartbeat.Sessions = &selected
		if _, fits := marshalHubHeartbeatForWire(heartbeat); !fits {
			selected = selected[:len(selected)-1]
			heartbeat.Sessions = &selected
			truncated = true
			break
		}
	}
	if len(selected) < len(candidates) {
		truncated = true
	}
	heartbeat.Truncated = truncated
	for {
		payload, fits := marshalHubHeartbeatForWire(heartbeat)
		if fits || len(selected) == 0 {
			return payload
		}
		selected = selected[:len(selected)-1]
		heartbeat.Sessions = &selected
		heartbeat.Truncated = true
	}
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
