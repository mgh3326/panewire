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
)

// HubSession is the allowlisted, metadata-only view of one local herdr agent.
// It intentionally has no prompt, transcript, terminal, path, or UUID field.
type HubSession struct {
	PaneID           string `json:"pane_id"`
	WorkspaceID      string `json:"workspace_id"`
	Label            string `json:"label"`
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
		sessions = append(sessions, HubSession{
			PaneID:           state.PaneID,
			WorkspaceID:      normalizedIdleWakeMetadata(state.WorkspaceID),
			Label:            normalizedIdleWakeMetadata(state.Label),
			Status:           state.Status,
			InteractiveReady: cloneBool(state.InteractiveReady),
			Revision:         state.Revision,
			StateChangeSeq:   state.SourceStateChangeSeq,
		})
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
		if len(fields) < 6 || len(fields) > 7 {
			return nil, false
		}
		for name := range fields {
			switch name {
			case "pane_id", "workspace_id", "label", "status", "interactive_ready", "revision", "state_change_seq":
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
		if rawReady, exists := fields["interactive_ready"]; exists && bytes.Equal(bytes.TrimSpace(rawReady), []byte("null")) {
			return nil, false
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
	return validIdleWakePane(session.PaneID) && validIdleWakeMetadata(session.WorkspaceID) && validIdleWakeMetadata(session.Label) && validObservedAgentStatus(session.Status) && session.Revision >= 0 && session.StateChangeSeq >= 0
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
