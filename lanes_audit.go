package panewire

import (
	"encoding/json"
	"errors"
	"io"
	"sort"
	"time"
)

// The lane verdict vocabulary is closed at exactly three values. A
// lane is "dead" only when the hub's latest session snapshot for its machine
// is fresh, complete, and decodable, and that snapshot does not contain the
// lane's pane. Every case where the observation is incomplete — the node is
// not returned, its snapshot is absent, stale, truncated, or malformed — is
// "indeterminate", never "dead": marking a lane dead on a partial observation
// would let a transient outage masquerade as a dead pane, and a lane deleted
// on that evidence silently loses every later escalation routed to it.
const (
	lanesAuditVerdictAlive         = "alive"
	lanesAuditVerdictDead          = "dead"
	lanesAuditVerdictIndeterminate = "indeterminate"
)

const (
	lanesAuditOutcomeOK        = "ok"
	lanesAuditOutcomeDeadLanes = "dead_lanes"
	lanesAuditOutcomePartial   = "partial"
	lanesAuditOutcomeError     = "error"
)

// Indeterminate reasons, closed vocabulary. Each names the observation gap
// that keeps the lane's pane membership unknowable from hub-held data alone.
const (
	lanesAuditReasonLaneInvalid        = "lane_projection_invalid"
	lanesAuditReasonNodesResponseBad   = "nodes_response_invalid"
	lanesAuditReasonNodesResponseBig   = "nodes_response_oversize"
	lanesAuditReasonNodeNotReturned    = "node_not_returned"
	lanesAuditReasonSnapshotMissing    = "snapshot_missing"
	lanesAuditReasonSnapshotInvalid    = "snapshot_invalid"
	lanesAuditReasonCollectorDown      = "collector_unavailable"
	lanesAuditReasonStatusUnknown      = "snapshot_status_unknown"
	lanesAuditReasonSessionsInvalid    = "sessions_invalid"
	lanesAuditReasonSessionsNull       = "sessions_null"
	lanesAuditReasonReceivedAtMissing  = "received_at_missing"
	lanesAuditReasonNodeStale          = "node_stale"
	lanesAuditReasonNodeDisconnected   = "node_disconnected"
	lanesAuditReasonNodeNotConnected   = "node_not_connected"
	lanesAuditReasonSnapshotStale      = "snapshot_stale"
	lanesAuditReasonReceivedAtFuture   = "received_at_future"
	lanesAuditReasonSnapshotAgeExpired = "snapshot_age_exceeded"
	lanesAuditReasonTruncated          = "truncated"
)

// lanesAuditReasonSessionMismatch is the one reason that rides on a dead
// verdict rather than an indeterminate one. It fires only under an explicit
// --sibling pair when the dead lane's pane is proven present on a sibling
// daemon of the same physical host: the pane is alive, but the lane points
// at the other herdr session's machine id. The verdict stays dead — the
// lane's own machine still does not carry the pane — so the three-value
// vocabulary is untouched.
const lanesAuditReasonSessionMismatch = "session_mismatch"

// lanesAuditResponseMaxBytes caps both hub response bodies. The reader pulls
// one byte past the cap so an oversized body is detected, never silently
// truncated — a cut /v1/nodes body could drop sessions and turn their lanes
// into false dead verdicts.
const lanesAuditResponseMaxBytes = 1 << 20

var errLanesAuditResponseOversize = errors.New("hub response body exceeds limit")

// lanesAuditLanesWire is the narrow /v1/lanes view this command needs: the
// lane rows alone. Control fields are other commands' concern, and unknown
// fields stay ignored so a hub-side additive change cannot break the audit.
type lanesAuditLanesWire struct {
	Lanes []hubLaneProjection `json:"lanes"`
}

// lanesAuditNodeWire is the narrow /v1/nodes row this command reads. Unknown
// fields stay ignored so a hub-side additive change cannot flip a verdict.
type lanesAuditNodeWire struct {
	MachineID       string          `json:"machine_id"`
	State           string          `json:"state"`
	SessionSnapshot json.RawMessage `json:"session_snapshot"`
}

// lanesAuditSnapshotWire keeps Sessions raw so a null or malformed list under
// snapshot_status=ok is distinguishable from an absent snapshot.
type lanesAuditSnapshotWire struct {
	Sessions       json.RawMessage `json:"sessions"`
	SnapshotStatus string          `json:"snapshot_status"`
	Truncated      bool            `json:"truncated"`
	ReceivedAt     time.Time       `json:"received_at"`
	Stale          bool            `json:"stale"`
}

// lanesAuditObservation is the per-machine verdict input: whether the node's
// snapshot is complete enough to judge pane membership, and the sessions it
// lists when it is.
type lanesAuditObservation struct {
	usable   bool
	reason   string
	sessions []HubSession
	lastSeen time.Time
	hasSeen  bool
}

type lanesAuditLaneRow struct {
	Lane      string `json:"lane"`
	Machine   string `json:"machine"`
	Pane      string `json:"pane"`
	Verdict   string `json:"verdict"`
	Reason    string `json:"reason,omitempty"`
	Sibling   string `json:"sibling,omitempty"`
	NodeState string `json:"node_state,omitempty"`
	LastSeen  string `json:"last_seen,omitempty"`
}

type lanesAuditSummary struct {
	Lanes         int `json:"lanes"`
	SinkSkipped   int `json:"sink_skipped"`
	Alive         int `json:"alive"`
	Dead          int `json:"dead"`
	Indeterminate int `json:"indeterminate"`
}

type lanesAuditResult struct {
	FetchedAt string              `json:"fetched_at"`
	Outcome   string              `json:"outcome"`
	Lanes     []lanesAuditLaneRow `json:"lanes"`
	Summary   lanesAuditSummary   `json:"summary"`
}

// classifyLanesAuditNode maps one hub-returned node onto "is this snapshot
// complete enough to judge pane absence". Precedence mirrors the sessions
// find coverage order: an absent/null snapshot first (no observation exists),
// then collector/status/shape contract gaps, then staleness (the whole
// observation is untrusted), then truncation (the pane could be in the cut
// tail). Only a fresh, complete, decodable snapshot is usable.
func classifyLanesAuditNode(node lanesAuditNodeWire, now time.Time) lanesAuditObservation {
	var observation lanesAuditObservation
	raw := node.SessionSnapshot
	if len(raw) == 0 || isJSONNull(raw) {
		observation.reason = lanesAuditReasonSnapshotMissing
		return observation
	}
	var snapshot lanesAuditSnapshotWire
	if json.Unmarshal(raw, &snapshot) != nil {
		observation.reason = lanesAuditReasonSnapshotInvalid
		return observation
	}
	switch snapshot.SnapshotStatus {
	case hubSnapshotStatusUnavailable:
		observation.reason = lanesAuditReasonCollectorDown
		return observation
	case hubSnapshotStatusOK:
	default:
		observation.reason = lanesAuditReasonStatusUnknown
		return observation
	}
	sessions, decodable := decodeLanesAuditSessions(snapshot.Sessions)
	if !decodable {
		observation.reason = lanesAuditReasonSessionsInvalid
		return observation
	}
	if sessions == nil {
		observation.reason = lanesAuditReasonSessionsNull
		return observation
	}
	observation.sessions = sessions
	if snapshot.ReceivedAt.IsZero() {
		observation.reason = lanesAuditReasonReceivedAtMissing
		return observation
	}
	observation.lastSeen = snapshot.ReceivedAt
	observation.hasSeen = true
	if node.State != "connected" {
		switch node.State {
		case "stale":
			observation.reason = lanesAuditReasonNodeStale
		case "disconnected":
			observation.reason = lanesAuditReasonNodeDisconnected
		default:
			observation.reason = lanesAuditReasonNodeNotConnected
		}
		return observation
	}
	if snapshot.Stale {
		observation.reason = lanesAuditReasonSnapshotStale
		return observation
	}
	if snapshot.ReceivedAt.After(now) {
		observation.reason = lanesAuditReasonReceivedAtFuture
		return observation
	}
	if now.Sub(snapshot.ReceivedAt) > sessionsFindSnapshotMaxAge {
		observation.reason = lanesAuditReasonSnapshotAgeExpired
		return observation
	}
	if snapshot.Truncated {
		observation.reason = lanesAuditReasonTruncated
		return observation
	}
	observation.usable = true
	return observation
}

// decodeLanesAuditSessions distinguishes a null/absent sessions field from a
// decodable list. Shape failures are contract gaps; unknown fields inside a
// session object stay ignored so a later wire addition cannot flip a lane to
// indeterminate.
func decodeLanesAuditSessions(raw json.RawMessage) ([]HubSession, bool) {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil, true
	}
	var sessions []HubSession
	if json.Unmarshal(raw, &sessions) != nil {
		return nil, false
	}
	if sessions == nil {
		return nil, false
	}
	for _, session := range sessions {
		if !validHubSession(session) {
			return nil, false
		}
	}
	return sessions, true
}

// lanesAuditSiblings maps a machine id to the sorted machine ids of the other
// daemons sharing its physical host. Built once from the --sibling pairs; an
// empty or nil index disables the sibling check entirely.
type lanesAuditSiblings map[string][]string

// lanesAuditSiblingIndex expands the --sibling A=B pairs into per-machine
// sorted sibling lists. Pairs are symmetric and transitive within one group:
// A=B and B=C put all of A, B, C on each other's lists. Sorted member lists
// keep the reported sibling deterministic when several could match.
func lanesAuditSiblingIndex(pairs [][2]string) lanesAuditSiblings {
	adjacency := make(map[string]map[string]bool)
	for _, pair := range pairs {
		for _, side := range pair {
			if adjacency[side] == nil {
				adjacency[side] = make(map[string]bool)
			}
		}
		adjacency[pair[0]][pair[1]] = true
		adjacency[pair[1]][pair[0]] = true
	}
	index := make(lanesAuditSiblings)
	seen := make(map[string]bool)
	for start := range adjacency {
		if seen[start] {
			continue
		}
		var group []string
		queue := []string{start}
		seen[start] = true
		for len(queue) > 0 {
			member := queue[0]
			queue = queue[1:]
			group = append(group, member)
			for next := range adjacency[member] {
				if !seen[next] {
					seen[next] = true
					queue = append(queue, next)
				}
			}
		}
		sort.Strings(group)
		for _, member := range group {
			others := make([]string, 0, len(group)-1)
			for _, candidate := range group {
				if candidate != member {
					others = append(others, candidate)
				}
			}
			index[member] = others
		}
	}
	return index
}

// auditLaneSiblingHit reports the first sibling machine (sorted order) whose
// own complete observation contains the dead lane's pane. A sibling that is
// absent from /v1/nodes or whose observation is incomplete is skipped:
// session_mismatch must name the daemon the hub's data actually proves the
// pane lives on, never a guess.
func auditLaneSiblingHit(lane hubLaneProjection, nodes map[string]lanesAuditNodeWire, candidates []string, now time.Time) string {
	for _, candidate := range candidates {
		node, returned := nodes[candidate]
		if !returned {
			continue
		}
		observation := classifyLanesAuditNode(node, now)
		if !observation.usable {
			continue
		}
		for _, session := range observation.sessions {
			if session.PaneID == lane.Pane {
				return candidate
			}
		}
	}
	return ""
}

// auditLane judges one lane against the hub's node rows. The verdict compares
// lane.Pane against sessions[].pane_id of the node named by lane.Machine
// only — a pane with the same id on a different machine proves nothing. Sink
// lanes never reach this function: the builder skips them before judging.
func auditLane(lane hubLaneProjection, nodes map[string]lanesAuditNodeWire, siblings lanesAuditSiblings, now time.Time) lanesAuditLaneRow {
	row := lanesAuditLaneRow{Lane: lane.Lane, Machine: lane.Machine, Pane: lane.Pane, Verdict: lanesAuditVerdictIndeterminate}
	if !validHubLaneProjection(lane) {
		row.Reason = lanesAuditReasonLaneInvalid
		return row
	}
	node, returned := nodes[lane.Machine]
	if !returned {
		row.Reason = lanesAuditReasonNodeNotReturned
		return row
	}
	row.NodeState = node.State
	observation := classifyLanesAuditNode(node, now)
	if observation.hasSeen {
		row.LastSeen = observation.lastSeen.UTC().Format(time.RFC3339)
	}
	if !observation.usable {
		row.Reason = observation.reason
		return row
	}
	for _, session := range observation.sessions {
		if session.PaneID == lane.Pane {
			row.Verdict = lanesAuditVerdictAlive
			return row
		}
	}
	row.Verdict = lanesAuditVerdictDead
	if sibling := auditLaneSiblingHit(lane, nodes, siblings[lane.Machine], now); sibling != "" {
		row.Reason = lanesAuditReasonSessionMismatch
		row.Sibling = sibling
	}
	return row
}

func buildLanesAuditResult(lanes []hubLaneProjection, nodes []lanesAuditNodeWire, now time.Time, siblingPairs ...[2]string) lanesAuditResult {
	result := lanesAuditResult{
		FetchedAt: now.UTC().Format(time.RFC3339),
		Lanes:     []lanesAuditLaneRow{},
	}
	byMachine := make(map[string]lanesAuditNodeWire, len(nodes))
	for _, node := range nodes {
		byMachine[node.MachineID] = node
	}
	siblings := lanesAuditSiblingIndex(siblingPairs)
	for _, lane := range lanes {
		if lane.Sink {
			result.Summary.SinkSkipped++
			continue
		}
		row := auditLane(lane, byMachine, siblings, now)
		result.Lanes = append(result.Lanes, row)
		result.Summary.Lanes++
		switch row.Verdict {
		case lanesAuditVerdictAlive:
			result.Summary.Alive++
		case lanesAuditVerdictDead:
			result.Summary.Dead++
		default:
			result.Summary.Indeterminate++
		}
	}
	sort.Slice(result.Lanes, func(i, j int) bool { return result.Lanes[i].Lane < result.Lanes[j].Lane })
	switch {
	case result.Summary.Indeterminate > 0:
		result.Outcome = lanesAuditOutcomePartial
	case result.Summary.Dead > 0:
		result.Outcome = lanesAuditOutcomeDeadLanes
	default:
		result.Outcome = lanesAuditOutcomeOK
	}
	return result
}

// buildLanesAuditNodesRejected renders the audit when the /v1/nodes body was
// rejected wholesale (oversize or unparseable): the lane table is intact, so
// every non-sink lane is shown as indeterminate with the rejection reason.
// Degrading this way is the point of the invariant — a corrupt observation
// must never collapse into a confident alive/dead answer.
func buildLanesAuditNodesRejected(lanes []hubLaneProjection, reason string, now time.Time) lanesAuditResult {
	result := lanesAuditResult{
		FetchedAt: now.UTC().Format(time.RFC3339),
		Outcome:   lanesAuditOutcomePartial,
		Lanes:     []lanesAuditLaneRow{},
	}
	for _, lane := range lanes {
		if lane.Sink {
			result.Summary.SinkSkipped++
			continue
		}
		row := lanesAuditLaneRow{Lane: lane.Lane, Machine: lane.Machine, Pane: lane.Pane, Verdict: lanesAuditVerdictIndeterminate, Reason: reason}
		if !validHubLaneProjection(lane) {
			row.Reason = lanesAuditReasonLaneInvalid
		}
		result.Lanes = append(result.Lanes, row)
		result.Summary.Lanes++
		result.Summary.Indeterminate++
	}
	sort.Slice(result.Lanes, func(i, j int) bool { return result.Lanes[i].Lane < result.Lanes[j].Lane })
	return result
}

// readLanesAuditResponse reads one hub response body under the size cap.
// Bodies larger than the cap are rejected as a class of their own; anything
// else the caller unmarshals whole, so trailing bytes after a valid envelope
// fail decode instead of being silently dropped.
func readLanesAuditResponse(body io.Reader) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(body, lanesAuditResponseMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > lanesAuditResponseMaxBytes {
		return nil, errLanesAuditResponseOversize
	}
	return raw, nil
}
