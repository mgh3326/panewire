package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The lanes-audit verdict vocabulary is closed at exactly three values. A
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

// auditLane judges one lane against the hub's node rows. The verdict compares
// lane.Pane against sessions[].pane_id of the node named by lane.Machine
// only — a pane with the same id on a different machine proves nothing. Sink
// lanes never reach this function: the builder skips them before judging.
func auditLane(lane hubLaneProjection, nodes map[string]lanesAuditNodeWire, now time.Time) lanesAuditLaneRow {
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
	return row
}

func buildLanesAuditResult(lanes []hubLaneProjection, nodes []lanesAuditNodeWire, now time.Time) lanesAuditResult {
	result := lanesAuditResult{
		FetchedAt: now.UTC().Format(time.RFC3339),
		Lanes:     []lanesAuditLaneRow{},
	}
	byMachine := make(map[string]lanesAuditNodeWire, len(nodes))
	for _, node := range nodes {
		byMachine[node.MachineID] = node
	}
	for _, lane := range lanes {
		if lane.Sink {
			result.Summary.SinkSkipped++
			continue
		}
		row := auditLane(lane, byMachine, now)
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

func renderLanesAuditResult(writer io.Writer, result lanesAuditResult, jsonOut bool) {
	if jsonOut {
		encoded, _ := json.Marshal(result)
		fmt.Fprintln(writer, string(encoded))
		return
	}
	fmt.Fprintf(writer, "fetched_at\t%s\n", result.FetchedAt)
	fmt.Fprintf(writer, "outcome\t%s\n", result.Outcome)
	summary := result.Summary
	fmt.Fprintf(writer, "summary\tlanes=%d\tsink_skipped=%d\talive=%d\tdead=%d\tindeterminate=%d\n",
		summary.Lanes, summary.SinkSkipped, summary.Alive, summary.Dead, summary.Indeterminate)
	for _, row := range result.Lanes {
		reason := row.Reason
		if reason == "" {
			reason = "-"
		}
		nodeState := row.NodeState
		if nodeState == "" {
			nodeState = "-"
		}
		lastSeen := row.LastSeen
		if lastSeen == "" {
			lastSeen = "-"
		}
		fmt.Fprintf(writer, "lane\t%s\t%s\t%s\tverdict=%s\treason=%s\tnode_state=%s\tlast_seen=%s\n",
			row.Lane, row.Machine, row.Pane, row.Verdict, reason, nodeState, lastSeen)
	}
}

type lanesAuditOptions struct {
	hubURL   string
	tokenEnv string
	cfEnv    string
	jsonOut  bool
}

func parseLanesAuditArgs(args []string) (lanesAuditOptions, error) {
	var options lanesAuditOptions
	seen := make(map[string]bool)
	positionals := 0
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if !strings.HasPrefix(argument, "-") {
			positionals++
			continue
		}
		name, value, hasValue := strings.Cut(argument, "=")
		switch name {
		case "--json":
			if seen[name] {
				return options, errors.New("duplicate lanes-audit flag")
			}
			seen[name] = true
			parsed := true
			if hasValue {
				parsedValue, err := strconv.ParseBool(value)
				if err != nil {
					return options, errors.New("invalid lanes-audit flag value")
				}
				parsed = parsedValue
			}
			options.jsonOut = parsed
		case "--hub-url", "--hub-token-env", "--hub-cf-env":
			if seen[name] {
				return options, errors.New("duplicate lanes-audit flag")
			}
			seen[name] = true
			if !hasValue {
				if index+1 >= len(args) {
					return options, errors.New("lanes-audit flag value is required")
				}
				index++
				value = args[index]
			}
			switch name {
			case "--hub-url":
				options.hubURL = value
			case "--hub-token-env":
				options.tokenEnv = value
			case "--hub-cf-env":
				options.cfEnv = value
			}
		default:
			return options, errors.New("unknown lanes-audit flag")
		}
	}
	if positionals != 0 || options.hubURL == "" || options.tokenEnv == "" {
		return options, errors.New("lanes-audit requires hub credentials and no positionals")
	}
	return options, nil
}

// runLanesAuditCLI reads exactly two operator endpoints — GET /v1/lanes and
// GET /v1/nodes — and renders the per-lane verdicts. It issues no other
// request and contacts no node: both inputs are data the hub already holds.
// The command is display-only; nothing about a lane, an event, or a file is
// ever written. A rejected /v1/nodes body degrades every lane to
// indeterminate rather than failing the command — the lane table is still
// worth showing. A rejected /v1/lanes body stays an error: indeterminate is
// a per-lane verdict and there are no lanes to attach it to.
func runLanesAuditCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	options, err := parseLanesAuditArgs(args)
	if err != nil {
		if missingHubCLIFlagValue(args, lanesAuditValueFlags, lanesAuditKnownFlags) {
			return writeHubCLIUsage(stderr, "lanes-audit: flag value is required", lanesAuditUsage)
		}
		return writeHubCLIUsage(stderr, "lanes-audit: "+err.Error(), lanesAuditUsage)
	}
	now := time.Now
	if deps.Now != nil {
		now = deps.Now
	}
	result := lanesAuditResult{Lanes: []lanesAuditLaneRow{}}
	fail := func(code int, reason string) int {
		result.FetchedAt = now().UTC().Format(time.RFC3339)
		result.Outcome = lanesAuditOutcomeError
		renderLanesAuditResult(stdout, result, options.jsonOut)
		fmt.Fprintln(stderr, "lanes-audit failed:", reason)
		return code
	}
	env, err := loadHubTokenEnv(options.tokenEnv)
	if err != nil || env.MachineID != hubOperatorMachineID {
		return fail(ExitConditionInvalid, "invalid_operator_token_env")
	}
	var cfAccess hubCFAccessEnv
	if options.cfEnv != "" {
		cfAccess, err = loadHubCFAccessEnv(options.cfEnv)
		if err != nil {
			return fail(ExitConditionInvalid, "invalid_cf_env")
		}
	}
	client, err := newHubOperatorClient(options.hubURL, env.Token, cfAccess, deps, lanesCLIRequestTimeout)
	if err != nil {
		return fail(ExitConditionInvalid, "invalid_hub_url")
	}
	ctx, cancel := context.WithTimeout(context.Background(), lanesCLIRequestTimeout)
	defer cancel()
	lanesResponse, err := client.do(ctx, http.MethodGet, "/v1/lanes", nil, nil)
	if err != nil {
		return fail(ExitInternal, "hub_unreachable")
	}
	defer lanesResponse.Body.Close()
	if lanesResponse.StatusCode != http.StatusOK {
		return fail(ExitDeliveryFailure, "lanes_status_"+fmt.Sprint(lanesResponse.StatusCode))
	}
	lanesRaw, err := readLanesAuditResponse(lanesResponse.Body)
	if err != nil {
		return fail(ExitInternal, "lanes_body_rejected")
	}
	var lanesBody lanesAuditLanesWire
	if json.Unmarshal(lanesRaw, &lanesBody) != nil {
		return fail(ExitInternal, "lanes_decode_failed")
	}
	nodesResponse, err := client.do(ctx, http.MethodGet, "/v1/nodes", nil, nil)
	if err != nil {
		return fail(ExitInternal, "hub_unreachable")
	}
	defer nodesResponse.Body.Close()
	if nodesResponse.StatusCode != http.StatusOK {
		return fail(ExitDeliveryFailure, "nodes_status_"+fmt.Sprint(nodesResponse.StatusCode))
	}
	nodesRaw, nodesErr := readLanesAuditResponse(nodesResponse.Body)
	var nodesBody struct {
		Nodes []lanesAuditNodeWire `json:"nodes"`
	}
	if nodesErr == nil && json.Unmarshal(nodesRaw, &nodesBody) != nil {
		nodesErr = errors.New("nodes body rejected")
	}
	if nodesErr != nil {
		reason := lanesAuditReasonNodesResponseBad
		if errors.Is(nodesErr, errLanesAuditResponseOversize) {
			reason = lanesAuditReasonNodesResponseBig
		}
		result = buildLanesAuditNodesRejected(lanesBody.Lanes, reason, now())
		renderLanesAuditResult(stdout, result, options.jsonOut)
		return ExitPartial
	}
	result = buildLanesAuditResult(lanesBody.Lanes, nodesBody.Nodes, now())
	renderLanesAuditResult(stdout, result, options.jsonOut)
	if result.Outcome == lanesAuditOutcomePartial {
		return ExitPartial
	}
	return ExitOK
}
