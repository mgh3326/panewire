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

// sessionsFindSnapshotMaxAge bounds how old a hub-reported session snapshot
// may be before find treats the observation as stale. Nodes heartbeat on a
// ~10s cadence, so 120s tolerates several missed beats without hiding a
// collector that stopped reporting. It is a named constant so tests can pin
// the contract and the value can be revisited against the real cadence.
const sessionsFindSnapshotMaxAge = 120 * time.Second

const sessionsFindCoverageScope = "hub_returned_nodes"

// The find coverage vocabulary is closed at exactly six values. A node whose
// snapshot is fresh, status ok, non-truncated, and reports sessions is fully
// covered and carries no state: it is counted in coverage.observed and shown
// through matches, never labeled OBSERVED_EMPTY.
const (
	sessionsFindStateUnobserved           = "UNOBSERVED"
	sessionsFindStateCollectorUnavailable = "COLLECTOR_UNAVAILABLE"
	sessionsFindStateObservedEmpty        = "OBSERVED_EMPTY"
	sessionsFindStateInvalidSnapshot      = "INVALID_SNAPSHOT"
	sessionsFindStateStale                = "STALE"
	sessionsFindStatePartial              = "PARTIAL"
)

const (
	sessionsFindOutcomeFound   = "FOUND"
	sessionsFindOutcomeNoMatch = "NO_MATCH"
	sessionsFindOutcomePartial = "PARTIAL"
	sessionsFindOutcomeError   = "ERROR"
)

type sessionsFindNodeWire struct {
	MachineID       string          `json:"machine_id"`
	State           string          `json:"state"`
	SessionSnapshot json.RawMessage `json:"session_snapshot"`
}

// sessionsFindSnapshotWire keeps Sessions as raw JSON so a null or malformed
// list under snapshot_status=ok is visible as INVALID_SNAPSHOT instead of
// decoding to the same shape as an absent snapshot.
type sessionsFindSnapshotWire struct {
	Sessions       json.RawMessage `json:"sessions"`
	SnapshotStatus string          `json:"snapshot_status"`
	Truncated      bool            `json:"truncated"`
	ReceivedAt     time.Time       `json:"received_at"`
	Stale          bool            `json:"stale"`
}

type sessionsFindNodeObservation struct {
	machine   string
	covered   bool
	state     string
	reason    string
	sessions  []HubSession
	lastSeen  time.Time
	hasSeen   bool
	decodable bool
}

type sessionsFindQuery struct {
	Label   string `json:"label"`
	Match   string `json:"match"`
	Machine string `json:"machine,omitempty"`
}

type sessionsFindScope struct {
	CoverageScope string `json:"coverage_scope"`
	Machine       string `json:"machine,omitempty"`
	Nodes         int    `json:"nodes"`
}

type sessionsFindMatch struct {
	Machine          string `json:"machine"`
	PaneID           string `json:"pane_id"`
	WorkspaceID      string `json:"workspace_id"`
	Label            string `json:"label"`
	Status           string `json:"status"`
	InteractiveReady *bool  `json:"interactive_ready,omitempty"`
	Fresh            bool   `json:"fresh"`
	LastSeen         string `json:"last_seen,omitempty"`
	SnapshotState    string `json:"snapshot_state,omitempty"`
}

type sessionsFindNodeCoverage struct {
	Machine  string `json:"machine"`
	Covered  bool   `json:"covered"`
	State    string `json:"state,omitempty"`
	Reason   string `json:"reason,omitempty"`
	Sessions *int   `json:"sessions,omitempty"`
	LastSeen string `json:"last_seen,omitempty"`
}

type sessionsFindCoverage struct {
	Expected    int                        `json:"expected"`
	Observed    int                        `json:"observed"`
	Missing     int                        `json:"missing"`
	Stale       int                        `json:"stale"`
	Truncated   int                        `json:"truncated"`
	Invalid     int                        `json:"invalid"`
	Unavailable int                        `json:"unavailable"`
	Nodes       []sessionsFindNodeCoverage `json:"nodes"`
	Reasons     []string                   `json:"reasons,omitempty"`
}

type sessionsFindResult struct {
	Query     sessionsFindQuery    `json:"query"`
	Scope     sessionsFindScope    `json:"scope"`
	FetchedAt string               `json:"fetched_at"`
	Matches   []sessionsFindMatch  `json:"matches"`
	Coverage  sessionsFindCoverage `json:"coverage"`
	Outcome   string               `json:"outcome"`
}

// classifySessionsFindNode maps one hub-returned node onto the closed
// six-state coverage vocabulary. Precedence is fixed: absent/null first (no
// observation exists to judge), then collector/status/shape contract errors,
// then staleness (the whole observation is untrusted), then truncation, and
// only then a healthy empty list. A fully observed node with sessions is the
// one unlabeled class: covered=true with state="".
func classifySessionsFindNode(node sessionsFindNodeWire, now time.Time) sessionsFindNodeObservation {
	observation := sessionsFindNodeObservation{machine: node.MachineID}
	raw := node.SessionSnapshot
	if len(raw) == 0 || isJSONNull(raw) {
		observation.state = sessionsFindStateUnobserved
		observation.reason = "unknown"
		return observation
	}
	var snapshot sessionsFindSnapshotWire
	if json.Unmarshal(raw, &snapshot) != nil {
		observation.state = sessionsFindStateInvalidSnapshot
		observation.reason = "snapshot_invalid"
		return observation
	}
	switch snapshot.SnapshotStatus {
	case hubSnapshotStatusUnavailable:
		observation.state = sessionsFindStateCollectorUnavailable
		observation.reason = "unavailable"
		return observation
	case hubSnapshotStatusOK:
	default:
		observation.state = sessionsFindStateInvalidSnapshot
		observation.reason = "snapshot_status_unknown"
		return observation
	}
	sessions, decodable := decodeSessionsFindSessions(snapshot.Sessions)
	observation.decodable = decodable
	if !decodable {
		observation.state = sessionsFindStateInvalidSnapshot
		observation.reason = "sessions_invalid"
		return observation
	}
	if sessions == nil {
		observation.state = sessionsFindStateInvalidSnapshot
		observation.reason = "sessions_null"
		return observation
	}
	observation.sessions = sessions
	if snapshot.ReceivedAt.IsZero() {
		observation.state = sessionsFindStateInvalidSnapshot
		observation.reason = "received_at_missing"
		return observation
	}
	observation.lastSeen = snapshot.ReceivedAt
	observation.hasSeen = true
	if node.State != "connected" {
		observation.state = sessionsFindStateStale
		observation.reason = "node_" + sanitizedSessionsFindReason(node.State)
		return observation
	}
	if snapshot.Stale {
		observation.state = sessionsFindStateStale
		observation.reason = "snapshot_stale"
		return observation
	}
	if snapshot.ReceivedAt.After(now) {
		observation.state = sessionsFindStateStale
		observation.reason = "received_at_future"
		return observation
	}
	if now.Sub(snapshot.ReceivedAt) > sessionsFindSnapshotMaxAge {
		observation.state = sessionsFindStateStale
		observation.reason = "snapshot_age_exceeded"
		return observation
	}
	if snapshot.Truncated {
		observation.state = sessionsFindStatePartial
		observation.reason = "truncated"
		return observation
	}
	observation.covered = true
	if len(sessions) == 0 {
		observation.state = sessionsFindStateObservedEmpty
	}
	return observation
}

func sanitizedSessionsFindReason(state string) string {
	if state == "stale" || state == "disconnected" {
		return state
	}
	return "not_connected"
}

// decodeSessionsFindSessions distinguishes a null/absent sessions field from
// a decodable list. Shape failures (non-array, non-object items, wrong field
// types, invalid values) are contract errors; unknown fields stay ignored so
// a later wire addition cannot flip a node to INVALID_SNAPSHOT.
func decodeSessionsFindSessions(raw json.RawMessage) ([]HubSession, bool) {
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

type sessionsFindOptions struct {
	label    string
	hubURL   string
	tokenEnv string
	cfEnv    string
	contains bool
	jsonOut  bool
	machine  string
}

// parseSessionsFindArgs accepts flags before or after the label, matching the
// interleaved grammar of the other operator commands. Go's flag package stops
// at the first positional, so the command parses arguments itself and rejects
// unknown, malformed, or repeated flags at the same usage boundary.
func parseSessionsFindArgs(args []string) (sessionsFindOptions, error) {
	var options sessionsFindOptions
	seen := make(map[string]bool)
	positionals := make([]string, 0, 1)
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if !strings.HasPrefix(argument, "-") {
			positionals = append(positionals, argument)
			continue
		}
		name, value, hasValue := strings.Cut(argument, "=")
		switch name {
		case "--contains", "--json":
			if seen[name] {
				return options, errors.New("duplicate sessions flag")
			}
			seen[name] = true
			parsed := true
			if hasValue {
				parsedValue, err := strconv.ParseBool(value)
				if err != nil {
					return options, errors.New("invalid sessions flag value")
				}
				parsed = parsedValue
			}
			if name == "--contains" {
				options.contains = parsed
			} else {
				options.jsonOut = parsed
			}
		case "--hub-url", "--hub-token-env", "--hub-cf-env", "--machine":
			if seen[name] {
				return options, errors.New("duplicate sessions flag")
			}
			seen[name] = true
			if !hasValue {
				if index+1 >= len(args) {
					return options, errors.New("sessions flag value is required")
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
			case "--machine":
				options.machine = value
			}
		default:
			return options, errors.New("unknown sessions flag")
		}
	}
	if len(positionals) != 1 || positionals[0] == "" || options.hubURL == "" || options.tokenEnv == "" {
		return options, errors.New("sessions find requires exactly one label and hub credentials")
	}
	options.label = positionals[0]
	return options, nil
}

func runSessionsCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 || args[0] != "find" {
		return ExitUsage
	}
	return runSessionsFindCLI(args[1:], stdout, stderr, deps)
}

func runSessionsFindCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	options, err := parseSessionsFindArgs(args)
	if err != nil || !validIdleWakeMetadata(options.label) {
		return ExitUsage
	}
	if options.machine != "" && !machineIDPattern.MatchString(options.machine) {
		return ExitUsage
	}
	now := time.Now
	if deps.Now != nil {
		now = deps.Now
	}
	result := sessionsFindResult{
		Query:   sessionsFindQuery{Label: options.label, Match: "exact", Machine: options.machine},
		Scope:   sessionsFindScope{CoverageScope: sessionsFindCoverageScope, Machine: options.machine},
		Matches: []sessionsFindMatch{},
	}
	result.Coverage.Nodes = []sessionsFindNodeCoverage{}
	if options.contains {
		result.Query.Match = "contains"
	}
	fail := func(code int, reason string) int {
		result.FetchedAt = now().UTC().Format(time.RFC3339)
		result.Outcome = sessionsFindOutcomeError
		result.Coverage.Reasons = []string{reason}
		renderSessionsFindResult(stdout, result, options.jsonOut)
		fmt.Fprintln(stderr, "sessions find failed:", reason)
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
	client, err := newHubOperatorClient(options.hubURL, env.Token, cfAccess, deps, 15*time.Second)
	if err != nil {
		return fail(ExitConditionInvalid, "invalid_hub_url")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	response, err := client.do(ctx, http.MethodGet, "/v1/nodes", nil, nil)
	if err != nil {
		return fail(ExitInternal, "hub_unreachable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fail(ExitDeliveryFailure, "hub_status_"+fmt.Sprint(response.StatusCode))
	}
	result.FetchedAt = now().UTC().Format(time.RFC3339)
	var body struct {
		Nodes []sessionsFindNodeWire `json:"nodes"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&body) != nil {
		return fail(ExitInternal, "decode_failed")
	}
	var scoped []sessionsFindNodeWire
	if options.machine != "" {
		for _, node := range body.Nodes {
			if node.MachineID == options.machine {
				scoped = append(scoped, node)
			}
		}
		if len(scoped) == 0 {
			return fail(ExitConditionInvalid, "machine_not_returned")
		}
	} else {
		scoped = body.Nodes
	}
	result = buildSessionsFindResult(result.Query, scoped, now())
	renderSessionsFindResult(stdout, result, options.jsonOut)
	switch result.Outcome {
	case sessionsFindOutcomeFound, sessionsFindOutcomeNoMatch:
		return ExitOK
	case sessionsFindOutcomePartial:
		return ExitPartial
	default:
		return ExitInternal
	}
}

func buildSessionsFindResult(query sessionsFindQuery, nodes []sessionsFindNodeWire, now time.Time) sessionsFindResult {
	result := sessionsFindResult{
		Query:     query,
		Scope:     sessionsFindScope{CoverageScope: sessionsFindCoverageScope, Machine: query.Machine},
		FetchedAt: now.UTC().Format(time.RFC3339),
		Matches:   []sessionsFindMatch{},
	}
	result.Coverage.Nodes = []sessionsFindNodeCoverage{}
	result.Scope.Nodes = len(nodes)
	result.Coverage.Expected = len(nodes)
	coverage := &result.Coverage
	for _, node := range nodes {
		observation := classifySessionsFindNode(node, now)
		entry := sessionsFindNodeCoverage{Machine: observation.machine, Covered: observation.covered, State: observation.state, Reason: observation.reason}
		if observation.decodable && observation.sessions != nil {
			count := len(observation.sessions)
			entry.Sessions = &count
		}
		if observation.hasSeen {
			entry.LastSeen = observation.lastSeen.UTC().Format(time.RFC3339)
		}
		coverage.Nodes = append(coverage.Nodes, entry)
		switch observation.state {
		case sessionsFindStateUnobserved:
			coverage.Missing++
		case sessionsFindStateCollectorUnavailable:
			coverage.Unavailable++
		case sessionsFindStateInvalidSnapshot:
			coverage.Invalid++
		case sessionsFindStateStale:
			coverage.Stale++
		case sessionsFindStatePartial:
			coverage.Truncated++
		}
		if observation.covered {
			coverage.Observed++
		}
		for _, session := range observation.sessions {
			if !sessionsFindLabelMatches(session.Label, query) {
				continue
			}
			match := sessionsFindMatch{
				Machine:          observation.machine,
				PaneID:           session.PaneID,
				WorkspaceID:      session.WorkspaceID,
				Label:            session.Label,
				Status:           session.Status,
				InteractiveReady: cloneBool(session.InteractiveReady),
				Fresh:            observation.covered || observation.state == sessionsFindStatePartial,
				SnapshotState:    observation.state,
			}
			if match.SnapshotState == sessionsFindStateObservedEmpty || match.SnapshotState == sessionsFindStateInvalidSnapshot {
				match.SnapshotState = ""
			}
			if observation.hasSeen {
				match.LastSeen = observation.lastSeen.UTC().Format(time.RFC3339)
			}
			result.Matches = append(result.Matches, match)
		}
	}
	sort.Slice(coverage.Nodes, func(i, j int) bool { return coverage.Nodes[i].Machine < coverage.Nodes[j].Machine })
	sort.Slice(result.Matches, func(i, j int) bool {
		left, right := result.Matches[i], result.Matches[j]
		if left.Machine != right.Machine {
			return left.Machine < right.Machine
		}
		if left.PaneID != right.PaneID {
			return left.PaneID < right.PaneID
		}
		if left.WorkspaceID != right.WorkspaceID {
			return left.WorkspaceID < right.WorkspaceID
		}
		if left.Label != right.Label {
			return left.Label < right.Label
		}
		return left.Status < right.Status
	})
	for _, reason := range []struct {
		name  string
		count int
	}{
		{"missing", coverage.Missing},
		{"unavailable", coverage.Unavailable},
		{"stale", coverage.Stale},
		{"truncated", coverage.Truncated},
		{"invalid", coverage.Invalid},
	} {
		if reason.count > 0 {
			coverage.Reasons = append(coverage.Reasons, reason.name)
		}
	}
	complete := coverage.Observed == coverage.Expected
	switch {
	case !complete:
		result.Outcome = sessionsFindOutcomePartial
	case len(result.Matches) > 0:
		result.Outcome = sessionsFindOutcomeFound
	default:
		result.Outcome = sessionsFindOutcomeNoMatch
	}
	return result
}

func sessionsFindLabelMatches(candidate string, query sessionsFindQuery) bool {
	if query.Match == "contains" {
		return strings.Contains(candidate, query.Label)
	}
	return candidate == query.Label
}

func renderSessionsFindResult(writer io.Writer, result sessionsFindResult, jsonOut bool) {
	if jsonOut {
		encoded, _ := json.Marshal(result)
		fmt.Fprintln(writer, string(encoded))
		return
	}
	machine := result.Query.Machine
	if machine == "" {
		machine = "all"
	}
	fmt.Fprintf(writer, "query\tlabel=%q\tmatch=%s\tmachine=%s\n", result.Query.Label, result.Query.Match, machine)
	fmt.Fprintf(writer, "fetched_at\t%s\n", result.FetchedAt)
	fmt.Fprintf(writer, "outcome\t%s\n", result.Outcome)
	fmt.Fprintf(writer, "scope\tcoverage_scope=%s\tnodes=%d\n", result.Scope.CoverageScope, result.Scope.Nodes)
	coverage := result.Coverage
	fmt.Fprintf(writer, "coverage\texpected=%d\tobserved=%d\tmissing=%d\tstale=%d\ttruncated=%d\tinvalid=%d\tunavailable=%d\n",
		coverage.Expected, coverage.Observed, coverage.Missing, coverage.Stale, coverage.Truncated, coverage.Invalid, coverage.Unavailable)
	for _, node := range coverage.Nodes {
		state := node.State
		if state == "" {
			state = "-"
		}
		reason := node.Reason
		if reason == "" {
			reason = "-"
		}
		sessions := "-"
		if node.Sessions != nil {
			sessions = fmt.Sprint(*node.Sessions)
		}
		lastSeen := node.LastSeen
		if lastSeen == "" {
			lastSeen = "-"
		}
		fmt.Fprintf(writer, "node\t%s\tcovered=%t\tstate=%s\treason=%s\tsessions=%s\tlast_seen=%s\n",
			node.Machine, node.Covered, state, reason, sessions, lastSeen)
	}
	fmt.Fprintf(writer, "matches\t%d\n", len(result.Matches))
	for _, match := range result.Matches {
		state := match.SnapshotState
		if state == "" {
			state = "-"
		}
		lastSeen := match.LastSeen
		if lastSeen == "" {
			lastSeen = "-"
		}
		fmt.Fprintf(writer, "match\t%s\t%s\t%s\t%s\t%s\tfresh=%t\tlast_seen=%s\tsnapshot_state=%s\n",
			match.Machine, match.PaneID, match.WorkspaceID, match.Label, match.Status, match.Fresh, lastSeen, state)
	}
	switch result.Outcome {
	case sessionsFindOutcomeNoMatch:
		fmt.Fprintln(writer, "note\tno match in observed snapshots")
	case sessionsFindOutcomePartial:
		fmt.Fprintf(writer, "note\tpartial coverage: %s\n", strings.Join(coverage.Reasons, ","))
	case sessionsFindOutcomeError:
		fmt.Fprintf(writer, "error\t%s\n", strings.Join(coverage.Reasons, ","))
	}
}
