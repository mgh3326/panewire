package panewire

import (
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"time"
)

// The hub half of session-reap (#603) stage 1: validate a node's report, keep
// the latest one per machine in memory, and serve it to operators. The hub
// never closes anything and never upgrades a node verdict. At read time it
// may only downgrade: a row whose pane is the route of some other lane (a
// resident session such as director-1 or checker-1) is shown as held.

const (
	sessionReapReasonLaneRoute       = "lane-route"
	sessionReapReasonLanesUnreadable = "lanes-unreadable"
)

var sessionReapReasonPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:=()-]{0,127}$`)

type hubSessionReapRecord struct {
	report     SessionReapReport
	receivedAt time.Time
}

// HubSessionReapNode is one machine's entry in GET /v1/session-reap. Stale is
// true when the node is not currently connected: the report then describes
// panes the hub can no longer vouch for.
type HubSessionReapNode struct {
	MachineID  string            `json:"machine_id"`
	State      string            `json:"state"`
	Stale      bool              `json:"stale"`
	ReceivedAt time.Time         `json:"received_at"`
	Report     SessionReapReport `json:"report"`
}

// parseHubSessionReapReport accepts only a session.reap.report event
// envelope. Anything else returns false so the regular inbound parser runs.
func parseHubSessionReapReport(payload []byte) (SessionReapReport, bool, bool) {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(payload, &envelope) != nil || len(envelope) != 3 {
		return SessionReapReport{}, false, false
	}
	var messageType, kind string
	if json.Unmarshal(envelope["type"], &messageType) != nil || messageType != "event" || json.Unmarshal(envelope["kind"], &kind) != nil || kind != sessionReapEventKind {
		return SessionReapReport{}, false, false
	}
	report, valid := decodeSessionReapReport(envelope["payload"])
	return report, true, valid
}

func decodeSessionReapReport(raw []byte) (SessionReapReport, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 8 {
		return SessionReapReport{}, false
	}
	for _, name := range []string{"schema", "generated_at", "grace_seconds", "observed", "jobs_readable", "truncated", "rows", "summary"} {
		if value, exists := fields[name]; !exists || isJSONNull(value) {
			return SessionReapReport{}, false
		}
	}
	var report SessionReapReport
	if json.Unmarshal(fields["schema"], &report.Schema) != nil || report.Schema != sessionReapSchema {
		return SessionReapReport{}, false
	}
	if json.Unmarshal(fields["generated_at"], &report.GeneratedAt) != nil {
		return SessionReapReport{}, false
	}
	if _, err := time.Parse(time.RFC3339, report.GeneratedAt); err != nil {
		return SessionReapReport{}, false
	}
	if json.Unmarshal(fields["grace_seconds"], &report.GraceSeconds) != nil || report.GraceSeconds < 0 || report.GraceSeconds > int64(sessionReapMaxGrace/time.Second) {
		return SessionReapReport{}, false
	}
	if json.Unmarshal(fields["observed"], &report.Observed) != nil || json.Unmarshal(fields["jobs_readable"], &report.JobsReadable) != nil || json.Unmarshal(fields["truncated"], &report.Truncated) != nil {
		return SessionReapReport{}, false
	}
	var summaryFields map[string]json.RawMessage
	if json.Unmarshal(fields["summary"], &summaryFields) != nil || len(summaryFields) != 5 {
		return SessionReapReport{}, false
	}
	for _, name := range []string{"panes", "no_job", "candidate", "builder_task_gate", "held"} {
		if _, exists := summaryFields[name]; !exists {
			return SessionReapReport{}, false
		}
	}
	if json.Unmarshal(fields["summary"], &report.Summary) != nil {
		return SessionReapReport{}, false
	}
	summary := report.Summary
	if summary.Panes < 0 || summary.NoJob < 0 || summary.Candidate < 0 || summary.BuilderTaskGate < 0 || summary.Held < 0 || summary.NoJob+summary.Candidate+summary.BuilderTaskGate+summary.Held != summary.Panes {
		return SessionReapReport{}, false
	}
	var rawRows []map[string]json.RawMessage
	if json.Unmarshal(fields["rows"], &rawRows) != nil || rawRows == nil || len(rawRows) > sessionReapMaxRows {
		return SessionReapReport{}, false
	}
	if (!report.Observed || !report.JobsReadable) && (len(rawRows) != 0 || summary.Panes != 0) {
		return SessionReapReport{}, false
	}
	report.Rows = make([]SessionReapRow, 0, len(rawRows))
	seen := make(map[string]struct{}, len(rawRows))
	counts := map[string]int{}
	for _, rawRow := range rawRows {
		row, valid := decodeSessionReapRow(rawRow)
		if !valid {
			return SessionReapReport{}, false
		}
		if _, duplicate := seen[row.PaneID]; duplicate {
			return SessionReapReport{}, false
		}
		seen[row.PaneID] = struct{}{}
		counts[row.Class]++
		report.Rows = append(report.Rows, row)
	}
	// Rows may be truncated, never inflated: each class count is an upper
	// bound for its rows, and an untruncated report lists every row.
	if counts[sessionReapClassCandidate] > summary.Candidate || counts[sessionReapClassBuilderTaskGate] > summary.BuilderTaskGate || counts[sessionReapClassHeld] > summary.Held {
		return SessionReapReport{}, false
	}
	if !report.Truncated && len(report.Rows) != summary.Candidate+summary.BuilderTaskGate+summary.Held {
		return SessionReapReport{}, false
	}
	return report, true
}

func decodeSessionReapRow(fields map[string]json.RawMessage) (SessionReapRow, bool) {
	for name, value := range fields {
		switch name {
		case "pane_id", "tab_id", "workspace_id", "agent_name", "status", "job_id", "owner_lane", "role", "class", "reason", "terminal_kind", "terminal_at", "terminal_age_seconds":
		default:
			return SessionReapRow{}, false
		}
		if isJSONNull(value) {
			return SessionReapRow{}, false
		}
	}
	for _, name := range []string{"pane_id", "status", "job_id", "class"} {
		if _, exists := fields[name]; !exists {
			return SessionReapRow{}, false
		}
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		return SessionReapRow{}, false
	}
	var row SessionReapRow
	if json.Unmarshal(encoded, &row) != nil {
		return SessionReapRow{}, false
	}
	if !validIdleWakePane(row.PaneID) || !validIdleWakeMetadata(row.TabID) || !validIdleWakeMetadata(row.WorkspaceID) || !validIdleWakeMetadata(row.AgentName) || !validObservedAgentStatus(row.Status) || !hubJobIDPattern.MatchString(row.JobID) {
		return SessionReapRow{}, false
	}
	if row.OwnerLane != "" && !hubAgentLabelPattern.MatchString(row.OwnerLane) {
		return SessionReapRow{}, false
	}
	if row.Role != "" && !hubLastEventKindPattern.MatchString(row.Role) {
		return SessionReapRow{}, false
	}
	if row.Reason != "" && !sessionReapReasonPattern.MatchString(row.Reason) {
		return SessionReapRow{}, false
	}
	if row.TerminalKind != "" && !fleetCensusTerminalKinds[row.TerminalKind] {
		return SessionReapRow{}, false
	}
	if row.TerminalAt != "" {
		if _, err := time.Parse(time.RFC3339, row.TerminalAt); err != nil {
			return SessionReapRow{}, false
		}
	}
	if (row.TerminalKind == "") != (row.TerminalAt == "") || (row.TerminalKind == "") != (row.TerminalAgeSeconds == nil) {
		return SessionReapRow{}, false
	}
	switch row.Class {
	case sessionReapClassCandidate:
		// A node only proposes a worker whose job reached a terminal event,
		// and it names the agent it matched on.
		if row.Reason != "" || row.Role != "worker" || row.TerminalKind == "" || row.AgentName == "" || (row.Status != "idle" && row.Status != "done") {
			return SessionReapRow{}, false
		}
	case sessionReapClassBuilderTaskGate:
		if row.Reason != "" || row.Role != "builder" || row.AgentName == "" || (row.Status != "idle" && row.Status != "done") {
			return SessionReapRow{}, false
		}
	case sessionReapClassHeld:
		if row.Reason == "" {
			return SessionReapRow{}, false
		}
	default:
		return SessionReapRow{}, false
	}
	return row, true
}

// recordSessionReapReport keeps the latest valid report per connected node.
func (h *HubServer) recordSessionReapReport(machineID string, agent *hubAgent, report SessionReapReport) bool {
	if agent.transient || !h.touch(machineID, agent) {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sessionReap == nil {
		h.sessionReap = make(map[string]*hubSessionReapRecord)
	}
	h.sessionReap[machineID] = &hubSessionReapRecord{report: report, receivedAt: h.now().UTC()}
	return true
}

// handleSessionReapMessage is the inbound hook. It returns true when the
// payload was a session.reap.report envelope, valid or not.
func (h *HubServer) handleSessionReapMessage(machineID string, agent *hubAgent, payload []byte) bool {
	report, isReport, valid := parseHubSessionReapReport(payload)
	if !isReport {
		return false
	}
	if !valid || !h.recordSessionReapReport(machineID, agent, report) {
		h.countUnknownMessage()
	}
	return true
}

func (h *HubServer) handleSessionReap(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	routes, _, _, lanesErr := loadReportRelayRoutesAndControlResult(h.reportRelayPath)
	h.mu.Lock()
	nodes := make([]HubSessionReapNode, 0, len(h.sessionReap))
	for machineID, record := range h.sessionReap {
		state := "unknown"
		if node := h.nodes[machineID]; node != nil {
			state = node.state
		}
		report := record.report
		report.Rows = append([]SessionReapRow(nil), record.report.Rows...)
		nodes = append(nodes, HubSessionReapNode{MachineID: machineID, State: state, Stale: state != "connected", ReceivedAt: record.receivedAt, Report: report})
	}
	h.mu.Unlock()
	for index := range nodes {
		downgradeSessionReapRows(&nodes[index], routes, lanesErr != nil)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].MachineID < nodes[j].MachineID })
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(struct {
		Nodes []HubSessionReapNode `json:"nodes"`
	}{Nodes: nodes})
}

// downgradeSessionReapRows holds every non-held row whose pane some other
// lane routes to, and every non-held row when the routes cannot be read. The
// summary moves with the rows so counts stay consistent.
func downgradeSessionReapRows(node *HubSessionReapNode, routes map[string]reportRelayRoute, lanesUnreadable bool) {
	for index := range node.Report.Rows {
		row := &node.Report.Rows[index]
		if row.Class == sessionReapClassHeld {
			continue
		}
		reason := ""
		if lanesUnreadable {
			reason = sessionReapReasonLanesUnreadable
		} else {
			for lane, route := range routes {
				if !route.Sink && route.Machine == node.MachineID && route.Pane == row.PaneID && lane != row.AgentName {
					reason = sessionReapReasonLaneRoute
					break
				}
			}
		}
		if reason == "" {
			continue
		}
		if row.Class == sessionReapClassCandidate {
			node.Report.Summary.Candidate--
		} else {
			node.Report.Summary.BuilderTaskGate--
		}
		node.Report.Summary.Held++
		row.Class, row.Reason = sessionReapClassHeld, reason
	}
}
