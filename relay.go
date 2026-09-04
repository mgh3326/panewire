package panewire

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

// reportRelayRoutes is intentionally a tiny operator-owned configuration:
// routes contain identifiers only, never host addresses, tokens, or panes
// from a particular installation.
type reportRelayRoutes struct {
	Routes map[string]reportRelayRoute `json:"routes"`
	Lanes  map[string]reportRelayRoute `json:"lanes"`
}
type reportRelayRoute struct {
	Machine string `json:"machine"`
	Pane    string `json:"pane"`
	Parent  string `json:"parent,omitempty"`
}

func loadReportRelayRoutes(path string) map[string]reportRelayRoute {
	if path == "" {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil || len(b) > 64<<10 {
		return nil
	}
	var routes reportRelayRoutes
	if json.Unmarshal(b, &routes) != nil {
		return nil
	}
	// lanes is the R19 contract. Keep routes as a deliberate compatibility
	// reader for installations that have not renamed their operator file yet.
	if routes.Lanes != nil {
		routes.Routes = routes.Lanes
	}
	for lane, route := range routes.Routes {
		if !hubAgentLabelPattern.MatchString(lane) || !machineIDPattern.MatchString(route.Machine) || strings.TrimSpace(route.Pane) == "" || len(route.Pane) > 128 {
			delete(routes.Routes, lane)
			continue
		}
		if route.Parent != "" && !hubAgentLabelPattern.MatchString(route.Parent) {
			delete(routes.Routes, lane)
		}
	}
	return routes.Routes
}

func relayText(completion hubJobEventPayload) string {
	return relayTextForKind("job.completed", completion)
}

func relayTextForKind(kind string, event hubJobEventPayload) string {
	if kind == "job.escalate" {
		return escalationRelayText(event)
	}
	if kind == "job.joined" {
		head := truncateRelayText(event.Head, 9)
		return boundRelayText("[joined] "+truncateRelayText(event.Label, 120)+" :: PR "+truncateRelayText(event.PR, 120)+" @ "+head+" → ", event.ReportPath)
	}
	return completedRelayText(event)
}

func escalationRelayText(event hubJobEventPayload) string {
	const max = 512
	prefix := "[escalate] " + truncateRelayText(event.Label, 64) + " (" + truncateRelayText(event.Host, 64) + ") :: Q: "
	question, _ := truncateHubRelayPayloadText(event.Question, true)
	arrow := " … → "
	fullText := " (전문: "
	path := event.ReportPath
	// Keep the question ahead of the path, but retain both the established
	// arrow form and an explicit full-text pointer within the 512-byte note.
	fixed := len(prefix) + len(arrow) + len(fullText) + len(")")
	roomForPaths := max - fixed - len(question)
	if roomForPaths < 2 {
		question = truncateRelayText(question, max-fixed-2)
		roomForPaths = 2
	}
	path = truncateRelayText(path, roomForPaths/2)
	return prefix + question + arrow + path + fullText + path + ")"
}

func completedRelayText(completion hubJobEventPayload) string {
	line := strings.ReplaceAll(strings.ReplaceAll(completion.ReportLastLine, "\n", " "), "\r", " ")
	if len(line) > 240 {
		line = line[:240]
	}
	reason := ""
	if completion.Reason != "" {
		reason = " [reason: " + completion.Reason + "]"
	}
	return "(같은 내용이 두 번 보이면 재실행 금지) [report] " + completion.Label + " (" + completion.Host + ")" + reason + " :: " + line + " → " + completion.ReportPath
}

func truncateRelayText(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	for limit > 0 && !utf8.ValidString(value[:limit]) {
		limit--
	}
	return value[:limit]
}

// boundRelayText protects the websocket note contract while keeping the
// human question/PR context intact; the report path is deliberately last.
func boundRelayText(prefix, reportPath string) string {
	const max = 512
	if len(prefix)+len(reportPath) <= max {
		return prefix + reportPath
	}
	if len(prefix) >= max {
		return truncateRelayText(prefix, max)
	}
	return prefix + truncateRelayText(reportPath, max-len(prefix))
}

func relayDedupeKey(completion hubJobEventPayload) string {
	return completion.JobID + "\x00" + strconv.FormatUint(completion.Epoch, 10) + "\x00" + completion.ReportPath
}

func relayEventDedupeKey(kind string, completion hubJobEventPayload) string {
	return kind + "\x00" + relayDedupeKey(completion) + "\x00" + completion.Reason
}

func (h *HubServer) relayJobCompletion(completion hubJobEventPayload) {
	h.relayJobCompletionFrom("", completion)
}

func (h *HubServer) relayJobEvent(kind string, event hubJobEventPayload) {
	h.relayJobEventFrom("", kind, event)
}

func (h *HubServer) relayJobCompletionFrom(sourceMachine string, completion hubJobEventPayload) {
	h.relayJobEventFrom(sourceMachine, "job.completed", completion)
}

func (h *HubServer) relayJobEventFrom(sourceMachine, kind string, event hubJobEventPayload) {
	if event.OwnerLane == "" || event.JobID == "" {
		return
	}
	key := relayEventDedupeKey(kind, event)
	h.mu.Lock()
	if _, exists := h.relayDedupe[key]; exists {
		h.mu.Unlock()
		return
	}
	h.relayDedupe[key] = struct{}{}
	if event.Replay && !event.EventTime.IsZero() && !event.EventTime.After(h.startedAt.Add(-h.relayReplayGrace)) {
		h.mu.Unlock()
		payload, _ := json.Marshal(event)
		h.broadcast(hubEvent{MachineID: sourceMachine, Kind: "relay.replayed", Payload: payload, Received: h.now().UTC()})
		h.sendRelayAck(relayPending{sourceMachine: sourceMachine, kind: kind, event: event}, "unconfirmed")
		return
	}
	routes := loadReportRelayRoutes(h.reportRelayPath)
	route, exists := routes[event.OwnerLane]
	if exists && (kind == "job.escalate" || kind == "job.joined") {
		parent, parentExists := routes[route.Parent]
		if route.Parent == "" || !parentExists {
			exists = false
		} else {
			route = parent
		}
	}
	var agent *hubAgent
	if exists && h.nodes[route.Machine] != nil {
		agent = h.nodes[route.Machine].agent
	}
	h.mu.Unlock()
	if !exists || agent == nil || !agent.queueRelay(hubRelayInjectEvent{Type: "relay.inject", JobID: event.JobID, Pane: route.Pane, Text: relayTextForKind(kind, event)}) {
		// An unrouted/temporarily disconnected target remains observable to the
		// operator event feed. It is deliberately not reinterpreted as success.
		payload, _ := json.Marshal(event)
		h.broadcast(hubEvent{Kind: "relay.unrouted", Payload: payload, Received: h.now().UTC()})
		return
	}
	h.startRelayAckForRelay(event.JobID, route.Machine, route.Pane, sourceMachine, kind, event)
}
