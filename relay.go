package panewire

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
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

func relayDedupeKey(completion hubJobEventPayload) string {
	return completion.JobID + "\x00" + strconv.FormatUint(completion.Epoch, 10) + "\x00" + completion.ReportPath
}

func relayEventDedupeKey(kind string, completion hubJobEventPayload) string {
	return kind + "\x00" + relayDedupeKey(completion)
}

func (h *HubServer) relayJobCompletion(completion hubJobEventPayload) {
	h.relayJobEvent("job.completed", completion)
}

func (h *HubServer) relayJobEvent(kind string, event hubJobEventPayload) {
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
	if !exists || agent == nil || !agent.queueRelay(hubRelayInjectEvent{Type: "relay.inject", JobID: event.JobID, Pane: route.Pane, Text: relayText(event)}) {
		// An unrouted/temporarily disconnected target remains observable to the
		// operator event feed. It is deliberately not reinterpreted as success.
		payload, _ := json.Marshal(event)
		h.broadcast(hubEvent{Kind: "relay.unrouted", Payload: payload, Received: h.now().UTC()})
		return
	}
	h.startRelayAck(event.JobID, route.Machine, route.Pane)
}
