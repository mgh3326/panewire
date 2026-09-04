package panewire

import (
	"encoding/json"
	"os"
	"strings"
)

// reportRelayRoutes is intentionally a tiny operator-owned configuration:
// routes contain identifiers only, never host addresses, tokens, or panes
// from a particular installation.
type reportRelayRoutes struct {
	Routes map[string]reportRelayRoute `json:"routes"`
}
type reportRelayRoute struct {
	Machine string `json:"machine"`
	Pane    string `json:"pane"`
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
	for lane, route := range routes.Routes {
		if !hubAgentLabelPattern.MatchString(lane) || !machineIDPattern.MatchString(route.Machine) || strings.TrimSpace(route.Pane) == "" || len(route.Pane) > 128 {
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
	return "(같은 내용이 두 번 보이면 재실행 금지) [report] " + completion.Label + " (" + completion.Host + ") :: " + line + " → " + completion.ReportPath
}

func (h *HubServer) relayJobCompletion(completion hubJobEventPayload) {
	if completion.OwnerLane == "" || completion.ReportPath == "" {
		return
	}
	key := completion.JobID + "\x00" + string(rune(completion.Epoch)) + "\x00" + completion.ReportPath
	h.mu.Lock()
	if _, exists := h.relayDedupe[key]; exists {
		h.mu.Unlock()
		return
	}
	h.relayDedupe[key] = struct{}{}
	route, exists := loadReportRelayRoutes(h.reportRelayPath)[completion.OwnerLane]
	var agent *hubAgent
	if exists && h.nodes[route.Machine] != nil {
		agent = h.nodes[route.Machine].agent
	}
	h.mu.Unlock()
	if !exists || agent == nil || !agent.queueRelay(hubRelayInjectEvent{Type: "relay.inject", JobID: completion.JobID, Pane: route.Pane, Text: relayText(completion)}) {
		// An unrouted/temporarily disconnected target remains observable to the
		// operator event feed. It is deliberately not reinterpreted as success.
		payload, _ := json.Marshal(completion)
		h.broadcast(hubEvent{Kind: "relay.unrouted", Payload: payload, Received: h.now().UTC()})
	}
}
