package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
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
	return kind + "\x00" + relayDedupeKey(completion)
}

func (h *HubServer) relayJobCompletion(completion hubJobEventPayload) {
	h.relayJobEvent("job.completed", completion)
}

// relayJobEvent routes, persists, then injects. The order is the contract:
// Postgres is the canonical record of "was this reported", so injecting an
// event nobody durably stored is how a restart turns into a resend storm.
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
	h.mu.Unlock()
	route, agent, routed := h.resolveRelayRoute(kind, event)
	if !routed {
		h.broadcastRelayUnrouted(event)
		return
	}
	eventID, persisted := h.persistRelayEvent(kind, event, route)
	if !persisted {
		// The event must stay resendable: the node still holds it, and its
		// next attempt has to survive this hub's in-memory dedupe.
		h.mu.Lock()
		delete(h.relayDedupe, key)
		h.mu.Unlock()
		h.broadcastRelayUnpersisted(kind, event)
		return
	}
	h.injectRelayEvent(kind, event, route, agent, eventID)
}

func (h *HubServer) resolveRelayRoute(kind string, event hubJobEventPayload) (reportRelayRoute, *hubAgent, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
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
	return route, agent, exists && agent != nil
}

func (h *HubServer) injectRelayEvent(kind string, event hubJobEventPayload, route reportRelayRoute, agent *hubAgent, eventID int64) {
	if !agent.queueRelay(hubRelayInjectEvent{Type: "relay.inject", JobID: event.JobID, Pane: route.Pane, Text: relayTextForKind(kind, event)}) {
		h.broadcastRelayUnrouted(event)
		return
	}
	h.startRelayAckEvent(event.JobID, route.Machine, route.Pane, eventID)
	if eventID != 0 {
		// The node retires its outbox row on this, not on the injection itself.
		agent.queuePersisted(hubRelayPersistedEvent{Type: "relay.persisted", JobID: event.JobID, Kind: kind, Epoch: event.Epoch, ReportPath: event.ReportPath, Reason: event.Reason, EventID: eventID})
	}
}

// An unrouted/temporarily disconnected target remains observable to the
// operator event feed. It is deliberately not reinterpreted as success.
func (h *HubServer) broadcastRelayUnrouted(event hubJobEventPayload) {
	payload, _ := json.Marshal(event)
	h.broadcast(hubEvent{Kind: "relay.unrouted", Payload: payload, Received: h.now().UTC()})
}

func (h *HubServer) broadcastRelayUnpersisted(kind string, event hubJobEventPayload) {
	h.countUnpersistedRelayEvent()
	payload, _ := json.Marshal(struct {
		JobID  string `json:"job_id"`
		Kind   string `json:"kind"`
		Reason string `json:"reason"`
	}{JobID: event.JobID, Kind: kind, Reason: "persist_failed"})
	h.broadcast(hubEvent{Kind: "relay.unpersisted", Payload: payload, Received: h.now().UTC()})
}

// persistRelayEvent returns the handoffkeep row id. A hub without
// --handoffkeep-env returns (0, true) and behaves exactly as it did before R20.
func (h *HubServer) persistRelayEvent(kind string, event hubJobEventPayload, route reportRelayRoute) (int64, bool) {
	if h.handoffkeep == nil {
		return 0, true
	}
	request := handoffkeepRelayEventRequest{
		Kind: kind, JobID: event.JobID, Epoch: int(event.Epoch), OwnerLane: event.OwnerLane,
		Machine: route.Machine, PaneID: route.Pane, ReportPath: event.ReportPath, ReportLastLine: event.ReportLastLine,
		Question: event.Question, PR: event.PR, Head: event.Head, Reason: event.Reason,
		EventTime: h.now().UTC().Format(time.RFC3339),
	}
	stored, status, err := h.handoffkeep.appendEvent(context.Background(), request)
	if err != nil {
		h.logger.Warn("relay event was not persisted", "job", event.JobID, "kind", kind, "status", status)
		return 0, false
	}
	// owner_lane is not part of handoffkeep's idempotency key, so a duplicate
	// key raised by another lane returns that lane's row. Routing stays as this
	// hub decided it; the divergence is only worth one line of operator signal.
	if status == http.StatusOK && stored.OwnerLane != event.OwnerLane {
		h.logger.Warn("persisted relay event reports a different owner lane", "job", event.JobID, "kind", kind, "sent", event.OwnerLane, "stored", stored.OwnerLane)
	}
	return stored.ID, true
}

// markRelayEventDelivered closes the loop on a node's relay.delivered. A
// failure here is operator signal only; it must never stall the relay path.
func (h *HubServer) markRelayEventDelivered(pending relayPending) {
	if h.handoffkeep == nil || pending.eventID == 0 {
		return
	}
	if err := h.handoffkeep.markDelivered(context.Background(), pending.eventID, pending.machine, pending.pane); err != nil {
		h.logger.Warn("relay delivery was not recorded", "event_id", pending.eventID, "machine", pending.machine)
	}
}

// replayUndeliveredRelayEvents re-routes what Postgres still holds as
// undelivered. It runs once at startup and never re-persists: the row exists.
func (h *HubServer) replayUndeliveredRelayEvents(ctx context.Context) {
	if h.handoffkeep == nil {
		return
	}
	seen := make(map[int64]struct{})
	// The contract exposes no cursor, so paging stops as soon as a page adds
	// nothing new. Rows come back id-ascending, so the oldest are replayed first.
	for page := 0; page < 16; page++ {
		records, err := h.handoffkeep.listUndelivered(ctx, "", handoffkeepReplayLimit)
		if err != nil {
			h.logger.Warn("undelivered relay events could not be read at startup")
			return
		}
		added := 0
		for _, record := range records {
			if _, duplicate := seen[record.ID]; duplicate {
				continue
			}
			seen[record.ID] = struct{}{}
			added++
			h.replayRelayEvent(record)
		}
		if added == 0 || len(records) < handoffkeepReplayLimit {
			return
		}
	}
}

func (h *HubServer) replayRelayEvent(record handoffkeepRelayEvent) {
	event := hubJobEventPayload{
		JobID: record.JobID, Epoch: uint64(record.Epoch), OwnerLane: record.OwnerLane,
		ReportPath: record.ReportPath, ReportLastLine: record.ReportLastLine,
		Question: record.Question, PR: record.PR, Head: record.Head, Reason: record.Reason, PaneID: record.PaneID,
	}
	if event.OwnerLane == "" || event.JobID == "" {
		return
	}
	key := relayEventDedupeKey(record.Kind, event)
	h.mu.Lock()
	if _, exists := h.relayDedupe[key]; exists {
		h.mu.Unlock()
		return
	}
	h.relayDedupe[key] = struct{}{}
	h.mu.Unlock()
	route, agent, routed := h.resolveRelayRoute(record.Kind, event)
	if !routed {
		h.broadcastRelayUnrouted(event)
		return
	}
	h.injectRelayEvent(record.Kind, event, route, agent, record.ID)
}
