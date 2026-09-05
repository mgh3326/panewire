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

// relayDedupeKey is the hub's half of the one dedupe key shared by the node
// outbox and handoffkeep's idempotency index. All three must count the same
// five fields: an event the other two treat as distinct but the hub folds into
// an earlier one is swallowed here, and its outbox row never learns it was
// persisted.
func relayDedupeKey(completion hubJobEventPayload) string {
	return completion.JobID + "\x00" + strconv.FormatUint(completion.Epoch, 10) + "\x00" + completion.ReportPath + "\x00" + completion.Reason
}

func relayEventDedupeKey(kind string, completion hubJobEventPayload) string {
	return kind + "\x00" + relayDedupeKey(completion)
}

func (h *HubServer) relayJobCompletion(completion hubJobEventPayload) {
	h.relayJobCompletionFrom("", completion)
}

func (h *HubServer) relayJobCompletionFrom(senderMachine string, completion hubJobEventPayload) {
	h.relayJobEventFrom(senderMachine, "job.completed", completion)
}

// h.relayDedupe maps one dedupe key to the handoffkeep row it stands for. The
// id is what lets a resend be acknowledged rather than swallowed; a key that
// stands for no durable row is deleted, never left behind.
//
// relayJobEvent routes, persists, then injects. The order is the contract:
// Postgres is the canonical record of "was this reported", so injecting an
// event nobody durably stored is how a restart turns into a resend storm.
func (h *HubServer) relayJobEvent(kind string, event hubJobEventPayload) {
	h.relayJobEventFrom("", kind, event)
}

func (h *HubServer) relayJobEventFrom(senderMachine, kind string, event hubJobEventPayload) {
	if event.OwnerLane == "" || event.JobID == "" {
		return
	}
	key := relayEventDedupeKey(kind, event)
	h.mu.Lock()
	knownID, duplicate := h.relayDedupe[key]
	if !duplicate {
		h.relayDedupe[key] = 0
	}
	h.mu.Unlock()
	if duplicate {
		// A node only resends what its outbox still owes it. Returning here
		// without an answer is what leaves persisted_at NULL for good.
		h.reacknowledgeRelayEventFrom(senderMachine, kind, event, key, knownID)
		return
	}
	route, destinationAgent, routed := h.resolveRelayRoute(kind, event)
	if !routed {
		// Nothing was persisted, so the key must not outlive the attempt.
		// Keeping it would make every resend after the destination reconnects
		// look like a duplicate of a row that does not exist.
		h.forgetRelayEvent(key)
		h.broadcastRelayUnrouted(event)
		return
	}
	stored, status, persisted := h.persistRelayEventRecord(kind, event, route)
	if !persisted {
		// The event must stay resendable: the node still holds it, and its
		// next attempt has to survive this hub's in-memory dedupe.
		h.forgetRelayEvent(key)
		h.broadcastRelayUnpersisted(kind, event)
		return
	}
	h.rememberRelayEvent(key, stored.ID)
	if relayEventAlreadyDelivered(status, stored) {
		// handoffkeep already holds this record as delivered, so the note is in
		// the pane. The node still owes its outbox row an answer: acknowledge,
		// and put the injection nobody needs on the operator feed instead.
		h.queueRelayPersisted(kind, event, h.relayPersistedAgent(senderMachine, destinationAgent), stored.ID)
		h.broadcastRelayAlreadyDelivered(kind, event, stored)
		return
	}
	h.injectRelayEvent(kind, event, route, destinationAgent, h.relayPersistedAgent(senderMachine, destinationAgent), stored.ID, key)
}

// relayEventAlreadyDelivered is the hub's inject gate. Its authority is
// handoffkeep's delivered_at on a row that already existed (200), never the
// node's `replay` flag: `replay` says a node restarted, which is not the same
// question as whether this record ever reached the pane. 201 is a row nothing
// can have delivered yet, so it always injects.
func relayEventAlreadyDelivered(status int, stored handoffkeepRelayEvent) bool {
	return status == http.StatusOK && strings.TrimSpace(stored.DeliveredAt) != ""
}

// reacknowledgeRelayEvent answers a resend of an event this hub already took.
// The row is in Postgres, so the node is owed relay.persisted and nothing
// else: re-injecting would put the same note in the pane twice, and staying
// silent would keep the node resending it after every restart forever.
func (h *HubServer) reacknowledgeRelayEvent(kind string, event hubJobEventPayload, key string, knownID int64) {
	h.reacknowledgeRelayEventFrom("", kind, event, key, knownID)
}

func (h *HubServer) reacknowledgeRelayEventFrom(senderMachine, kind string, event hubJobEventPayload, key string, knownID int64) {
	if h.handoffkeep == nil {
		// A hub with no durable record has no acknowledgement to give. This
		// is the pre-R20 behavior the compatibility path depends on.
		return
	}
	route, destinationAgent, routed := h.resolveRelayRoute(kind, event)
	if !routed {
		h.forgetRelayEvent(key)
		h.broadcastRelayUnrouted(event)
		return
	}
	// A re-POST is the only attempt counter the contract exposes: the
	// idempotency key collides and handoffkeep returns 200 with the row.
	if eventID, persisted := h.persistRelayEvent(kind, event, route); persisted && eventID != 0 {
		knownID = eventID
		h.rememberRelayEvent(key, eventID)
	} else if !persisted && knownID == 0 {
		// Neither this hub nor handoffkeep can name the row, so the key is
		// holding back a resend for nothing.
		h.forgetRelayEvent(key)
		h.broadcastRelayUnpersisted(kind, event)
		return
	}
	if knownID == 0 {
		return
	}
	h.queueRelayPersisted(kind, event, h.relayPersistedAgent(senderMachine, destinationAgent), knownID)
}

// rememberRelayEvent records the handoffkeep row a dedupe key stands for, so a
// later resend can be acknowledged instead of dropped.
func (h *HubServer) rememberRelayEvent(key string, eventID int64) {
	if eventID == 0 {
		return
	}
	h.mu.Lock()
	h.relayDedupe[key] = eventID
	h.mu.Unlock()
}

// forgetRelayEvent releases a dedupe key that stands for no durable row.
func (h *HubServer) forgetRelayEvent(key string) {
	h.mu.Lock()
	delete(h.relayDedupe, key)
	h.mu.Unlock()
}

// queueRelayPersisted tells a node its outbox row may be retired. The node
// keys the row by exactly these five fields.
func (h *HubServer) queueRelayPersisted(kind string, event hubJobEventPayload, agent *hubAgent, eventID int64) bool {
	if eventID == 0 || agent == nil {
		return false
	}
	return agent.queuePersisted(hubRelayPersistedEvent{Type: "relay.persisted", JobID: event.JobID, Kind: kind, Epoch: event.Epoch, ReportPath: event.ReportPath, Reason: event.Reason, EventID: eventID})
}

func (h *HubServer) relayPersistedAgent(senderMachine string, fallback *hubAgent) *hubAgent {
	if senderMachine == "" {
		return fallback
	}
	h.mu.Lock()
	sender := h.nodes[senderMachine]
	h.mu.Unlock()
	if sender == nil || sender.agent == nil {
		// The hub retains no acknowledgement state. A disconnected sender
		// resends after reconnecting, then this path addresses its new agent.
		return nil
	}
	return sender.agent
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

// injectRelayEvent reports whether the injection was queued.
func (h *HubServer) injectRelayEvent(kind string, event hubJobEventPayload, route reportRelayRoute, destinationAgent, persistedAgent *hubAgent, eventID int64, key string) bool {
	if !destinationAgent.queueRelay(hubRelayInjectEvent{Type: "relay.inject", JobID: event.JobID, Pane: route.Pane, Text: relayTextForKind(kind, event)}) {
		// The row is durable but this injection never happened. Holding the
		// key back would swallow the node's resend, and withholding the
		// acknowledgement would keep that resend coming forever. Do neither:
		// the row exists, so re-injection belongs to the undelivered replay.
		h.forgetRelayEvent(key)
		h.queueRelayPersisted(kind, event, persistedAgent, eventID)
		h.broadcastRelayUnrouted(event)
		return false
	}
	h.startRelayAckEvent(kind, event, route.Machine, route.Pane, eventID)
	// The node retires its outbox row on this, not on the injection itself.
	h.queueRelayPersisted(kind, event, persistedAgent, eventID)
	return true
}

// An unrouted/temporarily disconnected target remains observable to the
// operator event feed. It is deliberately not reinterpreted as success.
func (h *HubServer) broadcastRelayUnrouted(event hubJobEventPayload) {
	payload, _ := json.Marshal(event)
	h.broadcast(hubEvent{Kind: "relay.unrouted", Payload: payload, Received: h.now().UTC()})
}

// broadcastRelayReplayExhausted keeps a row the hub has stopped replaying
// visible. Dropping it silently is how a stuck record turns into an
// unexplained gap between Postgres and the pane.
func (h *HubServer) broadcastRelayReplayExhausted(record handoffkeepRelayEvent) {
	h.countReplayExhaustedEvent()
	payload, _ := json.Marshal(struct {
		JobID    string `json:"job_id"`
		Kind     string `json:"kind"`
		EventID  int64  `json:"event_id"`
		Attempts int    `json:"attempts"`
		Reason   string `json:"reason"`
	}{JobID: record.JobID, Kind: record.Kind, EventID: record.ID, Attempts: record.Attempts, Reason: "attempts_exhausted"})
	h.broadcast(hubEvent{Kind: "relay.replay_exhausted", Payload: payload, Received: h.now().UTC()})
}

// broadcastRelayAlreadyDelivered keeps a suppressed injection visible. An
// operator who sees the note absent from the pane must be able to tell "the
// hub decided it was already there" from "the relay lost it".
func (h *HubServer) broadcastRelayAlreadyDelivered(kind string, event hubJobEventPayload, stored handoffkeepRelayEvent) {
	h.countAlreadyDeliveredRelayEvent()
	payload, _ := json.Marshal(struct {
		JobID       string `json:"job_id"`
		Kind        string `json:"kind"`
		EventID     int64  `json:"event_id"`
		DeliveredAt string `json:"delivered_at"`
		Reason      string `json:"reason"`
	}{JobID: event.JobID, Kind: kind, EventID: stored.ID, DeliveredAt: stored.DeliveredAt, Reason: "already_delivered"})
	h.broadcast(hubEvent{Kind: "relay.already_delivered", Payload: payload, Received: h.now().UTC()})
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

// relayEventRequest builds the one request body every handoffkeep write uses.
// A first write, a resend, and an attempt bump must be byte-identical in the
// five fields the idempotency key reads, or a bump would mint a second row.
func (h *HubServer) relayEventRequest(kind string, event hubJobEventPayload, route reportRelayRoute) handoffkeepRelayEventRequest {
	return handoffkeepRelayEventRequest{
		Kind: kind, JobID: event.JobID, Epoch: int(event.Epoch), OwnerLane: event.OwnerLane,
		Machine: route.Machine, PaneID: route.Pane, ReportPath: event.ReportPath, ReportLastLine: event.ReportLastLine,
		Question: event.Question, PR: event.PR, Head: event.Head, Reason: event.Reason,
		EventTime: h.now().UTC().Format(time.RFC3339),
	}
}

// bumpRelayEventAttempts records one spent delivery attempt. handoffkeep
// exposes no counter endpoint, so the hub re-POSTs the row it already stored:
// the idempotency key collides, the reply is 200 with attempts+1, and
// delivered_at is not touched by that path.
func (h *HubServer) bumpRelayEventAttempts(kind string, event hubJobEventPayload, route reportRelayRoute) (int, bool) {
	if h.handoffkeep == nil {
		return 0, false
	}
	stored, status, err := h.handoffkeep.appendEvent(context.Background(), h.relayEventRequest(kind, event, route))
	if err != nil {
		h.logger.Warn("relay delivery attempt was not recorded", "job", event.JobID, "kind", kind, "status", status)
		return 0, false
	}
	return stored.Attempts, true
}

// recordRelayAttempt is the ack-timeout half of the same counter: an injection
// nobody confirmed is a spent attempt, and the startup replay gate reads it.
func (h *HubServer) recordRelayAttempt(pending relayPending) {
	if h.handoffkeep == nil || pending.eventID == 0 || pending.kind == "" {
		return
	}
	h.bumpRelayEventAttempts(pending.kind, pending.event, reportRelayRoute{Machine: pending.machine, Pane: pending.pane})
}

// persistRelayEvent returns the handoffkeep row id. A hub without
// --handoffkeep-env returns (0, true) and behaves exactly as it did before R20.
// 201 (new row) and 200 (the existing row for this idempotency key) are both
// success, so a resend is acknowledged exactly like a first send.
func (h *HubServer) persistRelayEvent(kind string, event hubJobEventPayload, route reportRelayRoute) (int64, bool) {
	stored, _, persisted := h.persistRelayEventRecord(kind, event, route)
	return stored.ID, persisted
}

// persistRelayEventRecord returns handoffkeep's own row and reply status
// alongside the id, because the id alone cannot tell a first write from a row
// the parent pane already received.
func (h *HubServer) persistRelayEventRecord(kind string, event hubJobEventPayload, route reportRelayRoute) (handoffkeepRelayEvent, int, bool) {
	if h.handoffkeep == nil {
		return handoffkeepRelayEvent{}, 0, true
	}
	stored, status, err := h.handoffkeep.appendEvent(context.Background(), h.relayEventRequest(kind, event, route))
	if err != nil {
		h.logger.Warn("relay event was not persisted", "job", event.JobID, "kind", kind, "status", status)
		return handoffkeepRelayEvent{}, status, false
	}
	// owner_lane is not part of handoffkeep's idempotency key, so a duplicate
	// key raised by another lane returns that lane's row. Routing stays as this
	// hub decided it; the divergence is only worth one line of operator signal.
	if status == http.StatusOK && stored.OwnerLane != event.OwnerLane {
		h.logger.Warn("persisted relay event reports a different owner lane", "job", event.JobID, "kind", kind, "sent", event.OwnerLane, "stored", stored.OwnerLane)
	}
	return stored, status, true
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
	// The query asks for undelivered rows, but the gate is the hub's own: a
	// row that has been delivered, or has already spent its attempts, is the
	// difference between a replay and a re-injection storm on every restart.
	if record.DeliveredAt != "" {
		return
	}
	if record.Attempts >= relayReplayMaxAttempts {
		h.broadcastRelayReplayExhausted(record)
		return
	}
	key := relayEventDedupeKey(record.Kind, event)
	h.mu.Lock()
	if _, exists := h.relayDedupe[key]; exists {
		h.mu.Unlock()
		return
	}
	h.relayDedupe[key] = record.ID
	h.mu.Unlock()
	route, agent, routed := h.resolveRelayRoute(record.Kind, event)
	if !routed {
		h.forgetRelayEvent(key)
		h.broadcastRelayUnrouted(event)
		return
	}
	// handoffkeep replay records the destination machine, not the original
	// sender. Do not acknowledge that destination; the sender's resend does so.
	if !h.injectRelayEvent(record.Kind, event, route, agent, nil, record.ID, key) {
		return
	}
	// The replay just spent an attempt. Recording it is what makes the gate
	// above converge instead of replaying the same row after every restart.
	h.bumpRelayEventAttempts(record.Kind, event, route)
}
