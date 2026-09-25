package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	lanePersistedMaxEntries        = 4096
	relayReplayExhaustedMaxEntries = 4096
	relayReplayRetiredMaxEntries   = 4096
	relayCancelledMaxEntries       = 4096
)

// reportRelayRoutes is intentionally a tiny operator-owned configuration:
// routes contain identifiers only, never host addresses, tokens, or panes
// from a particular installation.
type reportRelayRoutes struct {
	Routes map[string]reportRelayRoute `json:"routes"`
	Lanes  map[string]reportRelayRoute `json:"lanes"`
	// Control stays raw here so a control block the hub cannot make sense of
	// never costs it the lane routes beside it. Both readers decode it
	// separately, and only the write path treats a failure as fatal.
	Control json.RawMessage `json:"control,omitempty"`
}

// reportRelayStandby is the optional alternate pane kept with a lane route
// for an external failover controller to swap into the primary destination.
type reportRelayStandby struct {
	Machine string `json:"machine"`
	Pane    string `json:"pane"`
}

type reportRelayRoute struct {
	Machine   string              `json:"machine"`
	Pane      string              `json:"pane"`
	Parent    string              `json:"parent,omitempty"`
	Sink      bool                `json:"sink,omitempty"`
	Deliver   string              `json:"deliver,omitempty"`
	Protected bool                `json:"protected,omitempty"`
	Standby   *reportRelayStandby `json:"standby,omitempty"`
}

var errReportRelayRoutesInvalid = errors.New("report relay routes invalid")

// loadReportRelayRoutes preserves the established best-effort relay behavior:
// absent, unreadable, oversized, and invalid files all produce no routes.
func loadReportRelayRoutes(path string) map[string]reportRelayRoute {
	routes, _ := loadReportRelayRoutesResult(path)
	return routes
}

// loadReportRelayRoutesResult is the shared R19 lanes loader. Read failures
// remain an empty result, while malformed or oversized file contents are
// reported so the operator HTTP projection can distinguish them.
func loadReportRelayRoutesResult(path string) (map[string]reportRelayRoute, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil
	}
	if len(b) > lanesFileMaxBytes {
		return nil, errReportRelayRoutesInvalid
	}
	return parseReportRelayRoutes(b)
}

func parseReportRelayRoutes(b []byte) (map[string]reportRelayRoute, error) {
	var routes reportRelayRoutes
	if err := json.Unmarshal(b, &routes); err != nil {
		return nil, err
	}
	// lanes is the R19 contract. Keep routes as a deliberate compatibility
	// reader for installations that have not renamed their operator file yet.
	if routes.Lanes != nil {
		routes.Routes = routes.Lanes
	}
	for lane, route := range routes.Routes {
		if !validReportRelayLaneName(lane) {
			delete(routes.Routes, lane)
			continue
		}
		if route.Parent != "" && !validReportRelayLaneName(route.Parent) {
			delete(routes.Routes, lane)
			continue
		}
		// A sink is an operator-only durable destination. An empty pane is the
		// backwards-compatible sink spelling; explicit sink wins over supplied
		// transport fields and is never eligible for pane injection.
		if route.Sink || strings.TrimSpace(route.Pane) == "" {
			route.Sink, route.Machine, route.Pane, route.Standby = true, "", "", nil
			routes.Routes[lane] = route
			continue
		}
		if !machineIDPattern.MatchString(route.Machine) || len(route.Pane) > 128 || (route.Standby != nil && !validReportRelayStandby(*route.Standby)) || (route.Deliver != "" && !validRelayDeliver(route.Deliver)) {
			delete(routes.Routes, lane)
		}
	}
	return routes.Routes, nil
}

// validReportRelayLaneName accepts both the established relay label spelling
// and every lane name the R28 write API is required to persist. Keeping the
// union prevents a successful write from becoming invisible to the hot loader
// while retaining compatibility with operator files created before R28.
func validReportRelayLaneName(value string) bool {
	return hubAgentLabelPattern.MatchString(value) || laneNamePattern.MatchString(value)
}

func validRelayDeliver(value string) bool {
	_, valid := parseRelayDeliveryPolicy(value)
	return valid
}

func validReportRelayStandby(standby reportRelayStandby) bool {
	return machineIDPattern.MatchString(standby.Machine) && strings.TrimSpace(standby.Pane) != "" && len(standby.Pane) <= 128
}

func relayText(completion hubJobEventPayload) string {
	return relayTextForKind("job.completed", completion)
}

func relayTextForKind(kind string, event hubJobEventPayload) string {
	if kind == "lane.event" {
		return "(같은 내용이 두 번 보이면 재실행 금지) [event] " + event.OwnerLane + " :: " + event.Text
	}
	if kind == "job.escalate" {
		return escalationRelayText(event)
	}
	if kind == "job.joined" {
		head := truncateRelayText(event.Head, 9)
		return boundRelayText("[joined] "+truncateRelayText(event.Label, 120)+" :: PR "+truncateRelayText(event.PR, 120)+" @ "+head+" → ", event.ReportPath)
	}
	if kind == "job.lost" || kind == "job.revoked" {
		tag := "[lost] "
		if kind == "job.revoked" {
			tag = "[revoked] "
		}
		return boundRelayText(tag+truncateRelayText(event.Label, 64)+" ("+truncateRelayText(event.Host, 64)+") :: "+truncateRelayText(event.Reason, 200)+" → ", event.ReportPath)
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

// relayDedupeKey is the five-field body shared by handoffkeep's idempotency
// index. The hub's own dedupe key and the node outbox append the producer's
// event identity on top: an event the hub folds into an earlier round is
// swallowed here, and its outbox row never learns it was persisted.
func relayDedupeKey(completion hubJobEventPayload) string {
	return completion.JobID + "\x00" + strconv.FormatUint(completion.Epoch, 10) + "\x00" + completion.ReportPath + "\x00" + completion.Reason
}

func relayEventDedupeKey(kind string, completion hubJobEventPayload) string {
	if kind == "lane.event" {
		return relayLaneEventOutboxKey(completion.OwnerLane, completion.EventID)
	}
	// The producer's event_id distinguishes one event file from the same job's
	// next round even though handoffkeep's five-field index folds them into a
	// single durable row. Events from old producers carry no event_id and keep
	// the historic key.
	return kind + "\x00" + relayDedupeKey(completion) + "\x00" + completion.EventID
}

func (h *HubServer) relayJobCompletion(completion hubJobEventPayload) {
	h.relayJobCompletionFrom("", completion)
}

func (h *HubServer) relayJobCompletionFrom(senderMachine string, completion hubJobEventPayload) {
	h.relayJobEventFrom(senderMachine, "job.completed", completion)
}

// relayLaneEventResult is the HTTP ingress view of the existing lane-event
// relay state machine. Node callers intentionally ignore it.
type relayLaneEventResult struct {
	ID              int64
	Routed          bool
	Machine         string
	Duplicate       bool
	PersistFailed   bool
	RejectedTooLong bool
	// AlreadyDelivered separates "the durable row exists and was delivered
	// before" from "the durable row exists and is still owed to a lane": the
	// chat outbox treats the first as terminal and the second as queued.
	AlreadyDelivered bool
}

// relayLaneEvent follows the R20 persistence cursor but has intentionally
// different routing failure semantics from job.*: no route is still a durable
// handoffkeep row, and a sending node is acknowledged once that row exists.
// A nil sender is the authenticated HTTP ingress: it uses the same durable
// and injection machinery, but has no producer node to acknowledge.
func (h *HubServer) relayLaneEvent(event hubJobEventPayload, sender *hubAgent) relayLaneEventResult {
	ingress := sender == nil
	if event.OwnerLane == "" || event.EventID == "" || event.Text == "" {
		return relayLaneEventResult{}
	}
	key := relayEventDedupeKey("lane.event", event)
	h.mu.Lock()
	persistedSHA := h.laneEventSHALocked(key)
	if persistedID := h.lanePersistedIDLocked(key); persistedID != 0 {
		h.mu.Unlock()
		// The hub already owns the durable row. A source retry is an ACK-loss
		// recovery, not another delivery attempt, so it must not re-POST and
		// consume the delivery budget. The one thing it must still do is say
		// when the resent body disagrees with the row that stands (#725): a
		// same-identity different-payload arrival is a mismatch, not a quiet
		// duplicate.
		h.noteLaneEventPayloadMismatch(event, persistedSHA)
		if !ingress {
			h.queueLaneRelayPersisted(event, sender, persistedID)
			return relayLaneEventResult{ID: persistedID}
		}
		return relayLaneEventResult{ID: persistedID, Duplicate: true}
	}
	knownID, duplicate := h.relayDedupe[key]
	if !duplicate {
		h.relayDedupe[key] = 0
	}
	h.mu.Unlock()
	if duplicate {
		h.noteLaneEventPayloadMismatch(event, persistedSHA)
		if ingress {
			// A concurrent request can see a claim before the first POST has
			// learned its durable id. It is still a duplicate; a retry can name
			// the id once the first request finishes.
			return relayLaneEventResult{ID: knownID, Duplicate: true}
		}
		if knownID != 0 {
			h.queueLaneRelayPersisted(event, sender, knownID)
		}
		return relayLaneEventResult{ID: knownID}
	}
	if h.handoffkeep == nil {
		h.forgetRelayEvent(key)
		h.broadcastRelayUnpersisted("lane.event", event)
		return relayLaneEventResult{PersistFailed: true}
	}
	route, target, routed := h.resolveRelayRoute("lane.event", event)
	if event.sinkOnly && !route.Sink {
		h.forgetRelayEvent(key)
		return relayLaneEventResult{}
	}
	if len(event.Text) > laneEventTextLimitSink || (!route.Sink && len(event.Text) > laneEventTextLimit) {
		h.forgetRelayEvent(key)
		h.broadcastRelayRejected(event, "text_too_long")
		return relayLaneEventResult{RejectedTooLong: true}
	}
	persistRoute := route
	if ingress {
		// HTTP has no producer node. Its supplied/default hub host describes
		// the durable source record; the resolved route remains injection-only.
		persistRoute = reportRelayRoute{Machine: event.Host, Pane: ""}
	}
	stored, status, persisted := h.persistRelayEventRecord("lane.event", event, persistRoute)
	if !persisted {
		h.logger.Error("lane.event persistence failed; handoffkeep schema v7 must be deployed before this hub", "lane", event.OwnerLane, "producer_event_id", event.EventID, "status", status)
		h.forgetRelayEvent(key)
		h.broadcastRelayUnpersisted("lane.event", event)
		return relayLaneEventResult{PersistFailed: true}
	}
	// #725: the durable text now names this identity for the rest of the
	// process, so every future resend can be compared against it — including
	// the resends that never reach a second POST.
	h.rememberLaneEventSHA(key, stored.Text)
	if status == http.StatusOK && stored.Text != event.Text {
		h.noteLaneEventPayloadMismatch(event, relayPayloadFingerprint(stored.Text))
	}
	if ingress && status == http.StatusOK {
		// A restarted hub discovers a durable duplicate only after its first
		// POST. Do not inject it here: replay owns an undelivered row. Retain
		// the id for future 409s, then release the active claim for replay.
		h.rememberLanePersisted(key, stored.ID)
		h.forgetRelayEvent(key)
		return relayLaneEventResult{ID: stored.ID, Duplicate: true}
	}
	// handoffkeep is first-writer-wins. A hub that restarted after the first
	// POST must inject the returned durable text, not a changed duplicate body.
	if status == http.StatusOK {
		event = laneEventFromStored(stored)
	}
	h.rememberLanePersisted(key, stored.ID)
	h.rememberRelayEvent(key, stored.ID)
	if event.Truncated {
		h.broadcastRelayTruncated(event)
	}
	// Delivery responsibility has moved to the hub at this point. This ACK is
	// deliberately sent to the producer's node, never the destination pane.
	if !ingress {
		h.queueLaneRelayPersisted(event, sender, stored.ID)
	}
	if relayEventAlreadyDelivered(status, stored, event) {
		h.broadcastRelayAlreadyDelivered("lane.event", event, stored)
		return relayLaneEventResult{ID: stored.ID, AlreadyDelivered: true}
	}
	if route.Sink {
		if err := h.handoffkeep.markDelivered(context.Background(), stored.ID, "sink", "sink:"+event.OwnerLane); err != nil {
			h.logger.Warn("sink relay delivery was not recorded", "event_id", stored.ID, "lane", event.OwnerLane)
			// The durable row remains undelivered for operator observation, but a
			// sink never falls back to a pane injection.
			h.forgetRelayEvent(key)
			h.broadcastRelayUnrouted(event)
			return relayLaneEventResult{ID: stored.ID}
		}
		return relayLaneEventResult{ID: stored.ID, Routed: true, Machine: "sink"}
	}
	if !routed {
		// The durable row remains for replay. The active injection claim is
		// released, while lanePersisted retains its row ID so producer retries
		// are acknowledged without re-POSTing.
		h.forgetRelayEvent(key)
		h.broadcastRelayUnrouted(event)
		return relayLaneEventResult{ID: stored.ID}
	}
	if match := chatRelayEventPattern.FindStringSubmatch(event.EventID); match != nil {
		// A chat row can leave "stored" while this relay is in flight — the
		// operator cancelled it, or a concurrent path closed it. The store is
		// consulted once more at the last moment so a cancelled directive can
		// never be injected after the cancel answered. On a store read error
		// the row stays queued for replay rather than being injected blind.
		if chatID, err := strconv.ParseInt(match[1], 10, 64); err == nil && h.chatReplayDisposition(chatID) != "inject" {
			if err := h.handoffkeep.markDelivered(context.Background(), stored.ID, "hub", "chat-terminal"); err != nil {
				h.logger.Warn("terminal chat relay row was not retired", "event_id", stored.ID)
			}
			h.forgetRelayEvent(key)
			return relayLaneEventResult{ID: stored.ID}
		}
	}
	if !h.injectLaneRelayEvent(event, route, target, stored.ID, key) {
		return relayLaneEventResult{ID: stored.ID}
	}
	return relayLaneEventResult{ID: stored.ID, Routed: true, Machine: route.Machine}
}

func laneEventFromStored(record handoffkeepRelayEvent) hubJobEventPayload {
	return hubJobEventPayload{
		JobID:     laneEventTransportID(record.OwnerLane, record.EventID),
		Epoch:     uint64(record.Epoch),
		OwnerLane: record.OwnerLane,
		EventID:   record.EventID,
		Text:      record.Text,
	}
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
	if relayEventAlreadyDelivered(status, stored, event) {
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
//
// handoffkeep's job.* idempotency index is the five-field key, so a later
// round for the same job folds into the first round's row and reports 200 for
// an event it never stored. A 200 only proves THIS event was delivered when
// the stored row names the same producer event identity; a folded row (or a
// legacy row written before event_id existed) cannot answer for the new
// event, so its note still goes to the pane.
func relayEventAlreadyDelivered(status int, stored handoffkeepRelayEvent, event hubJobEventPayload) bool {
	return status == http.StatusOK && strings.TrimSpace(stored.DeliveredAt) != "" && stored.EventID == event.EventID
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

// rememberLanePersisted retains a durable lane row independently from the
// active injection claim. The latter is released after a route miss or ACK
// timeout so replay can try again; retaining this ID keeps producer resends
// from being miscounted as delivery attempts in the meantime.
func (h *HubServer) rememberLanePersisted(key string, eventID int64) {
	if eventID == 0 {
		return
	}
	h.mu.Lock()
	h.rememberLanePersistedLocked(key, eventID)
	h.mu.Unlock()
}

func (h *HubServer) lanePersistedIDLocked(key string) int64 {
	persistedID := h.lanePersisted[key]
	if persistedID != 0 {
		h.lanePersistedOrder.touch(key, lanePersistedMaxEntries)
	}
	return persistedID
}

// laneEventSHALocked returns the durable payload fingerprint recorded for a
// lane.event dedupe key, or "" while the durable row's text is still unknown
// (a first POST in flight). The map survives the lifecycle cleanups around it
// — delivery forgets lanePersisted but must not forget the fingerprint,
// because the resend that needs comparing is the post-delivery one.
func (h *HubServer) laneEventSHALocked(key string) string {
	sha := h.laneEventSHA[key]
	if sha != "" {
		h.laneEventSHAOrder.touch(key, lanePersistedMaxEntries)
	}
	return sha
}

func (h *HubServer) rememberLaneEventSHA(key, text string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.laneEventSHA == nil {
		h.laneEventSHA = make(map[string]string)
	}
	_, evicted, overflowed := h.laneEventSHAOrder.touch(key, lanePersistedMaxEntries)
	if overflowed {
		// Eviction can only lose a future mismatch observation; dedupe and
		// delivery never depend on the fingerprint's presence.
		delete(h.laneEventSHA, evicted)
	}
	h.laneEventSHA[key] = relayPayloadFingerprint(text)
}

// noteLaneEventPayloadMismatch is the contract's mismatch clause: the same
// (lane, event_id) arriving with a different payload is recorded and
// broadcast, never silently absorbed into the durable duplicate path.
func (h *HubServer) noteLaneEventPayloadMismatch(event hubJobEventPayload, storedSHA string) {
	if storedSHA == "" || relayPayloadFingerprint(event.Text) == storedSHA {
		return
	}
	h.countRelayPayloadMismatch()
	h.logger.Warn("lane.event resent with a different payload; durable row still wins", "lane", event.OwnerLane, "event_id", event.EventID)
	payload, _ := json.Marshal(struct {
		Lane    string `json:"lane"`
		EventID string `json:"event_id"`
		Reason  string `json:"reason"`
	}{Lane: event.OwnerLane, EventID: event.EventID, Reason: "payload_mismatch"})
	h.broadcast(hubEvent{Kind: "relay.mismatch", Payload: payload, Received: h.now().UTC()})
}

func (h *HubServer) rememberLanePersistedLocked(key string, eventID int64) {
	if h.lanePersisted == nil {
		h.lanePersisted = make(map[string]int64)
	}
	_, evicted, overflowed := h.lanePersistedOrder.touch(key, lanePersistedMaxEntries)
	if overflowed {
		// An evicted producer resend re-POSTs to handoffkeep. Its idempotency
		// key is first-writer-wins, so handoffkeep returns the same durable row:
		// this trades one POST for bounded memory, not delivery loss.
		delete(h.lanePersisted, evicted)
	}
	h.lanePersisted[key] = eventID
}

func (h *HubServer) forgetLanePersistedLocked(key string) {
	delete(h.lanePersisted, key)
	h.lanePersistedOrder.forget(key)
}

// forgetRelayEvent releases a dedupe key that stands for no durable row.
func (h *HubServer) forgetRelayEvent(key string) {
	h.mu.Lock()
	delete(h.relayDedupe, key)
	h.mu.Unlock()
}

// queueRelayPersisted tells a node its outbox row may be retired. The node
// keys the row by the five shared fields plus the producer's event identity,
// which is echoed back so an acknowledgement names exactly one event's row.
func (h *HubServer) queueRelayPersisted(kind string, event hubJobEventPayload, agent *hubAgent, eventID int64) bool {
	if eventID == 0 || agent == nil {
		return false
	}
	return agent.queuePersisted(hubRelayPersistedEvent{Type: "relay.persisted", JobID: event.JobID, Kind: kind, Epoch: event.Epoch, ReportPath: event.ReportPath, Reason: event.Reason, EventID: eventID, ProducerEventID: event.EventID})
}

// relayPersistedAgent selects the actual producer when this arrived over a
// node connection. Direct unit-test and internal callers without sender
// identity retain the historical destination fallback. A disconnected sender
// receives no speculative acknowledgement; its resend after reconnecting is
// addressed to its newly connected agent.
func (h *HubServer) relayPersistedAgent(senderMachine string, fallback *hubAgent) *hubAgent {
	if senderMachine == "" {
		return fallback
	}
	h.mu.Lock()
	sender := h.nodes[senderMachine]
	h.mu.Unlock()
	if sender == nil || sender.agent == nil {
		return nil
	}
	return sender.agent
}

func (h *HubServer) queueLaneRelayPersisted(event hubJobEventPayload, agent *hubAgent, eventID int64) bool {
	if agent == nil || eventID == 0 {
		return false
	}
	return agent.queuePersisted(hubRelayPersistedEvent{Type: "relay.persisted", JobID: event.JobID, Kind: "lane.event", Epoch: event.Epoch, EventID: eventID, Lane: event.OwnerLane, ProducerEventID: event.EventID})
}

func (h *HubServer) resolveRelayRoute(kind string, event hubJobEventPayload) (reportRelayRoute, *hubAgent, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	routes := loadReportRelayRoutes(h.reportRelayPath)
	route, exists := routes[event.OwnerLane]
	// Escalation, join and revocation are upward reports: they reach the owner
	// lane's parent, the way a builder's joined reaches its director. job.lost
	// is an observation for the owner itself and stays on the owner route.
	if exists && (kind == "job.escalate" || kind == "job.joined" || kind == "job.revoked") {
		parent, parentExists := routes[route.Parent]
		if route.Parent == "" || !parentExists {
			exists = false
		} else {
			route = parent
		}
	}
	if exists && route.Sink && kind == "lane.event" {
		return route, nil, true
	}
	var agent *hubAgent
	if exists && h.nodes[route.Machine] != nil {
		agent = h.nodes[route.Machine].agent
	}
	return route, agent, exists && agent != nil
}

// injectRelayEvent reports whether the injection was queued.
func (h *HubServer) injectRelayEvent(kind string, event hubJobEventPayload, route reportRelayRoute, destinationAgent, persistedAgent *hubAgent, eventID int64, key string) bool {
	h.registerRelayAckEvent(kind, event, route.Machine, route.Pane, eventID)
	if !destinationAgent.queueRelay(relayInjectDirective(kind, event, route, eventID)) {
		// The row is durable but this injection never happened. Holding the
		// key back would swallow the node's resend, and withholding the
		// acknowledgement would keep that resend coming forever. Do neither:
		// the row exists, so re-injection belongs to the undelivered replay.
		h.forgetRelayEvent(key)
		h.forgetRelayAckEvent(eventID, event.JobID)
		h.queueRelayPersisted(kind, event, persistedAgent, eventID)
		h.broadcastRelayUnrouted(event)
		return false
	}
	h.armRelayAckEvent(eventID, event.JobID)
	// The node retires its outbox row on this, not on the injection itself.
	h.queueRelayPersisted(kind, event, persistedAgent, eventID)
	return true
}

func (h *HubServer) injectLaneRelayEvent(event hubJobEventPayload, route reportRelayRoute, agent *hubAgent, eventID int64, key string) bool {
	h.registerRelayAckEvent("lane.event", event, route.Machine, route.Pane, eventID)
	if !agent.queueRelay(relayInjectDirective("lane.event", event, route, eventID)) {
		h.forgetRelayEvent(key)
		h.forgetRelayAckEvent(eventID, event.JobID)
		h.broadcastRelayUnrouted(event)
		return false
	}
	h.armRelayAckEvent(eventID, event.JobID)
	return true
}

func relayInjectDirective(kind string, event hubJobEventPayload, route reportRelayRoute, eventID int64) hubRelayInjectEvent {
	if eventID == 0 {
		// Pre-R20/no-handoffkeep operation has no durable identity and therefore
		// cannot hold safely. Keep its established wire shape and fail-open
		// delivery behavior rather than emitting an unparseable hybrid message.
		directive := hubRelayInjectEvent{Type: "relay.inject", JobID: event.JobID, Pane: route.Pane, Text: relayTextForKind(kind, event)}
		if kind == "lane.event" {
			directive.Kind = kind
		}
		return directive
	}
	deliver := event.DeliverPolicy
	if deliver == "" {
		deliver = route.Deliver
	}
	if deliver == "" {
		deliver = "idle"
	}
	return hubRelayInjectEvent{Type: "relay.inject", Kind: kind, JobID: event.JobID, Pane: route.Pane, Text: relayTextForKind(kind, event), Lane: event.OwnerLane, EventID: eventID, Deliver: deliver}
}

// An unrouted/temporarily disconnected target remains observable to the
// operator event feed. It is deliberately not reinterpreted as success.
func (h *HubServer) broadcastRelayUnrouted(event hubJobEventPayload) {
	payload, _ := json.Marshal(event)
	h.broadcast(hubEvent{Kind: "relay.unrouted", Payload: payload, Received: h.now().UTC()})
}

func (h *HubServer) broadcastRelayRejected(event hubJobEventPayload, reason string) {
	event.Reason = reason
	payload, _ := json.Marshal(event)
	h.broadcast(hubEvent{Kind: "relay.rejected", Payload: payload, Received: h.now().UTC()})
}

func (h *HubServer) broadcastRelayTruncated(event hubJobEventPayload) {
	payload, _ := json.Marshal(struct {
		Lane    string `json:"lane"`
		EventID string `json:"event_id"`
	}{Lane: event.OwnerLane, EventID: event.EventID})
	h.broadcast(hubEvent{Kind: "relay.truncated", Payload: payload, Received: h.now().UTC()})
}

// broadcastRelayReplayExhausted keeps a row the hub has stopped replaying
// visible. Dropping it silently is how a stuck record turns into an
// unexplained gap between Postgres and the pane.
func (h *HubServer) broadcastRelayReplayExhausted(record handoffkeepRelayEvent) {
	if !h.countReplayExhaustedEvent(record.ID) {
		return
	}
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
		Question: event.Question, PR: event.PR, Head: event.Head, Reason: event.Reason, EventID: event.EventID, Text: event.Text,
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
	// event_id is not part of the job.* idempotency key either, so a new round
	// folds into the first round's row. The folded row is still the durable
	// receipt this event is acknowledged by, but the mismatch is how a dropped
	// notification would otherwise hide.
	if status == http.StatusOK && stored.EventID != event.EventID {
		h.logger.Warn("relay event folded into a different event's durable row", "job", event.JobID, "kind", kind, "sent", event.EventID, "stored", stored.EventID)
	}
	return stored, status, true
}

// markRelayEventDelivered closes the loop on a node's relay.delivered. A
// failure here is operator signal only; it must never stall the relay path.
// It reports whether delivered_at was written (or there is no durable row).
func (h *HubServer) markRelayEventDelivered(pending relayPending) bool {
	// Cleanup belongs to the successful relay.delivered acknowledgement, not
	// to handoffkeep. Pre-R20 deployments still need bounded local state.
	h.mu.Lock()
	h.r19a.forgetRelayTimeout(pending.event.JobID)
	if pending.kind == "lane.event" {
		h.forgetLanePersistedLocked(relayEventDedupeKey("lane.event", pending.event))
	}
	h.mu.Unlock()
	if h.handoffkeep == nil || pending.eventID == 0 {
		return true
	}
	if err := h.handoffkeep.markDelivered(context.Background(), pending.eventID, pending.machine, pending.pane); err != nil {
		h.logger.Warn("relay delivery was not recorded", "event_id", pending.eventID, "machine", pending.machine)
		return false
	}
	return true
}

// recordLateRelayDelivery records a relay.delivered that no longer matches an
// ack window (#650). The window is memory only: a relay.unconfirmed consumes
// it before the node's retry lands, an expiry drops it, and a hub restart
// loses all of them. Discarding the ack in those cases left delivered_at NULL
// on rows the pane had already shown, and every restart replayed them. The
// row itself is the authority for whether this node may close it: the ack
// must name the row's transport id and arrive from the row's destination or
// the owner lane's current route.
func (h *HubServer) recordLateRelayDelivery(machineID string, ack relayAckPayload) bool {
	if h.handoffkeep == nil || ack.OriginalEventID < 1 {
		return false
	}
	record, found, err := h.handoffkeep.relayEvent(context.Background(), ack.OriginalEventID)
	if err != nil {
		h.logger.Warn("late relay delivery could not be checked", "event_id", ack.OriginalEventID, "machine", machineID)
		return false
	}
	if !found {
		return false
	}
	event := relayEventFromRecord(record)
	if event.JobID != ack.JobID {
		return false
	}
	route, _, _ := h.resolveRelayRoute(record.Kind, event)
	fromRow := record.Machine == machineID && record.PaneID == ack.Pane
	fromRoute := !route.Sink && route.Machine == machineID && route.Pane == ack.Pane
	if !fromRow && !fromRoute {
		return false
	}
	if record.DeliveredAt != "" {
		return true
	}
	// Only a durable write accepts the ack: a rejected one leaves the row
	// undelivered, and broadcasting it as delivered would hide that.
	return h.markRelayEventDelivered(relayPending{machine: machineID, pane: ack.Pane, eventID: record.ID, kind: record.Kind, event: event})
}

// relayEventFromRecord rebuilds the relay payload a durable row stands for.
func relayEventFromRecord(record handoffkeepRelayEvent) hubJobEventPayload {
	event := hubJobEventPayload{
		JobID: record.JobID, Epoch: uint64(record.Epoch), OwnerLane: record.OwnerLane,
		ReportPath: record.ReportPath, ReportLastLine: record.ReportLastLine,
		Question: record.Question, PR: record.PR, Head: record.Head, Reason: record.Reason, PaneID: record.PaneID, EventID: record.EventID, Text: record.Text,
	}
	if record.Kind == "lane.event" {
		event.JobID = laneEventTransportID(record.OwnerLane, record.EventID)
	}
	return event
}

const (
	// relayReplayMaxAge matches the node outbox's default bound
	// (defaultRelayOutboxMaxAge): a node stops offering an event after a day,
	// and the hub stops re-injecting one after the same day.
	relayReplayMaxAge = defaultRelayOutboxMaxAge
	// idleWakeReplayMaxAge bounds an idle-wake replay. It reports one pane
	// state at one instant; half an hour later the owner has to look at the
	// pane anyway, and a replayed wake reads as a new transition (#650).
	idleWakeReplayMaxAge = 30 * time.Minute
	// relayReplayRetiredMachine and the pane prefix below are the
	// delivered_to handoffkeep keeps for a retired row, so the audit record
	// names why it was closed without reaching a pane.
	relayReplayRetiredMachine = "hub"
	relayReplayRetiredPane    = "replay-retired:"
)

// relayReplayRetireReason is the replay gate for rows that must not reach a
// pane after a restart or node hello (#650), or "" when the row may replay.
// An idle-wake is retired when its subject pane is no lane's pane any more
// (the lane was removed, or moved to another pane) or it is older than
// idleWakeReplayMaxAge; any other row when it is older than relayReplayMaxAge.
// A lanes file that cannot be read proves nothing about lanes, so it never
// retires a row on lane grounds.
func (h *HubServer) relayReplayRetireReason(record handoffkeepRelayEvent) string {
	now := h.now().UTC()
	if record.Kind == "lane.event" {
		if wake, ok := decodeIdleWakeRelayText(record.Text); ok {
			if routes, err := loadReportRelayRoutesResult(h.reportRelayPath); err == nil && routes != nil && !idleWakePaneHasLane(routes, wake.Pane) {
				return "lane_gone"
			}
			if now.Sub(wake.ChangedAt) > idleWakeReplayMaxAge {
				return "stale"
			}
			return ""
		}
	}
	if received, err := time.Parse(time.RFC3339Nano, record.ReceivedAt); err == nil && now.Sub(received) > relayReplayMaxAge {
		return "stale"
	}
	return ""
}

func idleWakePaneHasLane(routes map[string]reportRelayRoute, pane string) bool {
	for _, route := range routes {
		if !route.Sink && route.Pane == pane {
			return true
		}
	}
	return false
}

type idleWakeRelayText struct {
	Pane      string
	ChangedAt time.Time
}

// decodeIdleWakeRelayText reads the subject of the text idleWakeRouteText
// wrote. Anything else is not an idle-wake and keeps the general gate.
func decodeIdleWakeRelayText(text string) (idleWakeRelayText, bool) {
	var wake struct {
		Kind      string `json:"kind"`
		Pane      string `json:"pane"`
		ChangedAt string `json:"changed_at"`
	}
	if json.Unmarshal([]byte(text), &wake) != nil || wake.Kind != "idle-wake" || !validIdleWakePane(wake.Pane) {
		return idleWakeRelayText{}, false
	}
	changedAt, ok := canonicalIdleWakeTime(wake.ChangedAt)
	if !ok {
		return idleWakeRelayText{}, false
	}
	return idleWakeRelayText{Pane: wake.Pane, ChangedAt: changedAt}, true
}

// relayReplayRetiredMarker is the delivered_to prefix retireRelayReplay
// writes. Current handoffkeep sets delivered_at and delivered_to together, so
// a retired row leaves the undelivered listing and this prefix is never seen
// there — the check stays as a guard for any server whose retire write the
// listing does not observe (#658).
const relayReplayRetiredMarker = relayReplayRetiredMachine + "/" + relayReplayRetiredPane

// retireRelayReplay closes a row the replay gate refused, so no later restart
// lists it again, and puts the reason on the operator feed.
func (h *HubServer) retireRelayReplay(record handoffkeepRelayEvent, reason string) {
	if !h.claimReplayRetire(record.ID) {
		h.logger.Debug("relay replay row already retired", "event_id", record.ID, "lane", record.OwnerLane, "delivered_to", record.DeliveredTo)
		return
	}
	if err := h.handoffkeep.markDelivered(context.Background(), record.ID, relayReplayRetiredMachine, relayReplayRetiredPane+reason); err != nil {
		h.releaseReplayRetire(record.ID)
		h.logger.Warn("retired relay replay was not recorded", "event_id", record.ID, "reason", reason)
		return
	}
	h.logger.Info("relay replay retired", "event_id", record.ID, "kind", record.Kind, "lane", record.OwnerLane, "reason", reason)
	payload, _ := json.Marshal(struct {
		EventID  int64  `json:"event_id"`
		Kind     string `json:"kind"`
		Lane     string `json:"lane"`
		SourceID string `json:"source_event_id,omitempty"`
		Reason   string `json:"reason"`
	}{EventID: record.ID, Kind: record.Kind, Lane: record.OwnerLane, SourceID: record.EventID, Reason: reason})
	h.broadcast(hubEvent{Kind: "relay.replay_retired", Payload: payload, Received: h.now().UTC()})
}

// replayUndeliveredRelayEvents re-routes what Postgres still holds as
// undelivered. It runs once at startup and never re-persists: the row exists.
func (h *HubServer) replayUndeliveredRelayEvents(ctx context.Context) {
	if h.handoffkeep == nil {
		return
	}
	var afterID int64
	for {
		pageStart := afterID
		records, err := h.handoffkeep.listUndelivered(ctx, "", "", afterID, handoffkeepReplayLimit)
		if err != nil {
			h.logger.Warn("undelivered relay events could not be read at startup")
			return
		}
		for _, record := range records {
			h.replayRelayEvent(record)
			if record.ID > afterID {
				afterID = record.ID
			}
		}
		if len(records) < handoffkeepReplayLimit {
			return
		}
		if afterID <= pageStart {
			h.logger.Warn("undelivered relay replay cursor did not advance", "after_id", pageStart)
			return
		}
	}
}

// replayUndeliveredLaneEvents runs after a node hello. A lanes.json edit has
// no daemon restart signal of its own, so the next destination registration is
// the safe retry trigger. The ordinary delivered_at and attempts gates remain
// centralized in replayRelayEvent.
func (h *HubServer) replayUndeliveredLaneEvents(ctx context.Context) {
	if h.handoffkeep == nil {
		return
	}
	var afterID int64
	for {
		pageStart := afterID
		records, err := h.handoffkeep.listUndelivered(ctx, "", "lane.event", afterID, handoffkeepReplayLimit)
		if err != nil {
			h.logger.Warn("undelivered lane events could not be read after node registration")
			return
		}
		for _, record := range records {
			h.replayRelayEvent(record)
			if record.ID > afterID {
				afterID = record.ID
			}
		}
		if len(records) < handoffkeepReplayLimit {
			return
		}
		if afterID <= pageStart {
			h.logger.Warn("undelivered lane replay cursor did not advance", "after_id", pageStart)
			return
		}
	}
}

func (h *HubServer) replayRelayEvent(record handoffkeepRelayEvent) {
	event := relayEventFromRecord(record)
	if event.OwnerLane == "" || event.JobID == "" {
		return
	}
	// The query asks for undelivered rows, but the gate is the hub's own: a
	// row that has been delivered, or has already spent its attempts, is the
	// difference between a replay and a re-injection storm on every restart.
	if record.DeliveredAt != "" {
		return
	}
	if strings.HasPrefix(record.DeliveredTo, relayReplayRetiredMarker) {
		return
	}
	if match := chatRelayEventPattern.FindStringSubmatch(record.EventID); match != nil {
		// The chat store is the truth the operator saw: only a stored row may
		// still be injected. Failed and delivered (cancelled) rows — and rows
		// the store no longer knows — retire their relay row instead. Rows
		// from before this invariant existed can still pair a terminal chat
		// row with a live relay row; this gate is what keeps them dead.
		if chatID, err := strconv.ParseInt(match[1], 10, 64); err == nil {
			switch h.chatReplayDisposition(chatID) {
			case "retire":
				if err := h.handoffkeep.markDelivered(context.Background(), record.ID, "hub", "chat-terminal"); err != nil {
					h.logger.Warn("terminal chat relay row was not retired", "event_id", record.ID)
				}
				return
			case "defer":
				return
			}
		}
	} else if reason := h.relayReplayRetireReason(record); reason != "" {
		// Chat rows keep the chat store as their only authority; every other
		// row passes the #650 gate before it can spend an attempt.
		h.retireRelayReplay(record, reason)
		return
	}
	if record.Attempts >= relayReplayMaxAttempts {
		h.broadcastRelayReplayExhausted(record)
		if match := chatRelayEventPattern.FindStringSubmatch(record.EventID); match != nil {
			if chatID, err := strconv.ParseInt(match[1], 10, 64); err == nil {
				h.noteChatRelayExhausted(chatID)
			}
		}
		return
	}
	key := relayEventDedupeKey(record.Kind, event)
	if record.Kind == "lane.event" {
		h.rememberLanePersisted(key, record.ID)
		h.rememberLaneEventSHA(key, record.Text)
	}
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
	if record.Kind == "lane.event" && route.Sink {
		// A failed sink delivery mark leaves an observable undelivered row, but
		// its later replay must never reinterpret the sink as a pane target.
		h.forgetRelayEvent(key)
		h.broadcastRelayUnrouted(event)
		return
	}
	if record.Kind == "lane.event" {
		h.registerRelayAckEvent("lane.event", event, route.Machine, route.Pane, record.ID)
		if !agent.queueRelay(relayInjectDirective("lane.event", event, route, record.ID)) {
			h.forgetRelayEvent(key)
			h.forgetRelayAckEvent(record.ID, event.JobID)
			h.broadcastRelayUnrouted(event)
			return
		}
		h.armRelayAckEvent(record.ID, event.JobID)
		// A queued chat directive just made it to the destination channel:
		// flip the chat row to delivered and resolve its linked question.
		if match := chatRelayEventPattern.FindStringSubmatch(record.EventID); match != nil {
			if chatID, err := strconv.ParseInt(match[1], 10, 64); err == nil {
				h.noteChatRelayDelivered(chatID, record.Question)
			}
		}
	} else if !h.injectRelayEvent(record.Kind, event, route, agent, nil, record.ID, key) {
		return
	}
	// The replay just spent an attempt. Recording it is what makes the gate
	// above converge instead of replaying the same row after every restart.
	h.bumpRelayEventAttempts(record.Kind, event, route)
}
