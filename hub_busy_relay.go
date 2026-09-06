package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// hubRelayHeldProjection is intentionally body-free. The node owns held text;
// this is only the current connected-node projection used by operators.
type hubRelayHeldProjection struct {
	ID            int64  `json:"id"`
	Lane          string `json:"lane"`
	Pane          string `json:"pane"`
	Machine       string `json:"machine"`
	Preview       string `json:"preview"`
	HeldSince     string `json:"held_since"`
	DeliverPolicy string `json:"deliver_policy"`
	JobID         string `json:"-"`
}

func decodeRelayHeldPayload(raw []byte) (relayHeldPayload, bool) {
	var value relayHeldPayload
	if json.Unmarshal(raw, &value) != nil || value.EventID < 1 || !hubJobIDPattern.MatchString(value.JobID) || value.Pane == "" || value.Pane == relayCancelledPane || len(value.Pane) > 128 || !hubAgentLabelPattern.MatchString(value.Lane) || (value.Reason != "working" && value.Reason != "blocked" && value.Reason != "restored") || len(value.Preview) > 240 || !validRelayDeliver(value.DeliverPolicy) {
		return relayHeldPayload{}, false
	}
	if _, err := time.Parse(time.RFC3339Nano, value.HeldSince); err != nil {
		return relayHeldPayload{}, false
	}
	return value, true
}

func decodeRelayReleasedPayload(raw []byte) (relayReleasedPayload, bool) {
	var value relayReleasedPayload
	if json.Unmarshal(raw, &value) != nil || !hubJobIDPattern.MatchString(value.JobID) || value.Pane == "" || value.Pane == relayCancelledPane || len(value.Pane) > 128 || !hubAgentLabelPattern.MatchString(value.Lane) || value.OriginalEventID < 1 || !validRelayFinalText(value.FinalText) {
		return relayReleasedPayload{}, false
	}
	return value, true
}

func decodeRelayCancelledPayload(raw []byte) (relayCancelledPayload, bool) {
	var value relayCancelledPayload
	return value, json.Unmarshal(raw, &value) == nil && value.OriginalEventID > 0
}

func decodeRelayBatchedPayload(raw []byte) (relayBatchedPayload, bool) {
	var value relayBatchedPayload
	if json.Unmarshal(raw, &value) != nil || value.Pane == "" || value.Pane == relayCancelledPane || !hubAgentLabelPattern.MatchString(value.Lane) || len(value.EventIDs) < 2 {
		return relayBatchedPayload{}, false
	}
	for _, id := range value.EventIDs {
		if id < 1 {
			return relayBatchedPayload{}, false
		}
	}
	return value, true
}

func (h *HubServer) rememberRelayHeld(machine string, held relayHeldPayload) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	record := h.nodes[machine]
	if record == nil || record.agent == nil {
		return false
	}
	h.relayHeld[held.EventID] = hubRelayHeldProjection{ID: held.EventID, Lane: held.Lane, Pane: held.Pane, Machine: machine, Preview: held.Preview, HeldSince: held.HeldSince, DeliverPolicy: held.DeliverPolicy, JobID: held.JobID}
	key := relayPendingKey(held.EventID, held.JobID)
	if pending, exists := h.r19a.relayPending[key]; exists && pending.machine == machine && pending.pane == held.Pane {
		pending.held = true
		h.r19a.relayPending[key] = pending
	}
	return true
}

func (h *HubServer) releaseRelayHeld(machine string, released relayReleasedPayload) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	held, exists := h.relayHeld[released.OriginalEventID]
	if exists && held.Machine != machine {
		return false
	}
	key := relayPendingKey(released.OriginalEventID, released.JobID)
	pending, pendingExists := h.r19a.relayPending[key]
	if !exists && !pendingExists {
		return false
	}
	if pendingExists && (pending.machine != machine || pending.pane != released.Pane) {
		return false
	}
	delete(h.relayHeld, released.OriginalEventID)
	if pendingExists {
		pending.held, pending.armed = false, false
		h.r19a.relayPending[key] = pending
	}
	go h.armRelayAckEvent(released.OriginalEventID, released.JobID)
	return true
}

func (h *HubServer) consumeRelayCancelled(id int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.relayCancelled[id]; !exists {
		delete(h.relayHeld, id)
		return false
	}
	delete(h.relayCancelled, id)
	return true
}

func (h *HubServer) handleRelayHeld(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	lane := request.URL.Query().Get("lane")
	if lane != "" && !hubAgentLabelPattern.MatchString(lane) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_lane"})
		return
	}
	h.mu.Lock()
	held := make([]hubRelayHeldProjection, 0, len(h.relayHeld))
	for _, item := range h.relayHeld {
		if lane == "" || item.Lane == lane {
			held = append(held, item)
		}
	}
	h.mu.Unlock()
	sort.Slice(held, func(i, j int) bool { return held[i].ID < held[j].ID })
	writeHubJSON(writer, http.StatusOK, struct {
		Held []hubRelayHeldProjection `json:"held"`
	}{Held: held})
}

func (h *HubServer) handleRelayHeldEdit(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	id, valid := parseRelayHeldID(request)
	if !valid {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_event"})
		return
	}
	var body struct {
		Text string `json:"text"`
	}
	if !decodeHubJSON(writer, request, hubRelayIngressMaxBodyBytes, &body) || !validRelayFinalText(body.Text) {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	if !h.queueRelayHeldControl(id, hubRelayInjectEvent{Type: "relay.edit", EventID: id, Text: body.Text}) {
		writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "not_held"})
		return
	}
	writeHubJSON(writer, http.StatusOK, map[string]any{"id": id})
}

func (h *HubServer) handleRelayHeldDelete(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	id, valid := parseRelayHeldID(request)
	if !valid {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_event"})
		return
	}
	h.mu.Lock()
	held, exists := h.relayHeld[id]
	if !exists {
		h.mu.Unlock()
		writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "not_held"})
		return
	}
	record := h.nodes[held.Machine]
	if record == nil || record.agent == nil || !record.agent.queueRelay(hubRelayInjectEvent{Type: "relay.cancel", EventID: id}) {
		h.mu.Unlock()
		writeHubJSON(writer, http.StatusConflict, map[string]string{"error": "node_unavailable"})
		return
	}
	delete(h.relayHeld, id)
	h.relayCancelled[id] = struct{}{}
	h.cancelRelayPendingLocked(id, held.JobID)
	h.mu.Unlock()
	if h.handoffkeep != nil {
		if err := h.handoffkeep.markDelivered(context.Background(), id, held.Machine, relayCancelledPane); err != nil {
			// The node still receives cancellation, but an undelivered record
			// must remain visible as an operator error rather than pretending it
			// can never replay.
			h.logger.Warn("relay cancellation was not recorded", "event_id", id)
		}
	}
	payload, _ := json.Marshal(relayCancelledPayload{OriginalEventID: id})
	h.broadcast(hubEvent{Kind: "relay.cancelled", Payload: payload, Received: h.now().UTC()})
	writer.WriteHeader(http.StatusNoContent)
}

func (h *HubServer) queueRelayHeldControl(id int64, control hubRelayInjectEvent) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	held, exists := h.relayHeld[id]
	if !exists {
		return false
	}
	record := h.nodes[held.Machine]
	return record != nil && record.agent != nil && record.agent.queueRelay(control)
}

func (h *HubServer) cancelRelayPendingLocked(eventID int64, jobID string) {
	delete(h.r19a.relayPending, relayPendingKey(eventID, jobID))
}

func parseRelayHeldID(request *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(request.PathValue("id"), 10, 64)
	return id, err == nil && id > 0
}
