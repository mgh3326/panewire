package panewire

import (
	"encoding/json"
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
	// Reasons are the busy_relay.go reportHeld call sites: the pane status
	// that held it (working/blocked), a restored row after a node restart,
	// and retryOrDropWithin's retry re-arm (#683: that one used to be
	// rejected, so retries were invisible in the held projection).
	if json.Unmarshal(raw, &value) != nil || value.EventID < 1 || !hubJobIDPattern.MatchString(value.JobID) || value.Pane == "" || value.Pane == relayCancelledPane || len(value.Pane) > 128 || !validLaneReference(value.Lane) || (value.Reason != "working" && value.Reason != "blocked" && value.Reason != "restored" && value.Reason != "retry") || len(value.Preview) > 240 || !validRelayDeliver(value.DeliverPolicy) {
		return relayHeldPayload{}, false
	}
	if _, err := time.Parse(time.RFC3339Nano, value.HeldSince); err != nil {
		return relayHeldPayload{}, false
	}
	return value, true
}

func decodeRelayReleasedPayload(raw []byte) (relayReleasedPayload, bool) {
	var value relayReleasedPayload
	if json.Unmarshal(raw, &value) != nil || !hubJobIDPattern.MatchString(value.JobID) || value.Pane == "" || value.Pane == relayCancelledPane || len(value.Pane) > 128 || !validLaneReference(value.Lane) || value.OriginalEventID < 1 || !validRelayFinalText(value.FinalText) {
		return relayReleasedPayload{}, false
	}
	return value, true
}

func decodeRelayCancelledPayload(raw []byte) (relayCancelledPayload, bool) {
	var value relayCancelledPayload
	return value, json.Unmarshal(raw, &value) == nil && value.OriginalEventID > 0
}

// decodeRelayDroppedPayload validates #264 D1's terminal signal: a node gave
// up retrying a held inject and deleted its own local row. The hub mirrors
// that by dropping the held projection in consumeRelayDropped below.
func decodeRelayDroppedPayload(raw []byte) (relayDroppedPayload, bool) {
	var value relayDroppedPayload
	if json.Unmarshal(raw, &value) != nil || !hubJobIDPattern.MatchString(value.JobID) || value.Pane == "" || value.Pane == relayCancelledPane || len(value.Pane) > 128 || !validLaneReference(value.Lane) || value.OriginalEventID < 1 || value.Reason == "" || len(value.Reason) > 64 {
		return relayDroppedPayload{}, false
	}
	return value, true
}

func decodeRelayBatchedPayload(raw []byte) (relayBatchedPayload, bool) {
	var value relayBatchedPayload
	if json.Unmarshal(raw, &value) != nil || value.Pane == "" || value.Pane == relayCancelledPane || !validLaneReference(value.Lane) || len(value.EventIDs) < 2 {
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
	// #989: a lane re-pointed between register and hold leaves the node
	// holding the row on a different pane than the window was armed for.
	// The machine is still the entitlement; the pane is a detail — but the
	// window must follow the pane the node actually holds, because the
	// node's own relay.dropped is pane-checked in consumeRelayDropped.
	if pending, exists := h.relayControl.relayPending[key]; exists && pending.machine == machine {
		pending.held = true
		pending.pane = held.Pane
		h.relayControl.relayPending[key] = pending
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
	pending, pendingExists := h.relayControl.relayPending[key]
	if !exists && !pendingExists {
		return false
	}
	// The releasing pane may differ from the window's armed pane when the
	// lane moved mid-hold (#989); the machine is the entitlement.
	if pendingExists && pending.machine != machine {
		return false
	}
	delete(h.relayHeld, released.OriginalEventID)
	if pendingExists {
		pending.held, pending.armed = false, false
		h.relayControl.relayPending[key] = pending
	}
	go h.armRelayAckEvent(released.OriginalEventID, released.JobID)
	return true
}

// consumeRelayDropped removes the held projection for a node's own explicit
// drop (#264 D1). Unlike releaseRelayHeld/consumeRelayCancelled it never
// races an operator action on the same row, so it only needs to guard
// against a stale report from a machine that no longer owns the row. The
// same ownership checks cover the pending ack entry: a drop may only retire
// a window bound to this machine and pane — the pane is part of the lease
// the node itself reported, so a drop naming another pane is inconsistent
// with its own held report, not with a lane move (#989).
func (h *HubServer) consumeRelayDropped(machine string, dropped relayDroppedPayload) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if held, exists := h.relayHeld[dropped.OriginalEventID]; exists && (held.Machine != machine || held.Pane != dropped.Pane) {
		return false
	}
	key := relayPendingKey(dropped.OriginalEventID, dropped.JobID)
	if pending, exists := h.relayControl.relayPending[key]; exists && (pending.machine != machine || pending.pane != dropped.Pane) {
		return false
	}
	delete(h.relayHeld, dropped.OriginalEventID)
	// A dropped lease will never emit relay.released, so its ack window would
	// otherwise sit held=true forever. The durable row is deliberately left
	// undelivered; replay re-registers a fresh pending entry when it retries.
	h.cancelRelayPendingLocked(dropped.OriginalEventID, dropped.JobID)
	return true
}

// forgetRelayHeld drops the held projection for an event a node reports as
// cancelled. Older nodes still emit relay.cancelled; the current hub no longer
// issues cancels itself.
func (h *HubServer) forgetRelayHeld(id int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.relayHeld, id)
}

func (h *HubServer) cancelRelayPendingLocked(eventID int64, jobID string) {
	delete(h.relayControl.relayPending, relayPendingKey(eventID, jobID))
}
