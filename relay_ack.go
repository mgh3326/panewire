package panewire

import (
	"encoding/json"
	"os"
	"strconv"
	"time"
)

const defaultRelayAckTimeout = 15 * time.Second

const relayTimeoutsMaxEntries = 4096

type relayPending struct {
	machine string
	pane    string
	// eventID is the handoffkeep row this injection came from. It lives only
	// for the acknowledgement window; the hub stays stateless about relays.
	eventID int64
	// kind and event are retained for the same window so an expiry can record
	// the spent attempt the only way the contract allows: a re-POST of this
	// exact row. Nothing else reads them.
	kind  string
	event hubJobEventPayload
	armed bool
	held  bool
}

func relayPendingKey(eventID int64, jobID string) string {
	if eventID > 0 {
		return "id:" + strconv.FormatInt(eventID, 10)
	}
	return "job:" + jobID
}

func relayAckTimeoutFromEnv() time.Duration {
	if timeout, err := time.ParseDuration(os.Getenv("RELAY_ACK_TIMEOUT")); err == nil && timeout > 0 {
		return timeout
	}
	return defaultRelayAckTimeout
}

func (h *HubServer) startRelayAck(jobID, machine, pane string) {
	h.startRelayAckEvent("", hubJobEventPayload{JobID: jobID}, machine, pane, 0)
}

func (h *HubServer) startRelayAckEvent(kind string, event hubJobEventPayload, machine, pane string, eventID int64) {
	h.registerRelayAckEvent(kind, event, machine, pane, eventID)
	h.armRelayAckEvent(eventID, event.JobID)
}

// registerRelayAckEvent records an accepted directive, but deliberately does
// not start the delivery clock until a busy node releases it to its pane.
func (h *HubServer) registerRelayAckEvent(kind string, event hubJobEventPayload, machine, pane string, eventID int64) {
	key := relayPendingKey(eventID, event.JobID)
	h.mu.Lock()
	if _, exists := h.r19a.relayPending[key]; exists {
		h.mu.Unlock()
		return
	}
	h.r19a.relayPending[key] = relayPending{machine: machine, pane: pane, eventID: eventID, kind: kind, event: event}
	h.mu.Unlock()
}

func (h *HubServer) armRelayAckEvent(eventID int64, jobID string) {
	key := relayPendingKey(eventID, jobID)
	h.mu.Lock()
	pending, exists := h.r19a.relayPending[key]
	if !exists || pending.armed || pending.held {
		h.mu.Unlock()
		return
	}
	pending.armed = true
	h.r19a.relayPending[key] = pending
	timeout := h.r19a.relayAckTimeout
	h.mu.Unlock()
	time.AfterFunc(timeout, func() { h.expireRelayAck(key) })
}

func (h *HubServer) forgetRelayAckEvent(eventID int64, jobID string) {
	h.mu.Lock()
	delete(h.r19a.relayPending, relayPendingKey(eventID, jobID))
	h.mu.Unlock()
}

func (h *HubServer) expireRelayAck(key string) {
	h.mu.Lock()
	pending, isPending := h.r19a.relayPending[key]
	if !isPending {
		// Kept for package tests and legacy internal callers which addressed
		// the old map by job id. Wire acknowledgements use original_event_id.
		for candidate, value := range h.r19a.relayPending {
			if value.event.JobID == key {
				key, pending, isPending = candidate, value, true
				break
			}
		}
	}
	if pending.held {
		h.mu.Unlock()
		return
	}
	if !isPending {
		h.mu.Unlock()
		return
	}
	delete(h.r19a.relayPending, key)
	h.mu.Unlock()
	// Every expired window spends an attempt and releases its lane.event claim.
	// relayTimeouts only suppresses duplicate operator broadcasts below.
	h.recordRelayAttempt(pending)
	if pending.kind == "lane.event" {
		// lanePersisted still remembers the durable row for source ACK recovery,
		// but this active injection claim must be released so a later node hello
		// can retry without a hub restart.
		h.forgetRelayEvent(relayEventDedupeKey("lane.event", pending.event))
	}
	h.mu.Lock()
	broadcast := h.r19a.rememberRelayTimeout(key)
	h.mu.Unlock()
	if !broadcast {
		return
	}
	payload, _ := json.Marshal(struct {
		JobID  string `json:"job_id"`
		Reason string `json:"reason"`
	}{JobID: pending.event.JobID, Reason: "ack_timeout"})
	h.broadcast(hubEvent{Kind: "relay.unconfirmed", Payload: payload, Received: h.now().UTC()})
}

func (h *HubServer) acknowledgeRelay(machineID string, ack relayAckPayload) bool {
	_, acknowledged := h.acknowledgeRelayPending(machineID, ack)
	return acknowledged
}

// acknowledgeRelayPending also returns the retired window so the caller can
// record the delivery against its durable relay event.
func (h *HubServer) acknowledgeRelayPending(machineID string, ack relayAckPayload) (relayPending, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := relayPendingKey(ack.OriginalEventID, ack.JobID)
	pending, exists := h.r19a.relayPending[key]
	if !exists && ack.OriginalEventID == 0 {
		key = relayPendingKey(0, ack.JobID)
		pending, exists = h.r19a.relayPending[key]
		if !exists {
			for candidate, value := range h.r19a.relayPending {
				if value.event.JobID == ack.JobID {
					key, pending, exists = candidate, value, true
					break
				}
			}
		}
	}
	if exists && pending.machine == machineID && pending.pane == ack.Pane {
		delete(h.r19a.relayPending, key)
		return pending, true
	}
	return relayPending{}, false
}
