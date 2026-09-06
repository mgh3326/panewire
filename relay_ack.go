package panewire

import (
	"encoding/json"
	"os"
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
	jobID := event.JobID
	h.mu.Lock()
	if _, exists := h.r19a.relayPending[jobID]; exists {
		h.mu.Unlock()
		return
	}
	h.r19a.relayPending[jobID] = relayPending{machine: machine, pane: pane, eventID: eventID, kind: kind, event: event}
	timeout := h.r19a.relayAckTimeout
	h.mu.Unlock()
	time.AfterFunc(timeout, func() { h.expireRelayAck(jobID) })
}

func (h *HubServer) expireRelayAck(jobID string) {
	h.mu.Lock()
	pending, isPending := h.r19a.relayPending[jobID]
	if !isPending {
		h.mu.Unlock()
		return
	}
	delete(h.r19a.relayPending, jobID)
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
	broadcast := h.r19a.rememberRelayTimeout(jobID)
	h.mu.Unlock()
	if !broadcast {
		return
	}
	payload, _ := json.Marshal(struct {
		JobID  string `json:"job_id"`
		Reason string `json:"reason"`
	}{JobID: jobID, Reason: "ack_timeout"})
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
	pending, exists := h.r19a.relayPending[ack.JobID]
	if exists && pending.machine == machineID && pending.pane == ack.Pane {
		delete(h.r19a.relayPending, ack.JobID)
		return pending, true
	}
	return relayPending{}, false
}
