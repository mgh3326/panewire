package panewire

import (
	"encoding/json"
	"os"
	"time"
)

const defaultRelayAckTimeout = 15 * time.Second

type relayPending struct {
	machine string
	pane    string
	// eventID is the handoffkeep row this injection came from. It lives only
	// for the acknowledgement window; the hub stays stateless about relays.
	eventID int64
}

func relayAckTimeoutFromEnv() time.Duration {
	if timeout, err := time.ParseDuration(os.Getenv("RELAY_ACK_TIMEOUT")); err == nil && timeout > 0 {
		return timeout
	}
	return defaultRelayAckTimeout
}

func (h *HubServer) startRelayAck(jobID, machine, pane string) {
	h.startRelayAckEvent(jobID, machine, pane, 0)
}

func (h *HubServer) startRelayAckEvent(jobID, machine, pane string, eventID int64) {
	h.mu.Lock()
	if _, exists := h.r19a.relayPending[jobID]; exists {
		h.mu.Unlock()
		return
	}
	h.r19a.relayPending[jobID] = relayPending{machine: machine, pane: pane, eventID: eventID}
	timeout := h.r19a.relayAckTimeout
	h.mu.Unlock()
	time.AfterFunc(timeout, func() { h.expireRelayAck(jobID) })
}

func (h *HubServer) expireRelayAck(jobID string) {
	h.mu.Lock()
	if _, pending := h.r19a.relayPending[jobID]; !pending {
		h.mu.Unlock()
		return
	}
	delete(h.r19a.relayPending, jobID)
	if _, already := h.r19a.relayTimeouts[jobID]; already {
		h.mu.Unlock()
		return
	}
	h.r19a.relayTimeouts[jobID] = struct{}{}
	h.mu.Unlock()
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
