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
}

func relayAckTimeoutFromEnv() time.Duration {
	if timeout, err := time.ParseDuration(os.Getenv("RELAY_ACK_TIMEOUT")); err == nil && timeout > 0 {
		return timeout
	}
	return defaultRelayAckTimeout
}

func (h *HubServer) startRelayAck(jobID, machine, pane string) {
	h.mu.Lock()
	if _, exists := h.relayPending[jobID]; exists {
		h.mu.Unlock()
		return
	}
	h.relayPending[jobID] = relayPending{machine: machine, pane: pane}
	timeout := h.relayAckTimeout
	h.mu.Unlock()
	time.AfterFunc(timeout, func() { h.expireRelayAck(jobID) })
}

func (h *HubServer) expireRelayAck(jobID string) {
	h.mu.Lock()
	if _, pending := h.relayPending[jobID]; !pending {
		h.mu.Unlock()
		return
	}
	delete(h.relayPending, jobID)
	if _, already := h.relayTimeouts[jobID]; already {
		h.mu.Unlock()
		return
	}
	h.relayTimeouts[jobID] = struct{}{}
	h.mu.Unlock()
	payload, _ := json.Marshal(struct {
		JobID  string `json:"job_id"`
		Reason string `json:"reason"`
	}{JobID: jobID, Reason: "ack_timeout"})
	h.broadcast(hubEvent{Kind: "relay.unconfirmed", Payload: payload, Received: h.now().UTC()})
}

func (h *HubServer) acknowledgeRelay(machineID string, ack relayAckPayload) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	pending, exists := h.relayPending[ack.JobID]
	if exists && pending.machine == machineID && pending.pane == ack.Pane {
		delete(h.relayPending, ack.JobID)
		return true
	}
	return false
}
