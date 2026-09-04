package panewire

import (
	"encoding/json"
	"os"
	"time"
)

const defaultRelayAckTimeout = 15 * time.Second
const defaultRelayReplayGrace = 10 * time.Minute

func relayReplayGraceFromEnv() time.Duration {
	if grace, err := time.ParseDuration(os.Getenv("RELAY_REPLAY_GRACE")); err == nil && grace > 0 {
		return grace
	}
	return defaultRelayReplayGrace
}

type relayPending struct {
	machine       string
	pane          string
	sourceMachine string
	event         hubJobEventPayload
	kind          string
}

func relayAckTimeoutFromEnv() time.Duration {
	if timeout, err := time.ParseDuration(os.Getenv("RELAY_ACK_TIMEOUT")); err == nil && timeout > 0 {
		return timeout
	}
	return defaultRelayAckTimeout
}

func (h *HubServer) startRelayAck(jobID, machine, pane string) {
	h.startRelayAckForRelay(jobID, machine, pane, "", "", hubJobEventPayload{})
}

func (h *HubServer) startRelayAckForRelay(jobID, machine, pane, sourceMachine, kind string, event hubJobEventPayload) {
	h.mu.Lock()
	if _, exists := h.r19a.relayPending[jobID]; exists {
		h.mu.Unlock()
		return
	}
	h.r19a.relayPending[jobID] = relayPending{machine: machine, pane: pane, sourceMachine: sourceMachine, kind: kind, event: event}
	timeout := h.r19a.relayAckTimeout
	h.mu.Unlock()
	time.AfterFunc(timeout, func() { h.expireRelayAck(jobID) })
}

func (h *HubServer) expireRelayAck(jobID string) {
	h.mu.Lock()
	pending, exists := h.r19a.relayPending[jobID]
	if !exists {
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
	h.sendRelayAck(pending, "unconfirmed")
}

func (h *HubServer) acknowledgeRelay(machineID string, ack relayAckPayload) (relayPending, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	pending, exists := h.r19a.relayPending[ack.JobID]
	if exists && pending.machine == machineID && pending.pane == ack.Pane {
		delete(h.r19a.relayPending, ack.JobID)
		return pending, true
	}
	return relayPending{}, false
}

func (h *HubServer) sendRelayAck(pending relayPending, status string) {
	if pending.sourceMachine == "" || pending.kind == "" {
		return
	}
	h.mu.Lock()
	record := h.nodes[pending.sourceMachine]
	var agent *hubAgent
	if record != nil {
		agent = record.agent
	}
	h.mu.Unlock()
	if agent != nil {
		agent.queueRelayAck(hubRelayAckEvent{Type: "relay.ack", Status: status, Kind: pending.kind, JobID: pending.event.JobID, Epoch: pending.event.Epoch, ReportPath: pending.event.ReportPath, Reason: pending.event.Reason})
	}
}
