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
	// Expiry and node reports use the original event's durable identity.
	kind       string
	event      hubJobEventPayload
	armToken   *byte
	windowID   *byte
	dropReason string
	held       bool
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
	h.mu.Lock()
	defer h.mu.Unlock()
	h.registerRelayAckEventLocked(kind, event, machine, pane, eventID)
}

func (h *HubServer) registerRelayAckEventLocked(kind string, event hubJobEventPayload, machine, pane string, eventID int64) {
	key := relayPendingKey(eventID, event.JobID)
	if _, exists := h.relayControl.relayPending[key]; !exists {
		h.relayControl.relayPending[key] = relayPending{machine: machine, pane: pane, eventID: eventID, kind: kind, event: event, windowID: new(byte)}
	}
}

func (h *HubServer) armRelayAckEvent(eventID int64, jobID string) {
	key := relayPendingKey(eventID, jobID)
	h.mu.Lock()
	pending, exists := h.relayControl.relayPending[key]
	if !exists || pending.armToken != nil || pending.held {
		h.mu.Unlock()
		return
	}
	token := new(byte)
	pending.armToken = token
	h.relayControl.relayPending[key] = pending
	timeout := h.relayControl.relayAckTimeout
	h.mu.Unlock()
	time.AfterFunc(timeout, func() { h.expireRelayAckWindow(key, token) })
}

func (h *HubServer) forgetRelayAckEvent(eventID int64, jobID string) {
	h.mu.Lock()
	delete(h.relayControl.relayPending, relayPendingKey(eventID, jobID))
	h.mu.Unlock()
}

func (h *HubServer) expireRelayAck(key string) {
	h.expireRelayAckWindow(key, nil)
}

func (h *HubServer) expireRelayAckWindow(key string, token *byte) {
	h.mu.Lock()
	pending, isPending := h.relayControl.relayPending[key]
	if !isPending {
		// Kept for package tests and legacy internal callers which addressed
		// the old map by job id. Wire acknowledgements use original_event_id.
		for candidate, value := range h.relayControl.relayPending {
			if value.event.JobID == key {
				key, pending, isPending = candidate, value, true
				break
			}
		}
	}
	if pending.held || (token != nil && pending.armToken != token) {
		h.mu.Unlock()
		return
	}
	if !isPending {
		h.mu.Unlock()
		return
	}
	if h.inbox == nil {
		delete(h.relayControl.relayPending, key)
		h.mu.Unlock()
		h.recordRelayAttempt(pending)
		if pending.kind == "lane.event" {
			h.forgetRelayEvent(relayEventDedupeKey(pending.kind, pending.event))
		}
		h.mu.Lock()
	} else {
		// Take over the timer while persistence is in progress. If an ack or
		// release changes the window meanwhile, leave its newer state alone.
		token = new(byte)
		pending.armToken = token
		h.relayControl.relayPending[key] = pending
		h.mu.Unlock()
		fault := obligationAckTimeout
		if pending.dropReason != "" {
			fault = obligationFaultUnconfirmed
			if pending.dropReason == relayDroppedWorkingExpired {
				fault = relayDroppedWorkingExpired
			}
		}
		scheduled := h.scheduleRelayRetry(h.closeCtx, pending, fault)
		h.mu.Lock()
		current, exists := h.relayControl.relayPending[key]
		if !exists || current.armToken != token || current.held {
			h.mu.Unlock()
			return
		}
		if scheduled == relayFaultFailed {
			timeout := h.relayControl.relayAckTimeout
			h.mu.Unlock()
			if h.closeCtx.Err() == nil {
				time.AfterFunc(timeout, func() { h.expireRelayAckWindow(key, token) })
			}
			return
		}
		delete(h.relayControl.relayPending, key)
		if pending.dropReason != "" {
			h.rememberWorkingExpiredLocked(pending, relayAckPayload{Reason: pending.dropReason})
			if scheduled == relayFaultScheduled {
				delete(h.relayDedupe, relayEventDedupeKey(pending.kind, pending.event))
			}
			h.mu.Unlock()
			return
		}
		if scheduled == relayFaultScheduled {
			delete(h.relayDedupe, relayEventDedupeKey(pending.kind, pending.event))
		}
	}

	broadcast := h.relayControl.rememberRelayTimeout(key)
	h.mu.Unlock()
	if !broadcast {
		return
	}
	payload, _ := json.Marshal(struct {
		JobID  string `json:"job_id"`
		Reason string `json:"reason"`
	}{JobID: pending.event.JobID, Reason: "ack_timeout"})
	h.broadcast(hubEvent{Kind: "relay.unconfirmed", Payload: payload, Received: h.now().UTC()})
	h.observeRelayUnconfirmed(pending, relayAckPayload{Reason: "ack_timeout"})
	h.recordInboxStage(relayEventDedupeKey(pending.kind, pending.event), laneInboxStageUnconfirmed, "ack_timeout", pending.machine, pending.pane)
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
	key, pending, exists := h.findRelayPendingLocked(machineID, ack)
	if !exists {
		return relayPending{}, false
	}
	// The pane an ack reports is where the node actually delivered. A lane
	// re-pointed between inject and ack (#989) must not strand the row: the
	// pane is a destination detail, the machine is the entitlement, so the
	// ack claims the row under the pane it truly reached.
	delete(h.relayControl.relayPending, key)
	pending.pane = ack.Pane
	return pending, true
}

// findRelayPendingLocked resolves the live window an ack binds to without
// retiring it. The unconfirmed path peeks first so the durable mark lands
// while the window is still live (#1002).
func (h *HubServer) findRelayPendingLocked(machineID string, ack relayAckPayload) (string, relayPending, bool) {
	key := relayPendingKey(ack.OriginalEventID, ack.JobID)
	pending, exists := h.relayControl.relayPending[key]
	if !exists && ack.OriginalEventID == 0 {
		key = relayPendingKey(0, ack.JobID)
		pending, exists = h.relayControl.relayPending[key]
		if !exists {
			// A legacy ack carries no durable id. The job scan prefers the
			// pending whose pane the ack still names so two live windows for
			// one job on different panes never trade acks; only then does a
			// machine match alone suffice.
			for candidate, value := range h.relayControl.relayPending {
				if value.event.JobID == ack.JobID && value.machine == machineID && value.pane == ack.Pane {
					key, pending, exists = candidate, value, true
					break
				}
			}
			if !exists {
				for candidate, value := range h.relayControl.relayPending {
					if value.event.JobID == ack.JobID && value.machine == machineID {
						key, pending, exists = candidate, value, true
						break
					}
				}
			}
		}
	}
	// The id-keyed lookup never compared jobs, so an ack whose id names
	// this row but whose job_id names another event would close it. The
	// window keeps the job binding the late path keeps; a mismatch falls
	// through to recordLateRelayDelivery, which re-checks it.
	if !exists || pending.machine != machineID || (ack.JobID != "" && pending.event.JobID != ack.JobID) {
		return "", relayPending{}, false
	}
	pending.pane = ack.Pane
	return key, pending, true
}

// peekRelayPending resolves the window an unconfirmed report binds to and
// keeps it live while its caller persists the fault.
func (h *HubServer) peekRelayPending(machineID string, ack relayAckPayload) (string, relayPending, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.findRelayPendingLocked(machineID, ack)
}

// finishUnconfirmedWindow only retires the same injection the report observed.
func (h *HubServer) finishUnconfirmedWindow(key string, pending relayPending, ack relayAckPayload, write relayFaultWrite) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	current, exists := h.relayControl.relayPending[key]
	if !exists || current.windowID != pending.windowID {
		return false
	}
	h.rememberWorkingExpiredLocked(pending, ack)
	delete(h.relayControl.relayPending, key)
	if write == relayFaultScheduled {
		delete(h.relayDedupe, relayEventDedupeKey(pending.kind, pending.event))
	}
	return true
}
