package panewire

// Contract: docs/relay-harness-delivery.md (workingUnprovable).
//
// hk 3411: a row a node dropped as working_wait_expired was never typed: the
// pane was still working when the hold ended. When the idle-wake observer
// reports that pane idle, the hub asks for one such row again: one row per idle
// wake, oldest first. A hub with an inbox store records the request on the
// row's delivery obligation and the next pass queues the copy (hk 6820,
// delivery_obligation_revive.go). A hub without one keeps the rows in memory
// (hub_relay_idle_legacy.go).

import "strings"

// relayReplaySourceIdle names the idle-wake re-delivery in replay logs and
// broadcasts, beside relayReplaySourceStartup and relayReplaySourceHello.
const relayReplaySourceIdle = "idle"

// redeliverWorkingExpiredOnIdle runs on an idle-wake route request: the pane
// has settled idle or done. A row whose working expiry is still being written
// keeps the wake on its ack window, and the write runs it again.
func (h *HubServer) redeliverWorkingExpiredOnIdle(machine string, request idleWakeRouteRequest) {
	if h.inbox == nil {
		h.redeliverLegacyWorkingExpiredOnIdle(machine, request)
		return
	}
	wake := idleRedeliveryWakeOf(request)
	h.mu.Lock()
	for key, pending := range h.relayControl.relayPending {
		if pending.dropReason != relayDroppedWorkingExpired || pending.machine != machine || pending.pane != request.Pane {
			continue
		}
		if pending.idleWake.Pane == "" || wake.newerThan(idleRedeliveryWakeOf(pending.idleWake)) {
			pending.idleWake = request
			h.relayControl.relayPending[key] = pending
		}
	}
	h.mu.Unlock()
	h.requestWorkingExpiredRevive(machine, request)
}

// requestWorkingExpiredRevive asks for the revived copy of the oldest
// working-expired row among the lanes routed to the pane. Only a wake newer
// than the last one that asked for a row of the pane asks for another, so a
// retried request for a wake already used, or for an older one, asks for
// nothing. A wake that finds no row is not used up.
func (h *HubServer) requestWorkingExpiredRevive(machine string, request idleWakeRouteRequest) {
	if h.handoffkeep == nil {
		return
	}
	// One request at a time: a second run of the same wake must see whether the
	// first used it.
	h.obligations.reviveMu.Lock()
	defer h.obligations.reviveMu.Unlock()
	routes, err := loadReportRelayRoutesResult(h.reportRelayPath)
	if err != nil {
		return
	}
	var lanes []string
	for lane, route := range routes {
		if !route.Sink && route.Machine == machine && route.Pane == request.Pane {
			lanes = append(lanes, lane)
		}
	}
	if len(lanes) == 0 {
		return
	}
	paneKey := machine + "\x00" + request.Pane
	wake := idleRedeliveryWakeOf(request)
	h.mu.Lock()
	previous, seen := h.relayControl.idleRedelivered[paneKey]
	h.mu.Unlock()
	if seen && !wake.newerThan(previous) {
		return
	}
	store := h.inbox.store
	marked, err := store.requestObligationRevive(h.closeCtx, lanes, h.now().UTC())
	if err != nil {
		h.logger.Warn("idle re-delivery was not requested; the next idle wake asks again", "machine", machine, "pane", request.Pane, "error", store.loggable("revive request", err))
		return
	}
	if !marked {
		return
	}
	h.rememberIdleRedeliveryWake(paneKey, wake)
	h.startObligationReconcile()
}

func (h *HubServer) rememberIdleRedeliveryWake(paneKey string, wake idleRedeliveryWake) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.relayControl.idleRedelivered == nil {
		h.relayControl.idleRedelivered = make(map[string]idleRedeliveryWake)
	}
	h.relayControl.idleRedelivered[paneKey] = wake
}

// idleRedeliveryWake orders the idle wakes of one pane: the node's sequence
// namespace and the pane's state change sequence, both from the wake's event
// id (validIdleWakeEventID has checked its shape).
type idleRedeliveryWake struct {
	namespace string
	sequence  uint64
}

func idleRedeliveryWakeOf(request idleWakeRouteRequest) idleRedeliveryWake {
	wake := idleRedeliveryWake{sequence: request.StateChangeSeq}
	if parts := strings.Split(request.EventID, ":"); len(parts) == 4 {
		wake.namespace = parts[1]
	}
	return wake
}

// newerThan reports whether wake is a later idle turn than previous. A new
// namespace means the node's idle-wake journal was reset, so its sequence
// restarts and the wake counts as newer.
func (wake idleRedeliveryWake) newerThan(previous idleRedeliveryWake) bool {
	return wake.namespace != previous.namespace || wake.sequence > previous.sequence
}

// relayIdleRedeliveryLabel is the one-line prefix an idle re-delivery carries.
// Nothing was typed the first time, so unlike the restart label it does not
// warn of a duplicate; it says why the message arrives late.
func relayIdleRedeliveryLabel(record handoffkeepRelayEvent) string {
	stamp := "unknown-time"
	if when, ok := unconfirmedMarkFirstSent(record); ok {
		stamp = when.In(relayUnconfirmedLabelZone).Format("2006-01-02 15:04:05 MST")
	}
	return "[held while this pane was working - first sent " + stamp + " - delivered now that it is idle] "
}
