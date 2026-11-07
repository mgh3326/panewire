package panewire

// Contract: docs/relay-harness-delivery.md (workingUnprovable).
//
// hk 3411: a row a node dropped as working_wait_expired was never typed: the
// pane was still working when the hold ended. Its durable row stays
// undelivered with an unconfirmed mark, and before this change only a hub
// restart or a node hello replayed it. The hub now remembers such a row when
// its mark lands and re-delivers it the next time the idle-wake observer
// reports its pane idle: one row per idle wake, oldest first, through the
// marked row's one labelled replay, so each row is re-delivered at most once.
// The memory is not durable; a hub restart replays every marked row anyway.

import (
	"context"
	"strings"
)

const (
	// relayReplaySourceIdle names the idle-wake re-delivery in replay logs and
	// broadcasts, beside relayReplaySourceStartup and relayReplaySourceHello.
	relayReplaySourceIdle = "idle"
	// relayWorkingExpiredMaxRows bounds the rows the hub remembers. Past it a
	// new row is left to the restart and hello replays.
	relayWorkingExpiredMaxRows = 4096
)

// rememberWorkingExpired records a row the node dropped as
// working_wait_expired once its unconfirmed mark is durable. Only lane.event
// rows are remembered: they are the rows the marked replay re-delivers.
func (h *HubServer) rememberWorkingExpired(pending relayPending, ack relayAckPayload) {
	if ack.Reason != relayDroppedWorkingExpired || pending.kind != "lane.event" || pending.event.OwnerLane == "" {
		return
	}
	id := pending.eventID
	if id == 0 {
		id = ack.OriginalEventID
	}
	if id < 1 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.relayControl.workingExpired == nil {
		h.relayControl.workingExpired = make(map[int64]string)
	}
	if len(h.relayControl.workingExpired) >= relayWorkingExpiredMaxRows {
		return
	}
	h.relayControl.workingExpired[id] = pending.event.OwnerLane
}

// redeliverWorkingExpiredOnIdle runs on an idle-wake route request: the pane
// has settled idle or done. It re-delivers the oldest remembered row whose
// lane routes to that pane, once. Only a wake newer than the last one that
// re-delivered a row for the pane re-delivers another, so a retried request
// for an idle wake already used, or for an older one, re-delivers nothing and
// two rows are never typed into one idle turn: the node types the row, the
// pane works, and its next idle wake brings the next row.
func (h *HubServer) redeliverWorkingExpiredOnIdle(machine string, request idleWakeRouteRequest) {
	h.mu.Lock()
	remembered := len(h.relayControl.workingExpired)
	h.mu.Unlock()
	if remembered == 0 || h.handoffkeep == nil {
		return
	}
	routes, err := loadReportRelayRoutesResult(h.reportRelayPath)
	if err != nil {
		return
	}
	paneKey := machine + "\x00" + request.Pane
	h.mu.Lock()
	var pick int64
	for id, lane := range h.relayControl.workingExpired {
		route, ok := routes[lane]
		if ok && !route.Sink && route.Machine == machine && route.Pane == request.Pane && (pick == 0 || id < pick) {
			pick = id
		}
	}
	wake := idleRedeliveryWakeOf(request)
	previous, seen := h.relayControl.idleRedelivered[paneKey]
	if pick == 0 || seen && !wake.newerThan(previous) {
		h.mu.Unlock()
		return
	}
	lane := h.relayControl.workingExpired[pick]
	delete(h.relayControl.workingExpired, pick)
	if h.relayControl.idleRedelivered == nil {
		h.relayControl.idleRedelivered = make(map[string]idleRedeliveryWake)
	}
	h.relayControl.idleRedelivered[paneKey] = wake
	h.mu.Unlock()
	ctx := context.Background()
	record, found, err := h.handoffkeep.relayEvent(ctx, pick)
	if err != nil {
		// Nothing was spent: the next idle wake tries the row again.
		h.mu.Lock()
		h.relayControl.workingExpired[pick] = lane
		if seen {
			h.relayControl.idleRedelivered[paneKey] = previous
		} else {
			delete(h.relayControl.idleRedelivered, paneKey)
		}
		h.mu.Unlock()
		h.logger.Warn("idle re-delivery could not read its row", "event_id", pick)
		return
	}
	if !found {
		return
	}
	// The original send's injection claim outlives the node's drop: nothing
	// releases it until a restart. That send is over, so its claim is released
	// here and the replay takes it again; a claim another row holds stays.
	key := relayEventDedupeKey(record.Kind, relayEventFromRecord(record))
	h.mu.Lock()
	if h.relayDedupe[key] == record.ID {
		delete(h.relayDedupe, key)
	}
	h.mu.Unlock()
	h.replayMarkedUnconfirmed(ctx, record, relayReplaySourceIdle, false)
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
