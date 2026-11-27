package panewire

// Idle re-delivery on a hub with no inbox store (--delivery-policy off, or no
// store): it keeps no delivery obligations, so the rows a node dropped as
// working_wait_expired are remembered in memory once their unconfirmed mark is
// durable, and re-delivered through the marked row's one labelled replay. The
// memory is not durable; a restart replays every marked row anyway.

import (
	"context"
	"strings"
	"time"
)

const (
	// relayWorkingExpiredMaxRows bounds the rows the hub remembers. Past it a
	// new row is left to the restart and hello replays.
	relayWorkingExpiredMaxRows = 4096
	// relayWorkingExpiredMaxTries bounds the idle wakes one row may use without
	// being spent. A row that stays open past it (exhausted, unroutable) would
	// otherwise be picked first at every wake and starve the rows behind it;
	// it is left to the restart and hello replays.
	relayWorkingExpiredMaxTries = 3
)

// legacyIdleRow is one remembered row: its lane, when the node dropped it, how
// many idle wakes tried it without the replay spending it, and whether a wake is
// trying it now. The drop time bounds the re-delivery's age: the row's receipt
// can be up to relay.cmd_hold_max older, since the node held it that long.
type legacyIdleRow struct {
	lane      string
	droppedAt time.Time
	tries     int
	claim     *byte
}

// rememberLegacyIdleRowLocked records a row the node dropped as
// working_wait_expired once its unconfirmed mark is durable. Only lane.event
// rows are remembered: they are the rows the marked replay re-delivers.
func (h *HubServer) rememberLegacyIdleRowLocked(pending relayPending, ack relayAckPayload) {
	if h.inbox != nil || ack.Reason != relayDroppedWorkingExpired || pending.kind != "lane.event" || pending.event.OwnerLane == "" {
		return
	}
	id := pending.eventID
	if id == 0 {
		id = ack.OriginalEventID
	}
	if id < 1 {
		return
	}
	if h.relayControl.legacyIdle == nil {
		h.relayControl.legacyIdle = make(map[int64]legacyIdleRow)
	}
	if len(h.relayControl.legacyIdle) >= relayWorkingExpiredMaxRows {
		return
	}
	h.relayControl.legacyIdle[id] = legacyIdleRow{lane: pending.event.OwnerLane, droppedAt: h.now().UTC()}
}

// redeliverLegacyWorkingExpiredOnIdle re-delivers the oldest remembered row
// whose lane routes to the pane, once. Only a wake newer than the last one that
// re-delivered a row for the pane re-delivers another, so two rows are never
// typed into one idle turn: the node types the row, the pane works, and its
// next idle wake brings the next row.
func (h *HubServer) redeliverLegacyWorkingExpiredOnIdle(machine string, request idleWakeRouteRequest) {
	h.mu.Lock()
	remembered := len(h.relayControl.legacyIdle)
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
	// A row past the replay window is dropped here rather than picked: the
	// restart replay retires it, and the rows behind it are still due this wake
	// (hk 6935).
	now := h.now().UTC()
	var pick int64
	for id, row := range h.relayControl.legacyIdle {
		route, ok := routes[row.lane]
		if row.claim != nil || !ok || route.Sink || route.Machine != machine || route.Pane != request.Pane {
			continue
		}
		if now.Sub(row.droppedAt) > relayReplayMaxAge {
			delete(h.relayControl.legacyIdle, id)
			continue
		}
		if pick == 0 || id < pick {
			pick = id
		}
	}
	wake := idleRedeliveryWakeOf(request)
	previous, seen := h.relayControl.idleRedelivered[paneKey]
	if pick == 0 || seen && !wake.newerThan(previous) {
		h.mu.Unlock()
		return
	}
	row := h.relayControl.legacyIdle[pick]
	if h.relayControl.idleRedelivered == nil {
		h.relayControl.idleRedelivered = make(map[string]idleRedeliveryWake)
	}
	h.relayControl.idleRedelivered[paneKey] = wake
	claim := new(byte)
	row.claim = claim
	h.relayControl.legacyIdle[pick] = row
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.relayControl.legacyIdle[pick].claim == claim {
			delete(h.relayControl.legacyIdle, pick)
		}
	}()
	// restore puts the row and the pane's previous wake back when this wake
	// spent nothing, so a later wake (or a retry of this one) tries again.
	restore := func(why string) {
		row.tries++
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.relayControl.idleRedelivered[paneKey] == wake {
			if seen {
				h.relayControl.idleRedelivered[paneKey] = previous
			} else {
				delete(h.relayControl.idleRedelivered, paneKey)
			}
		}
		if row.tries >= relayWorkingExpiredMaxTries {
			h.logger.Warn("idle re-delivery gave up; left to the restart replay", "event_id", pick, "why", why)
			return
		}
		h.logger.Warn("idle re-delivery will retry at the next idle wake", "event_id", pick, "why", why)
		row.claim = nil
		h.relayControl.legacyIdle[pick] = row
	}
	ctx := context.Background()
	record, found, err := h.handoffkeep.relayEvent(ctx, pick)
	if err != nil {
		restore("row_read_failed")
		return
	}
	if !found {
		return
	}
	key := relayEventDedupeKey(record.Kind, relayEventFromRecord(record))
	// The idle path also replays a row; serialize it with registration just
	// like the startup and hello walks, and retry after the report arrives.
	h.replayMu.RLock()
	defer h.replayMu.RUnlock()
	if h.replayAwaitsAppliedReport(record) {
		restore("applied_report_pending")
		return
	}
	// The original send's injection claim outlives the node's drop: nothing
	// releases it until a restart. That send is over, so its claim is released
	// here and the replay takes it again; a claim another row holds stays.
	h.mu.Lock()
	if h.relayDedupe[key] == record.ID {
		delete(h.relayDedupe, key)
	}
	h.mu.Unlock()
	h.replayLegacyUnconfirmed(ctx, record, relayReplaySourceIdle, false)
	// The replay reports nothing back. It spends a row by closing it (the
	// retire it writes before anything is queued, or a delivery), so a row
	// still open afterwards was not spent: a failed retire, a failed re-read,
	// a deferred inbox lookup. An unreadable row cannot be proven spent; the
	// next try re-reads it before it queues anything.
	after, found, err := h.handoffkeep.relayEvent(ctx, pick)
	switch {
	case err != nil:
		restore("row_reread_failed")
	case found && after.DeliveredAt == "" && !strings.HasPrefix(after.DeliveredTo, relayReplayRetiredMarker):
		restore("row_not_spent")
	}
}
