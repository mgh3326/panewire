package panewire

// A lane_inbox row records the stage its delivery reached, so the sender of a
// lane.event can ask `panewire inbox status --event-id ID` whether the text
// reached the pane instead of reading the hub journal (hk 3467). The stage is
// written on the row itself, so it survives a hub restart; ack-state exposes
// it together with the acked flag and the same sender scoping as before.

// The stage values ack-state reports. queued is the state of a row the hub has
// accepted but not yet injected; inbox_only is a row the lane reads from its
// inbox instead of receiving in the pane.
const (
	laneInboxStageQueued      = "queued"
	laneInboxStageHeld        = "held"
	laneInboxStageTyped       = "typed"
	laneInboxStageDelivered   = "delivered"
	laneInboxStageUnconfirmed = "unconfirmed"
	laneInboxStageDropped     = "dropped"
	laneInboxStageInboxOnly   = "inbox_only"
)

// laneInboxStageFor resolves the stage stored for a row. A row written before
// the stage columns keeps an empty stage; its delivered_at is the historical
// record of a confirmed delivery, and everything else was queued.
func laneInboxStageFor(stage string, deliveredAt int64) string {
	if stage != "" {
		return stage
	}
	if deliveredAt > 0 {
		return laneInboxStageDelivered
	}
	return laneInboxStageQueued
}

// laneInboxStageRank orders the stages so a report that arrives after a later
// one cannot move a row backwards: a delivered row never returns to an earlier
// stage, and a late held/typed report never overwrites an unconfirmed or
// dropped outcome.
func laneInboxStageRank(stage string) int {
	switch stage {
	case laneInboxStageHeld:
		return 1
	case laneInboxStageTyped:
		return 2
	case laneInboxStageUnconfirmed, laneInboxStageDropped:
		return 3
	case laneInboxStageDelivered:
		return 4
	}
	return 0
}

// recordInboxDelivered marks the event's row delivered without a live ack
// window, for a durable row handoffkeep already records as delivered.
func (h *HubServer) recordInboxDelivered(key, machine, pane string) {
	if h.inbox == nil || key == "" {
		return
	}
	if err := h.inbox.store.recordDelivery(key, machine, pane, h.now().UTC()); err != nil {
		h.logger.Warn("inbox delivery confirmation was not recorded")
	}
}

// recordInboxStage records a delivery stage on the lane_inbox row the event key
// names. It is best-effort operator signal: a failure is logged and never
// stalls the relay path.
func (h *HubServer) recordInboxStage(key, stage, reason, machine, pane string) {
	if h.inbox == nil || key == "" {
		return
	}
	if err := h.inbox.store.recordStage(key, stage, reason, machine, pane, h.now().UTC()); err != nil {
		h.logger.Warn("inbox delivery stage was not recorded", "stage", stage)
	}
}

// recordInboxStageByRelayID is recordInboxStage for the node reports that name
// the durable handoffkeep row (held, released, dropped) rather than the
// producer event id.
func (h *HubServer) recordInboxStageByRelayID(relayID int64, stage, reason, machine, pane string) {
	if h.inbox == nil || relayID <= 0 {
		return
	}
	if err := h.inbox.store.recordStageByRelayID(relayID, stage, reason, machine, pane, h.now().UTC()); err != nil {
		h.logger.Warn("inbox delivery stage was not recorded", "stage", stage)
	}
}

// linkInboxRelayID ties the durable handoffkeep row the hub persisted to the
// lane_inbox row for the same event, so a later node report that names only
// the durable id can update the delivery stage.
func (h *HubServer) linkInboxRelayID(key string, relayID int64) {
	if h.inbox == nil || key == "" || relayID <= 0 {
		return
	}
	if err := h.inbox.store.linkRelayID(key, relayID); err != nil {
		h.logger.Warn("inbox relay row link was not recorded")
	}
}
