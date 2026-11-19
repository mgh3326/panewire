package panewire

// hk 5307: a durable lane.event row whose lane has no route stays undelivered
// until its replay window ends (relayReplayMaxAge). Every node hello and every
// applied-report replay walks the undelivered rows again, and each walk found
// the same unroutable row and announced it again: a relay.unrouted broadcast
// and a "relay unrouted" WARN every time the walk ran. The row itself must stay
// open, since a route that returns still delivers it, so only its announcement
// is spent once per row.

// announceUnroutedReplay announces a row a replay walk found unroutable. The
// first walk to find the row announces it; later walks leave it quiet and keep
// it open. The set is bounded like replayRetired: forgetting an old row can
// only repeat its announcement once, never re-deliver anything.
func (h *HubServer) announceUnroutedReplay(rowID int64, event hubJobEventPayload) {
	if !h.claimUnroutedReplayAnnounce(rowID) {
		return
	}
	h.broadcastRelayUnrouted(event)
}

// claimUnroutedReplayAnnounce reports whether rowID is announced for the first
// time by this hub process.
func (h *HubServer) claimUnroutedReplayAnnounce(rowID int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, already := h.replayUnrouted[rowID]; already {
		h.replayUnroutedOrder.touch(rowID, relayReplayUnroutedMaxEntries)
		return false
	}
	if h.replayUnrouted == nil {
		h.replayUnrouted = make(map[int64]struct{})
	}
	_, evicted, overflowed := h.replayUnroutedOrder.touch(rowID, relayReplayUnroutedMaxEntries)
	if overflowed {
		delete(h.replayUnrouted, evicted)
	}
	h.replayUnrouted[rowID] = struct{}{}
	return true
}
