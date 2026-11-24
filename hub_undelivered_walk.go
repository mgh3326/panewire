package panewire

import (
	"context"
	"time"
)

// walkUndeliveredRows pages every undelivered relay row, across all lanes, in
// id order over handoffkeep's after_id cursor, and hands each row to visit.
// One walk replaces a lane-scoped listing per lane: a caller that wants one
// lane keeps the rows whose OwnerLane matches. A failed page returns its error
// and the rows visited so far are not a complete answer. A page that does not
// advance the cursor ends the walk with a warning, as the lane-scoped walks
// always did.
func (h *HubServer) walkUndeliveredRows(ctx context.Context, visit func(handoffkeepRelayEvent)) error {
	var afterID int64
	for {
		pageStart := afterID
		records, err := h.handoffkeep.listUndelivered(ctx, "", "", afterID, handoffkeepReplayLimit)
		if err != nil {
			return err
		}
		for _, record := range records {
			visit(record)
			if record.ID > afterID {
				afterID = record.ID
			}
		}
		if len(records) < handoffkeepReplayLimit {
			return nil
		}
		if afterID <= pageStart {
			h.logger.Warn("undelivered walk cursor did not advance", "after_id", pageStart)
			return nil
		}
	}
}

// observeLanesUndelivered reads the durable backlog of every listed lane with
// one fleet-wide walk, split by each row's OwnerLane. Each returned observation
// equals what a lane-scoped walk of that lane alone would have produced. A
// failed walk leaves every lane unobserved, so a transient read error cannot
// declare or end an episode. Lanes absent from the returned map are unobserved.
func (h *HubServer) observeLanesUndelivered(ctx context.Context, lanes []string, now time.Time) map[string]laneStallObservation {
	if len(lanes) == 0 {
		return nil
	}
	obs := make(map[string]laneStallObservation, len(lanes))
	for _, lane := range lanes {
		obs[lane] = laneStallObservation{lane: lane}
	}
	err := h.walkUndeliveredRows(ctx, func(record handoffkeepRelayEvent) {
		o, wanted := obs[record.OwnerLane]
		if !wanted {
			return
		}
		o.consider(record, now)
		obs[record.OwnerLane] = o
	})
	if err != nil {
		for _, lane := range lanes {
			h.logger.Warn("lane stall check could not read undelivered relay events", "lane", lane)
		}
		return nil
	}
	for lane, o := range obs {
		o.observed = true
		obs[lane] = o
	}
	return obs
}
