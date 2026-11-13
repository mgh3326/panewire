package panewire

// hk 3435, hk 2777: the node's durable relay_delivered record is the one fact
// that says a pane already showed a row. The relay.delivered ack that carries
// it to the hub is a socket event, and a socket that dies between the
// delivery and the ack leaves the hub row open — a restart then replays a
// batch the pane has already read. The node therefore repeats the fact in
// every hello (Store.RelayAppliedIDs) and the registration closes those rows
// before it replays.

import (
	"context"
	"encoding/json"
	"sort"
)

// relayAppliedReportPane is the delivered_to pane of a row a node's hello
// closed, so the durable record shows why no ack named it.
const relayAppliedReportPane = "applied-report"

// decodeHubHelloApplied reads a hello's applied report: lane → durable row
// ids. Anything outside the bounds the node itself keeps (relayApplied*) is a
// malformed hello, not a report to truncate.
func decodeHubHelloApplied(raw json.RawMessage) (map[string][]int64, bool) {
	var applied map[string][]int64
	if json.Unmarshal(raw, &applied) != nil || len(applied) > relayAppliedMaxLanes {
		return nil, false
	}
	total := 0
	for lane, ids := range applied {
		total += len(ids)
		if !validReportRelayLaneName(lane) || len(ids) > relayAppliedMaxPerLane || total > relayAppliedMaxIDs {
			return nil, false
		}
		for _, id := range ids {
			if id < 1 {
				return nil, false
			}
		}
	}
	if len(applied) == 0 {
		return nil, true
	}
	return applied, true
}

// settleNodeApplied closes, as delivered to machine, every undelivered row of
// a reported lane whose id the node lists as applied. A listing or write
// failure leaves the row open: the replay then re-offers it and the node's own
// dedupe answers for it, as before.
func (h *HubServer) settleNodeApplied(ctx context.Context, machine string, applied map[string][]int64) {
	if h.handoffkeep == nil || len(applied) == 0 {
		return
	}
	lanes := make([]string, 0, len(applied))
	for lane := range applied {
		lanes = append(lanes, lane)
	}
	sort.Strings(lanes)
	for _, lane := range lanes {
		reported := make(map[int64]bool, len(applied[lane]))
		for _, id := range applied[lane] {
			reported[id] = true
		}
		closed := 0
		var afterID int64
		for {
			pageStart := afterID
			records, err := h.handoffkeep.listUndelivered(ctx, lane, "", afterID, handoffkeepReplayLimit)
			if err != nil {
				h.logger.Warn("applied report could not list undelivered rows", "machine", machine, "lane", lane)
				break
			}
			for _, record := range records {
				if record.ID > afterID {
					afterID = record.ID
				}
				if !reported[record.ID] || record.DeliveredAt != "" || isUnconfirmedMarkRecord(record) {
					continue
				}
				if err := h.handoffkeep.markDelivered(ctx, record.ID, machine, relayAppliedReportPane); err != nil {
					h.logger.Warn("applied report row was not closed", "machine", machine, "lane", lane, "event_id", record.ID)
					continue
				}
				closed++
			}
			if len(records) < handoffkeepReplayLimit || afterID <= pageStart {
				break
			}
		}
		if closed > 0 {
			h.logger.Info("rows closed from the node's applied report", "machine", machine, "lane", lane, "rows", closed)
		}
	}
}
