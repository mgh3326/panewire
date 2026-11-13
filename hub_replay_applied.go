package panewire

// hk 3435, hk 2777: the node's durable relay_delivered record is the one fact
// that says a pane already showed a row. The relay.delivered ack that carries
// it to the hub is a socket event, and a socket that dies between the
// delivery and the ack leaves the hub row open — a restart then replays a
// batch the pane has already read. The node therefore repeats the fact in
// every registration (Store.RelayAppliedIDs, sent as the "applied" message
// right behind the hello) and the hub closes those rows before it replays.

import (
	"context"
	"encoding/json"
	"sort"
)

// hubCapabilityApplied is the capability a node advertises when it sends the
// applied report. A hub that predates it ignores an unknown capability name
// and replays at registration as before; the node's report then arrives as an
// unknown message type and is dropped.
const hubCapabilityApplied = "applied_v1"

// relayAppliedReportPane is the delivered_to pane of a row a node's hello
// closed, so the durable record shows why no ack named it.
const relayAppliedReportPane = "applied-report"

// decodeHubAppliedReport reads an applied report: lane → durable row ids.
// Anything outside the bounds the node itself keeps (relayApplied*) is a
// malformed message, not a report to truncate.
func decodeHubAppliedReport(raw json.RawMessage) (map[string][]int64, bool) {
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

// handleAppliedReport is the registration's second half: it closes what the
// node reports, then runs the replay the hello deferred.
func (h *HubServer) handleAppliedReport(machine string, applied map[string][]int64) {
	h.settleNodeApplied(context.Background(), machine, applied)
	h.replayUndeliveredLaneEvents(context.Background())
}

// settleNodeApplied closes, as delivered to machine, every undelivered row of
// a reported lane whose id the node lists as applied and that was routed to
// that machine: the row's own machine, or the lane's current route, exactly
// the entitlement a late relay.delivered ack needs (recordLateRelayDelivery).
// A listing or write failure leaves the row open: the replay then re-offers it
// and the node's own dedupe answers for it, as before.
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
				if record.Machine != machine {
					route, _, _ := h.resolveRelayRoute(record.Kind, relayEventFromRecord(record))
					if route.Sink || route.Machine != machine {
						h.logger.Warn("applied report names a row routed to another machine", "machine", machine, "lane", lane, "event_id", record.ID, "row_machine", record.Machine, "route_machine", route.Machine)
						continue
					}
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
