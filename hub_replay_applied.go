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
	"time"
)

// hubCapabilityApplied is the capability a node advertises when it sends the
// applied report. A hub that predates it ignores an unknown capability name
// and replays at registration as before; the node's report then arrives as an
// unknown message type and is dropped.
const hubCapabilityApplied = "applied_v1"

// A missing or invalid report must not hold a node's replay indefinitely.
const hubAppliedReportTimeout = 5 * time.Second

// hubAppliedSettleBudget bounds the handoffkeep work one report may cause.
// Settlement runs outside replayMu (hk 4981), so a slow store delays only this
// node's replay trigger; the budget keeps the rows it names skipped by replay
// for a bounded time even when handoffkeep stops answering.
const hubAppliedSettleBudget = 30 * time.Second

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
		if !validLaneReference(lane) || len(ids) > relayAppliedMaxPerLane || total > relayAppliedMaxIDs {
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
//
// The handoffkeep calls that close the rows run outside replayMu (hk 4981):
// holding it across them made every other node's connect() — and so its hello
// — wait out the whole settlement. The ordering contract is kept by the
// pending flag, not the lock: replayAwaitsAppliedReport skips a lane.event row
// whose route is a registration with appliedPending set, and settlement leaves
// appliedPending set until the last row it names is closed. A replay that runs
// while this one settles therefore still counts only rows the report does not
// close, and the lock is taken only to claim the report, to refuse a report
// expireAppliedReport already released, and to release the replay afterwards.
func (h *HubServer) handleAppliedReport(machine string, registration *hubNodeRecord, applied map[string][]int64) {
	h.replayMu.Lock()
	h.mu.Lock()
	// appliedExpired is the report's refusal: expireAppliedReport has already
	// released a replay that re-offers these rows, and settling on top of that
	// would close rows a replay was told to send.
	settling := registration != nil && h.nodes[machine] == registration && registration.agent != nil &&
		!registration.appliedExpired && !registration.appliedSettling
	if settling {
		// The report holds the registration pending for the whole settlement,
		// so a replay that runs meanwhile still skips the rows it closes.
		registration.appliedSettling = true
		registration.appliedPending = true
		if registration.appliedTimer != nil {
			registration.appliedTimer.Stop()
		}
	}
	h.mu.Unlock()
	h.replayMu.Unlock()
	if !settling {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), hubAppliedSettleBudget)
	h.settleNodeApplied(ctx, machine, applied)
	cancel()
	h.replayMu.Lock()
	h.mu.Lock()
	// A disconnect can happen while the report is being settled.
	if h.nodes[machine] == registration {
		registration.appliedSettling = false
		registration.appliedPending = false
	}
	h.mu.Unlock()
	h.replayMu.Unlock()
	h.replayUndeliveredLaneEvents(context.Background())
}

// expireAppliedReport releases only the registration that started this timer.
// A late report is still accepted, but replay must eventually make progress.
func (h *HubServer) expireAppliedReport(machine string, registration *hubNodeRecord) {
	h.replayMu.Lock()
	h.mu.Lock()
	// A report that is already settling owns the release: it clears the flag
	// itself once its rows are closed, and expiring mid-settlement would let a
	// replay re-offer the rows it is about to close.
	pending := h.nodes[machine] == registration && registration.agent != nil && registration.appliedPending && !registration.appliedSettling
	if pending {
		registration.appliedPending = false
		registration.appliedExpired = true
	}
	h.mu.Unlock()
	h.replayMu.Unlock()
	if pending {
		h.logger.Warn("node applied report timed out; replaying without it", "machine", machine)
		h.replayUndeliveredLaneEvents(context.Background())
	}
}

// replayAwaitsAppliedReport checks the current destination before any replay
// side effects, including gap collection and retirement. replayMu keeps the
// registration stable for the whole walk.
func (h *HubServer) replayAwaitsAppliedReport(record handoffkeepRelayEvent) bool {
	if record.Kind != "lane.event" {
		return false
	}
	route, agent, routed := h.resolveRelayRoute(record.Kind, relayEventFromRecord(record))
	if !routed || agent == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	node := h.nodes[route.Machine]
	return node != nil && node.agent == agent && node.appliedPending
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
				// The ordinary close, as a late ack takes it: it also records the
				// delivery on the lane inbox item, and the ack window the hub may
				// still hold for this row is dropped after it, so no
				// relay.unconfirmed follows a row the report closed.
				pending := relayPending{machine: machine, pane: relayAppliedReportPane, eventID: record.ID, kind: record.Kind, event: relayEventFromRecord(record)}
				if !h.markRelayEventDeliveredContext(ctx, pending) {
					h.logger.Warn("applied report row was not closed", "machine", machine, "lane", lane, "event_id", record.ID)
					continue
				}
				h.forgetRelayAckEvent(record.ID, pending.event.JobID)
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
