package panewire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Drain wake (hk 4473, operator decision 233). A design lane whose in-flight
// work falls to relay.drain_wake_threshold while hk still lists open backlog
// for its project gets one line typed into its pane: "N backlog, M in work:
// dispatch the next batch". The line goes out on the drain edge, and at most
// once per relay.drain_wake_interval. It is an interim until hk 4376 (a
// hub-owned queue) replaces the hk read. Policy: docs/quiet-delivery.md.

// drainWakeTaskLimit is the hk page size for the backlog read. The read
// follows the cursor until a short page.
const drainWakeTaskLimit = 1000

// drainWakeLane is one design lane's drain state. Only the lane-stall sweep
// goroutine touches it, under laneStallMu.
type drainWakeLane struct {
	known    bool      // an observation has been taken
	drained  bool      // the last observation was at or below the threshold
	pending  bool      // drained since the last line and still owed one
	lastSent time.Time // when the last line went out; zero before the first
}

// sweepDrainWakes runs in the lane-stall sweep goroutine after the stall
// observation. A lane first seen idle gets no line: the line answers a drain,
// and a lane only drains after it has had work.
func (h *HubServer) sweepDrainWakes(ctx context.Context, now time.Time) {
	if h.inbox == nil || h.inbox.policy == nil {
		return
	}
	policy := h.inbox.policy
	threshold := policy.Relay.DrainWakeThreshold
	interval := policy.Relay.drainWakeInterval
	routes := loadReportRelayRoutes(h.reportRelayPath)
	if h.drainWake == nil {
		h.drainWake = make(map[string]*drainWakeLane)
	}
	for lane := range routes {
		if ctx.Err() != nil {
			// Budget spent: the unobserved lanes keep their state for the next sweep.
			return
		}
		if !policy.designLane(lane) {
			continue
		}
		state := h.drainWake[lane]
		if state == nil {
			state = &drainWakeLane{}
			h.drainWake[lane] = state
		}
		inFlight := h.laneActiveJobCount(lane, routes)
		if inFlight > threshold {
			state.known, state.drained, state.pending = true, false, false
			continue
		}
		if state.known && !state.drained {
			state.pending = true
		}
		state.known, state.drained = true, true
		if !state.pending {
			continue
		}
		if !state.lastSent.IsZero() && now.Sub(state.lastSent) < interval {
			continue
		}
		route, agent, routed := h.resolveRelayRoute("lane.event", hubJobEventPayload{OwnerLane: lane})
		if !routed || route.Sink || route.Pane == "" || agent == nil {
			// Unrouted: the line stays owed and the next sweep retries.
			continue
		}
		backlog, err := h.drainWakeBacklog(ctx, drainWakeProject(lane))
		if err != nil {
			h.logger.Warn("drain wake skipped: hk backlog unavailable; retrying on the next sweep", "lane", lane, "error", err)
			continue
		}
		if backlog == 0 {
			// Nothing open for the lane's project: the drain owes no line.
			state.pending = false
			continue
		}
		text := drainWakeText(backlog, inFlight)
		if !agent.queueRelay(hubRelayInjectEvent{Type: "relay.inject", JobID: laneEventTransportID(lane, "drain-wake"), Pane: route.Pane, Text: text}) {
			h.emitFleet(fleetEvent{Kind: "drain_wake", Job: lane, Outcome: "skipped", Reason: "unrouted", Detail: "lane=" + lane})
			continue
		}
		state.pending = false
		state.lastSent = now
		h.emitFleet(fleetEvent{Kind: "drain_wake", Job: lane, Outcome: "sent", Reason: "drained", Detail: fmt.Sprintf("lane=%s backlog=%d in_flight=%d pane=%s", lane, backlog, inFlight, route.Pane)})
	}
}

// drainWakeOpenStates are the hk task states that count as a lane's open
// backlog: the two pre-start states (hk internal/store/states.go). backlog is
// the queued work; unstarted is hk's open pre-start state. hk has no "ready".
var drainWakeOpenStates = []string{"backlog", "unstarted"}

// drainWakeBacklog counts hk tasks for project in each open state. hk filters
// by project and state server side, so each page is already the answer.
func (h *HubServer) drainWakeBacklog(ctx context.Context, project string) (int, error) {
	total := 0
	for _, state := range drainWakeOpenStates {
		count, err := h.drainWakeStateCount(ctx, project, state)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

// drainWakeStateCount reads every page of one project and state in ID order.
// An unadvancing cursor is an error, not a partial count.
func (h *HubServer) drainWakeStateCount(ctx context.Context, project, state string) (int, error) {
	count := 0
	var afterID int64
	for {
		tasks, err := h.handoffkeep.listTasks(ctx, project, state, afterID, drainWakeTaskLimit)
		if err != nil {
			return 0, err
		}
		count += len(tasks)
		if len(tasks) < drainWakeTaskLimit {
			return count, nil
		}
		next := tasks[len(tasks)-1].ID
		if next <= afterID {
			return 0, errors.New("handoffkeep task cursor did not advance")
		}
		afterID = next
	}
}

// drainWakeProject maps a design lane to its hk project: the lane name without
// the -design suffix (panewire-design → panewire).
func drainWakeProject(lane string) string {
	return strings.TrimSuffix(lane, "-design")
}

func drainWakeText(backlog, inFlight int) string {
	return fmt.Sprintf("%d backlog, %d in work: dispatch the next batch", backlog, inFlight)
}
