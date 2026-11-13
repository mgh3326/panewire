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
// and a lane only drains after it has had work. The owed line and the last
// send are kept in the inbox store (drain_wake_lanes), so a hub restart keeps
// a line that is still owed.
func (h *HubServer) sweepDrainWakes(ctx context.Context, now time.Time) {
	if h.inbox == nil || h.inbox.policy == nil {
		return
	}
	if h.drainWake == nil {
		owed, err := h.inbox.store.drainWakeOwed(ctx)
		if err != nil {
			h.logger.Warn("drain wake skipped: owed lines unreadable; retrying on the next sweep", "error", err)
			return
		}
		h.drainWake = owed
	}
	if h.drainWakeUnreported() {
		// A connected host has not heartbeated its active jobs since its hello,
		// so every lane count is a lower bound. Observe nothing; keep each lane's state.
		return
	}
	routes := loadReportRelayRoutes(h.reportRelayPath)
	for lane := range routes {
		if ctx.Err() != nil {
			// Budget spent: the unobserved lanes keep their state for the next sweep.
			return
		}
		if !h.inbox.policy.designLane(lane) {
			continue
		}
		state := h.drainWake[lane]
		if state == nil {
			state = &drainWakeLane{}
			h.drainWake[lane] = state
		}
		before := *state
		h.drainWakeLaneStep(ctx, lane, state, routes, now)
		if state.pending != before.pending || !state.lastSent.Equal(before.lastSent) {
			if err := h.inbox.store.saveDrainWake(ctx, lane, state); err != nil {
				h.logger.Warn("drain wake state not saved; a restart may lose the owed line", "lane", lane, "error", err)
			}
		}
	}
}

// drainWakeLaneStep advances one lane's drain state by one observation.
func (h *HubServer) drainWakeLaneStep(ctx context.Context, lane string, state *drainWakeLane, routes map[string]reportRelayRoute, now time.Time) {
	policy := h.inbox.policy
	threshold := policy.Relay.DrainWakeThreshold
	interval := policy.Relay.drainWakeInterval
	inFlight := h.laneActiveJobCount(lane, routes)
	if inFlight > threshold {
		state.known, state.drained, state.pending = true, false, false
		return
	}
	if state.known && !state.drained {
		state.pending = true
	}
	state.known, state.drained = true, true
	if !state.pending {
		return
	}
	if !state.lastSent.IsZero() && now.Sub(state.lastSent) < interval {
		return
	}
	route, agent, routed := h.resolveRelayRoute("lane.event", hubJobEventPayload{OwnerLane: lane})
	if !routed || route.Sink || route.Pane == "" || agent == nil {
		// Unrouted: the line stays owed and the next sweep retries.
		return
	}
	backlog, err := h.drainWakeBacklog(ctx, drainWakeProject(lane))
	if err != nil {
		h.logger.Warn("drain wake skipped: hk backlog unavailable; retrying on the next sweep", "lane", lane, "error", err)
		return
	}
	if backlog == 0 {
		// Nothing open for the lane's project: the drain owes no line.
		state.pending = false
		return
	}
	text := drainWakeText(backlog, inFlight)
	if !agent.queueRelay(hubRelayInjectEvent{Type: "relay.inject", JobID: laneEventTransportID(lane, "drain-wake"), Pane: route.Pane, Text: text}) {
		h.emitFleet(fleetEvent{Kind: "drain_wake", Job: lane, Outcome: "skipped", Reason: "unrouted", Detail: "lane=" + lane})
		return
	}
	state.pending = false
	state.lastSent = now
	h.emitFleet(fleetEvent{Kind: "drain_wake", Job: lane, Outcome: "sent", Reason: "drained", Detail: fmt.Sprintf("lane=%s backlog=%d in_flight=%d pane=%s", lane, backlog, inFlight, route.Pane)})
}

// drainWakeUnreported reports whether a connected host has not yet heartbeated
// its active jobs since its hello. The hello empties activeJobs, so until that
// heartbeat a drain read is not a drain (hk 4567).
func (h *HubServer) drainWakeUnreported() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, record := range h.nodes {
		if record.state == "connected" && !record.activeReported {
			return true
		}
	}
	return false
}

// drainWakeOwed reads the owed lines and last sends. A lane's known and drained
// flags are not stored: a restored lane is treated as drained, so only an owed
// line carries over, and a lane first seen after the restart still gets no line
// until it drains.
func (s *laneInboxStore) drainWakeOwed(ctx context.Context) (map[string]*drainWakeLane, error) {
	ctx, cancel := s.op(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT lane,pending,last_sent FROM drain_wake_lanes`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	owed := make(map[string]*drainWakeLane)
	for rows.Next() {
		var lane string
		var pending, sentMS int64
		if err := rows.Scan(&lane, &pending, &sentMS); err != nil {
			return nil, err
		}
		state := &drainWakeLane{known: true, drained: true, pending: pending != 0}
		if sentMS > 0 {
			state.lastSent = time.UnixMilli(sentMS).UTC()
		}
		owed[lane] = state
	}
	return owed, rows.Err()
}

// saveDrainWake writes one lane's owed line and last send.
func (s *laneInboxStore) saveDrainWake(ctx context.Context, lane string, state *drainWakeLane) error {
	ctx, cancel := s.op(ctx)
	defer cancel()
	var pending, sentMS int64
	if state.pending {
		pending = 1
	}
	if !state.lastSent.IsZero() {
		sentMS = state.lastSent.UnixMilli()
	}
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO drain_wake_lanes(lane,pending,last_sent) VALUES(?,?,?) ON CONFLICT(lane) DO UPDATE SET pending=excluded.pending,last_sent=excluded.last_sent`), lane, pending, sentMS)
	return err
}

// drainWakeOpenStates are the hk task states that count as a lane's open
// backlog: the two pre-start states (hk internal/store/states.go). backlog is
// the queued work; unstarted is hk's open pre-start state. hk has no "ready".
var drainWakeOpenStates = []string{"backlog", "unstarted"}

// drainWakeBacklog counts hk tasks for project in each open state, leaving out
// the ones awaiting an operator decision. hk filters project and state server
// side; the awaiting check happens here.
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

// drainWakeStateCount reads every page of one project and state in ID order and
// counts the tasks not awaiting an operator decision (decision 233). An
// unadvancing cursor is an error, not a partial count.
func (h *HubServer) drainWakeStateCount(ctx context.Context, project, state string) (int, error) {
	count := 0
	var afterID int64
	for {
		tasks, err := h.handoffkeep.listTasks(ctx, project, state, afterID, drainWakeTaskLimit)
		if err != nil {
			return 0, err
		}
		for _, task := range tasks {
			if !task.AwaitingDecision {
				count++
			}
		}
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
