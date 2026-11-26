package panewire

// hk 6900: a completed job keeps its lane route only while the node's latest
// fresh pane snapshot shows the job's own agent on the route's pane. Anything else
// ends the route job_ended. These tests drive the hub's relay path through the
// same reader-side call a node's lane.event takes (relayLaneEvent with the sending
// agent), with the verdict read from the snapshot the hub already holds.

import (
	"fmt"
	"testing"
	"time"
)

// t6900Route registers lane on node-a's live-pane, naming job as the route's job.
func t6900Route(lane, job string) string {
	return fmt.Sprintf(`{"lanes":{%q:{"machine":"node-a","pane":"live-pane","job":%q}}}`, lane, job)
}

// t6900Relay relays one lane.event to lane from node-a's agent and returns the result.
func t6900Relay(t *testing.T, h *HubServer, agent *hubAgent, lane, eventID string) relayLaneEventResult {
	t.Helper()
	event := hubJobEventPayload{JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, Text: "review requested"}
	result := h.relayLaneEvent(event, agent)
	if result.ID == 0 || result.PersistFailed {
		t.Fatalf("relay %s to %s: %+v", eventID, lane, result)
	}
	return result
}

// t6900Snapshot is a fresh, complete snapshot of node-a's panes at now.
func t6900Snapshot(now time.Time, sessions ...HubSession) *hubSessionSnapshotRecord {
	return &hubSessionSnapshotRecord{sessions: sessions, paneIDsComplete: true, snapshotStatus: hubSnapshotStatusOK, receivedAt: now}
}

// t6900SetSnapshot records snap as node-a's latest pane snapshot.
func t6900SetSnapshot(h *HubServer, snap *hubSessionSnapshotRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nodes["node-a"].sessionSnapshot = snap
}

// A completed job whose own agent holds its pane keeps its route: the lane event
// reaches the pane. The sender is the route's own node, the reader-path case.
// The design lane row is the desk/design case of hk 6613 item 1.
func TestCompletedJobRouteDeliversWhilePaneLives(t *testing.T) {
	for _, lane := range []string{"lane-a", "panewire-design"} {
		t.Run(lane, func(t *testing.T) {
			jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: lane, Pane: "live-pane"}, Node: "node-a", Completed: true}}
			h, _, agent := relayRegisteredRouteHub(t, t6900Route(lane, "job-x"), jobs)
			t6900SetSnapshot(h, t6900Snapshot(h.now(), HubSession{PaneID: "live-pane", Label: lane}))
			result := t6900Relay(t, h, agent, lane, "ev-live")
			if !result.Routed {
				t.Fatalf("a completed job whose agent holds its pane must keep its route: %+v", result)
			}
			if len(agent.relays) != 1 {
				t.Fatalf("live pane received %d injections, want 1", len(agent.relays))
			}
		})
	}
}

// A completed job's route on its own node, with the agent's socket never read by
// the hub (the reader-path case), is decided from the snapshot without waiting on
// the node: the event is routed promptly and the agent gets no command.
func TestCompletedRouteSameNodeEventNeverWaitsOnNode(t *testing.T) {
	jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
	h, _, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
	t6900SetSnapshot(h, t6900Snapshot(h.now(), HubSession{PaneID: "live-pane", Label: "lane-a"}))
	started := time.Now()
	result := t6900Relay(t, h, agent, "lane-a", "ev-same-node")
	if !result.Routed || time.Since(started) > 2*time.Second {
		t.Fatalf("same-node event: routed=%t elapsed=%s, want routed without waiting on the node", result.Routed, time.Since(started))
	}
}

// A recorded pane.close of the job's pane ends its route even while the snapshot
// still shows the agent: the event is dropped job_ended and gets no injection.
func TestRecordedPaneCloseEndsCompletedJobRoute(t *testing.T) {
	jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
	h, server, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
	t6900SetSnapshot(h, t6900Snapshot(h.now(), HubSession{PaneID: "live-pane", Label: "lane-a"}))
	closed := hubNodeCommandResult{Result: []byte(`{"outcome":"closed"}`)}
	h.releaseClosedPaneLanes("node-a", nodePaneCloseArgs{PaneID: "live-pane"}, closed, time.Now())
	h.mu.Lock()
	gone := h.jobs["job-x"].PaneGone
	h.mu.Unlock()
	if !gone {
		t.Fatal("pane.close outcome closed did not record the job's pane as gone")
	}
	result := t6900Relay(t, h, agent, "lane-a", "ev-closed")
	if result.Routed || len(agent.relays) != 0 {
		t.Fatalf("a closed pane took a lane event: result=%+v injections=%d", result, len(agent.relays))
	}
	state := deliveryState(t, server, pw2Caller{"node-a", "node-a", ackStateNodeAToken}, "ev-closed")
	if reason, _ := state["reason"].(string); state["stage"] != laneInboxStageDropped || reason != relayDroppedJobEnded {
		t.Fatalf("stage=%v reason=%q, want dropped job_ended", state["stage"], reason)
	}
}

// The job's agent is not on the route's pane. Each case ends the route, so the
// event is dropped job_ended and the pane gets no injection.
func TestCompletedRouteEndedWhenJobAgentDoesNotHoldPane(t *testing.T) {
	cases := []struct {
		name string
		snap func(now time.Time) *hubSessionSnapshotRecord
	}{
		{name: "the pane is unlisted", snap: func(now time.Time) *hubSessionSnapshotRecord {
			return t6900Snapshot(now, HubSession{PaneID: "other-pane", Label: "other-agent"})
		}},
		{name: "the pane id is reused by another agent", snap: func(now time.Time) *hubSessionSnapshotRecord {
			return t6900Snapshot(now, HubSession{PaneID: "live-pane", Label: "other-agent"})
		}},
		{name: "the job's label is listed on another pane", snap: func(now time.Time) *hubSessionSnapshotRecord {
			return t6900Snapshot(now, HubSession{PaneID: "successor-pane", Label: "lane-a"})
		}},
		{name: "the snapshot is stale", snap: func(now time.Time) *hubSessionSnapshotRecord {
			return t6900Snapshot(now.Add(-sessionsFindSnapshotMaxAge-time.Second), HubSession{PaneID: "live-pane", Label: "lane-a"})
		}},
		{name: "the snapshot is truncated", snap: func(now time.Time) *hubSessionSnapshotRecord {
			snap := t6900Snapshot(now, HubSession{PaneID: "live-pane", Label: "lane-a"})
			snap.truncated = true
			return snap
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
			h, server, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
			t6900SetSnapshot(h, test.snap(h.now()))
			result := t6900Relay(t, h, agent, "lane-a", "ev-not-held")
			if result.Routed || len(agent.relays) != 0 {
				t.Fatalf("a pane the job's agent does not hold took a lane event: result=%+v injections=%d", result, len(agent.relays))
			}
			state := deliveryState(t, server, pw2Caller{"node-a", "node-a", ackStateNodeAToken}, "ev-not-held")
			if reason, _ := state["reason"].(string); state["stage"] != laneInboxStageDropped || reason != relayDroppedJobEnded {
				t.Fatalf("stage=%v reason=%q, want dropped job_ended", state["stage"], reason)
			}
		})
	}
}

// A registered route whose node has no live agent resolves to no route, which
// the route-less drop handles as before: the completed job's event is dropped
// job_ended, as it was before hk 6900.
func TestCompletedRouteWithoutLiveAgentIsDroppedAsBefore(t *testing.T) {
	jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
	h, server, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
	h.mu.Lock()
	h.nodes["node-a"] = &hubNodeRecord{machineID: "node-a", state: "connected", sessionSnapshot: t6900Snapshot(h.now(), HubSession{PaneID: "live-pane", Label: "lane-a"})}
	h.mu.Unlock()
	result := t6900Relay(t, h, agent, "lane-a", "ev-no-agent")
	if result.Routed {
		t.Fatalf("a route with no live agent routed: %+v", result)
	}
	state := deliveryState(t, server, pw2Caller{"node-a", "node-a", ackStateNodeAToken}, "ev-no-agent")
	if reason, _ := state["reason"].(string); state["stage"] != laneInboxStageDropped || reason != relayDroppedJobEnded {
		t.Fatalf("stage=%v reason=%q, want dropped job_ended", state["stage"], reason)
	}
}

// The verdict follows the latest snapshot: held, then not held, then held again.
func TestCompletedRouteVerdictFollowsLatestSnapshot(t *testing.T) {
	jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
	h, _, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
	held := []HubSession{{PaneID: "live-pane", Label: "lane-a"}}
	t6900SetSnapshot(h, t6900Snapshot(h.now(), held...))
	if result := t6900Relay(t, h, agent, "lane-a", "ev-held-1"); !result.Routed {
		t.Fatalf("held pane: %+v", result)
	}
	t6900SetSnapshot(h, t6900Snapshot(h.now()))
	if result := t6900Relay(t, h, agent, "lane-a", "ev-unlisted"); result.Routed {
		t.Fatalf("unlisted pane routed: %+v", result)
	}
	t6900SetSnapshot(h, t6900Snapshot(h.now(), held...))
	if result := t6900Relay(t, h, agent, "lane-a", "ev-held-2"); !result.Routed {
		t.Fatalf("pane held again: %+v", result)
	}
}

// A stale route row names a pane the job's agent does not hold. That ends the
// event on that route, but must not mark the job's own pane gone: once the row is
// repointed to the pane the agent holds, the next event is delivered.
func TestCompletedRouteRepointedToLivePaneDeliversAfterStaleRow(t *testing.T) {
	jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
	h, _, agent := relayRegisteredRouteHub(t, `{"lanes":{"lane-a":{"machine":"node-a","pane":"old-pane","job":"job-x"}}}`, jobs)
	t6900SetSnapshot(h, t6900Snapshot(h.now(), HubSession{PaneID: "live-pane", Label: "lane-a"}))
	if result := t6900Relay(t, h, agent, "lane-a", "ev-stale-row"); result.Routed {
		t.Fatalf("a stale row routed to a pane the job does not hold: %+v", result)
	}
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"node-a","pane":"live-pane","job":"job-x"}}}`)
	if result := t6900Relay(t, h, agent, "lane-a", "ev-repointed"); !result.Routed {
		t.Fatalf("after the row was repointed to the live pane, the event must deliver: %+v", result)
	}
	h.mu.Lock()
	gone := h.jobs["job-x"].PaneGone
	h.mu.Unlock()
	if gone {
		t.Fatal("a stale route row marked the job's own pane gone")
	}
}

// A takeover route names its successor job. A completed predecessor that shares
// the pane and whose pane is recorded gone must not end the successor's route.
func TestTakeoverRouteDeliversToSuccessorDespiteReapedPredecessor(t *testing.T) {
	jobs := map[string]*hubJobRecord{
		"predecessor": {HubActiveJob: HubActiveJob{JobID: "predecessor", AgentLabel: "lane-a", Pane: "successor-pane"}, Node: "node-a", Completed: true, PaneGone: true},
		"successor":   {HubActiveJob: HubActiveJob{JobID: "successor", AgentLabel: "successor-label", Pane: "successor-pane"}, Node: "node-a"},
	}
	lanes := `{"lanes":{"lane-a":{"machine":"node-a","pane":"successor-pane","job":"successor","taken_over_from":"predecessor"}}}`
	h, _, agent := relayRegisteredRouteHub(t, lanes, jobs)
	result := t6900Relay(t, h, agent, "lane-a", "ev-takeover-6900")
	if !result.Routed {
		t.Fatalf("a takeover route with a live successor must deliver: %+v", result)
	}
	if got := (<-agent.relays).Pane; got != "successor-pane" {
		t.Fatalf("injected pane=%q, want successor-pane", got)
	}
}
