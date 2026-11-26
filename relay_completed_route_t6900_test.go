package panewire

// hk 6900: a completed job keeps its lane route only while the job's own agent
// holds the route's pane. Anything else ends the route job_ended. These tests
// drive the hub's relay path with the node probe replaced by a fixture seam
// (relay_route_pane.go).

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
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

// t6900Occupant is the seam answer for a pane the job's own agent holds.
func t6900Occupant(_ context.Context, _ string, pane, label string) heldProbe {
	return heldProbe{state: heldPanePresent, row: nodeAgentRow{PaneID: pane, Name: label}}
}

// unreachableRoutePaneProbe is the seam answer for a node that cannot be asked.
// Rigs whose agents hold no connection use it: a probe there would write to a
// nil socket, and no answer ends the route, as an unanswered probe does.
func unreachableRoutePaneProbe(context.Context, string, string, string) heldProbe {
	return heldProbe{reason: "node_unreachable"}
}

// A completed job whose own agent holds its pane keeps its route: the lane event
// reaches the pane. The design lane row is the desk/design case of hk 6613 item 1.
func TestCompletedJobRouteDeliversWhilePaneLives(t *testing.T) {
	for _, lane := range []string{"lane-a", "panewire-design"} {
		t.Run(lane, func(t *testing.T) {
			jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: lane, Pane: "live-pane"}, Node: "node-a", Completed: true}}
			h, _, agent := relayRegisteredRouteHub(t, t6900Route(lane, "job-x"), jobs)
			h.routePaneProbe = t6900Occupant
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

// A recorded pane.close of the job's pane ends its route with no probe: the event
// is dropped job_ended and the pane gets no injection.
func TestRecordedPaneCloseEndsCompletedJobRoute(t *testing.T) {
	jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
	h, server, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
	h.routePaneProbe = func(context.Context, string, string, string) heldProbe {
		t.Error("a recorded close must not need a probe")
		return heldProbe{}
	}
	closed := hubNodeCommandResult{Result: json.RawMessage(`{"outcome":"closed"}`)}
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

// The job's agent does not hold the route's pane: the pane is unlisted, reused by
// another agent, or the job's label is listed only on another pane. Each ends the
// route, so the event is dropped job_ended and the pane gets no injection.
func TestCompletedRouteEndedWhenJobAgentDoesNotHoldPane(t *testing.T) {
	cases := []struct {
		name  string
		probe heldProbe
	}{
		{name: "the pane is unlisted", probe: heldProbe{state: heldPaneAbsent, reason: "pane_and_label_not_listed"}},
		{name: "the pane id is reused by another agent", probe: heldProbe{state: heldPanePresent, row: nodeAgentRow{PaneID: "live-pane", Name: "other-agent"}}},
		{name: "the job's label is listed on another pane", probe: heldProbe{state: heldPanePresent, row: nodeAgentRow{PaneID: "successor-pane", Name: "lane-a"}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
			h, server, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
			h.routePaneProbe = func(context.Context, string, string, string) heldProbe { return test.probe }
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
	h.nodes["node-a"] = &hubNodeRecord{machineID: "node-a", state: "connected"}
	h.mu.Unlock()
	h.routePaneProbe = t6900Occupant
	result := t6900Relay(t, h, agent, "lane-a", "ev-no-agent")
	if result.Routed {
		t.Fatalf("a route with no live agent routed: %+v", result)
	}
	state := deliveryState(t, server, pw2Caller{"node-a", "node-a", ackStateNodeAToken}, "ev-no-agent")
	if reason, _ := state["reason"].(string); state["stage"] != laneInboxStageDropped || reason != relayDroppedJobEnded {
		t.Fatalf("stage=%v reason=%q, want dropped job_ended", state["stage"], reason)
	}
}

// The verdict is taken per event: each lane event to a completed route asks the
// node, and no earlier answer is reused.
func TestCompletedRouteAsksTheNodePerEvent(t *testing.T) {
	jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
	h, _, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
	var probes atomic.Int32
	h.routePaneProbe = func(ctx context.Context, machine, pane, label string) heldProbe {
		probes.Add(1)
		return t6900Occupant(ctx, machine, pane, label)
	}
	for i := range 3 {
		t6900Relay(t, h, agent, "lane-a", fmt.Sprintf("ev-each-%d", i))
	}
	if got := probes.Load(); got != 3 {
		t.Fatalf("probes=%d, want one per event", got)
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

// A stale route row names a pane the job's agent does not hold. That ends the
// event on that route, but must not mark the job's own pane gone: once the row is
// repointed to the pane the agent holds, the next event is delivered.
func TestCompletedRouteRepointedToLivePaneDeliversAfterStaleRow(t *testing.T) {
	jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
	h, _, agent := relayRegisteredRouteHub(t, `{"lanes":{"lane-a":{"machine":"node-a","pane":"old-pane","job":"job-x"}}}`, jobs)
	h.routePaneProbe = func(ctx context.Context, machine, pane, label string) heldProbe {
		if pane == "live-pane" {
			return t6900Occupant(ctx, machine, pane, label)
		}
		return heldProbe{state: heldPaneAbsent, reason: "pane_and_label_not_listed"}
	}
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
