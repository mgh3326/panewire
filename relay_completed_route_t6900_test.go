package panewire

// hk 6900: a completed job keeps its lane route while its pane is alive. Only a
// recorded reap or close, a lost pane, or a probe that finds the pane unlisted
// ends the route (relay_route_pane.go). These tests drive the hub's relay path
// with the pane probe replaced by a fixture seam.

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

// unreachableRoutePaneProbe is the seam answer for a node that cannot be asked.
// Rigs whose agents hold no connection use it: a probe there would write to a
// nil socket, and no answer keeps the route, as a real unreachable node does.
func unreachableRoutePaneProbe(context.Context, string, string, string) heldProbe {
	return heldProbe{reason: "node_unreachable"}
}

// t6900PresentProbe is the seam answer for a pane the node still lists.
func t6900PresentProbe(context.Context, string, string, string) heldProbe {
	return heldProbe{state: heldPanePresent}
}

// A completed job whose pane is alive keeps its route: the lane event reaches
// the pane. The design lane row is the desk/design case of hk 6613 item 1.
func TestCompletedJobRouteDeliversWhilePaneLives(t *testing.T) {
	for _, lane := range []string{"lane-a", "panewire-design"} {
		t.Run(lane, func(t *testing.T) {
			jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: lane, Pane: "live-pane"}, Node: "node-a", Completed: true}}
			h, _, agent := relayRegisteredRouteHub(t, t6900Route(lane, "job-x"), jobs)
			h.routePaneProbe = t6900PresentProbe
			result := t6900Relay(t, h, agent, lane, "ev-live")
			if !result.Routed {
				t.Fatalf("a completed job whose pane lives must keep its route: %+v", result)
			}
			if len(agent.relays) != 1 {
				t.Fatalf("live pane received %d injections, want 1", len(agent.relays))
			}
		})
	}
}

// A recorded pane.close of the job's pane ends its route: the event is dropped
// job_ended and the pane gets no injection.
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

// A probe that finds the pane unlisted proves it gone. The probe runs off the
// relay path, so the first event may still see the route; once the answer is
// recorded, the next event is dropped job_ended.
func TestProbeFindingPaneUnlistedEndsCompletedJobRoute(t *testing.T) {
	jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
	h, server, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
	h.routePaneProbe = func(context.Context, string, string, string) heldProbe {
		return heldProbe{state: heldPaneAbsent, reason: "pane_and_label_not_listed"}
	}
	t6900Relay(t, h, agent, "lane-a", "ev-first")
	eventually(t, 5*time.Second, "the probe to record the pane gone", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.jobs["job-x"].PaneGone
	})
	result := t6900Relay(t, h, agent, "lane-a", "ev-second")
	if result.Routed {
		t.Fatalf("a pane the probe found unlisted took a lane event: %+v", result)
	}
	state := deliveryState(t, server, pw2Caller{"node-a", "node-a", ackStateNodeAToken}, "ev-second")
	if reason, _ := state["reason"].(string); state["stage"] != laneInboxStageDropped || reason != relayDroppedJobEnded {
		t.Fatalf("stage=%v reason=%q, want dropped job_ended", state["stage"], reason)
	}
}

// The probe answers at most once per completedPaneProbeTTL while the pane is
// listed, however many events arrive for the route.
func TestCompletedRouteProbeIsBoundedByTTL(t *testing.T) {
	jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true}}
	h, _, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
	var probes atomic.Int32
	h.routePaneProbe = func(context.Context, string, string, string) heldProbe {
		probes.Add(1)
		return heldProbe{state: heldPanePresent}
	}
	for i := range 3 {
		t6900Relay(t, h, agent, "lane-a", fmt.Sprintf("ev-ttl-%d", i))
	}
	eventually(t, 5*time.Second, "the probe answer to be recorded", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return !h.jobs["job-x"].PaneProbedAt.IsZero()
	})
	t6900Relay(t, h, agent, "lane-a", "ev-ttl-after")
	if got := probes.Load(); got != 1 {
		t.Fatalf("probes=%d, want 1 within the TTL", got)
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

// A registered route whose node has no live agent resolves to no route. Its
// completed job's pane may still live, so the event waits as a queued row; once
// the pane is recorded gone it is dropped job_ended.
func TestCompletedRouteWithoutLiveAgentQueuesUntilPaneGone(t *testing.T) {
	cases := []struct {
		name       string
		paneGone   bool
		wantStage  string
		wantReason string
	}{
		{name: "the pane may still live", paneGone: false, wantStage: laneInboxStageQueued, wantReason: ""},
		{name: "the pane is recorded gone", paneGone: true, wantStage: laneInboxStageDropped, wantReason: relayDroppedJobEnded},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			jobs := map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane"}, Node: "node-a", Completed: true, PaneGone: test.paneGone}}
			h, server, agent := relayRegisteredRouteHub(t, t6900Route("lane-a", "job-x"), jobs)
			h.mu.Lock()
			h.nodes["node-a"] = &hubNodeRecord{machineID: "node-a", state: "connected"}
			h.mu.Unlock()
			result := t6900Relay(t, h, agent, "lane-a", "ev-no-agent")
			if result.Routed {
				t.Fatalf("a route with no live agent routed: %+v", result)
			}
			state := deliveryState(t, server, pw2Caller{"node-a", "node-a", ackStateNodeAToken}, "ev-no-agent")
			reason, _ := state["reason"].(string)
			if state["stage"] != test.wantStage || reason != test.wantReason {
				t.Fatalf("stage=%v reason=%q, want stage %s reason %q", state["stage"], reason, test.wantStage, test.wantReason)
			}
		})
	}
}
