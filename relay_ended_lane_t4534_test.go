package panewire

// hk 4534: a lane.event addressed to the lane of a job that has ended can never
// reach a pane, so the sender's row reports it dropped with the job_ended
// reason instead of leaving it queued. A lane that may still get a pane keeps
// the queued stage.

import (
	"testing"
)

func TestLaneEventToEndedJobLaneIsUndeliverable(t *testing.T) {
	cases := []struct {
		name   string
		jobs   map[string]*hubJobRecord
		stage  string
		reason string
	}{
		{"every job in the lane completed", map[string]*hubJobRecord{"job-x": {AgentLabel: "lane-x", Completed: true}}, laneInboxStageDropped, relayDroppedJobEnded},
		{"a job in the lane is still live", map[string]*hubJobRecord{"job-x": {AgentLabel: "lane-x"}}, laneInboxStageQueued, ""},
		{"the hub knows no job in the lane", map[string]*hubJobRecord{}, laneInboxStageQueued, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, server, _ := deliveryStageHub(t)
			h.mu.Lock()
			h.jobs = tc.jobs
			h.mu.Unlock()
			result, _ := relayStageEvent(t, h, "node-a", "lane-x", "ev-x")
			if result.Routed {
				t.Fatalf("lane-x has no pane but the event routed: %+v", result)
			}
			nodeA := pw2Caller{"node-a", "node-a", ackStateNodeAToken}
			state := deliveryState(t, server, nodeA, "ev-x")
			// A queued row carries no reason key at all.
			reason, _ := state["reason"].(string)
			if state["stage"] != tc.stage || reason != tc.reason {
				t.Fatalf("stage=%v reason=%q, want stage %s reason %q", state["stage"], reason, tc.stage, tc.reason)
			}
		})
	}
}

// A registered route can outlive its completed job until reap, and its node
// may still be connected. The event must not be injected into that pane: it is
// unrouted, and the sender's row says job_ended.
func TestLaneEventToEndedJobLaneWithRegisteredRouteIsNotInjected(t *testing.T) {
	h, server, _ := deliveryStageHub(t)
	agent := &hubAgent{machineID: "node-a", relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	h.mu.Lock()
	h.nodes = map[string]*hubNodeRecord{"node-a": {machineID: "node-a", agent: agent, state: "connected"}}
	h.jobs = map[string]*hubJobRecord{"job-a": {HubActiveJob: HubActiveJob{JobID: "job-a", AgentLabel: "lane-a"}, Completed: true}}
	h.mu.Unlock()
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"node-a","pane":"completed-pane","job":"job-a"}}}`)
	event := hubJobEventPayload{JobID: laneEventTransportID("lane-a", "ev-r"), Epoch: 1, OwnerLane: "lane-a", EventID: "ev-r", Text: "approval requested"}
	result := h.relayLaneEvent(event, agent)
	if result.ID == 0 || result.PersistFailed || result.Routed {
		t.Fatalf("relay of an ended lane: %+v", result)
	}
	if len(agent.relays) != 0 {
		t.Fatalf("the completed job's pane received %d injections", len(agent.relays))
	}
	nodeA := pw2Caller{"node-a", "node-a", ackStateNodeAToken}
	state := deliveryState(t, server, nodeA, "ev-r")
	reason, _ := state["reason"].(string)
	if state["stage"] != laneInboxStageDropped || reason != relayDroppedJobEnded {
		t.Fatalf("stage=%v reason=%q, want dropped job_ended", state["stage"], reason)
	}
}

// A takeover reuses the predecessor lane name for a different, live job. The
// route's current job owns delivery even while the predecessor is completed.
func TestLaneEventToTakenOverRouteUsesSuccessorJob(t *testing.T) {
	h, _, _ := deliveryStageHub(t)
	agent := &hubAgent{machineID: "node-a", relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	h.mu.Lock()
	h.nodes = map[string]*hubNodeRecord{"node-a": {machineID: "node-a", agent: agent, state: "connected"}}
	h.jobs = map[string]*hubJobRecord{
		"predecessor": {HubActiveJob: HubActiveJob{JobID: "predecessor", AgentLabel: "lane-a"}, Completed: true},
		"successor":   {HubActiveJob: HubActiveJob{JobID: "successor", AgentLabel: "new-label", Pane: "successor-pane"}, Node: "node-a"},
	}
	h.mu.Unlock()
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"node-a","pane":"successor-pane","job":"successor","taken_over_from":"predecessor"}}}`)
	event := hubJobEventPayload{JobID: laneEventTransportID("lane-a", "ev-takeover"), Epoch: 1, OwnerLane: "lane-a", EventID: "ev-takeover", Text: "approval requested"}
	result := h.relayLaneEvent(event, agent)
	if result.ID == 0 || result.PersistFailed || !result.Routed {
		t.Fatalf("relay of a live takeover route: %+v", result)
	}
	if len(agent.relays) != 1 {
		t.Fatalf("successor pane received %d injections, want 1", len(agent.relays))
	}
	if got := (<-agent.relays).Pane; got != "successor-pane" {
		t.Fatalf("injected pane=%q, want successor-pane", got)
	}
}
