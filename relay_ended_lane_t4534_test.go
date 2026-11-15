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
