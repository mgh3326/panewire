package panewire

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// reviewerSweepActiveJob registers the reviewer's job the way a node heartbeat
// does: in the node's active set (which liveSpawnTasks reads) and in the hub's
// job registry (whose newest event kind decides an open decision wait). The
// fake node sends no heartbeat of its own.
func reviewerSweepActiveJob(t *testing.T, hub *HubServer, machine, job, pane, role, lastEventKind string) {
	t.Helper()
	now := hub.now()
	active := HubActiveJob{JobID: job, Epoch: 1, AgentLabel: job, OwnerLane: "parent", Pane: pane, Role: role, LastEventKind: lastEventKind}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	record := hub.nodes[machine]
	if record == nil {
		t.Fatalf("node %s is not registered", machine)
	}
	record.activeJobs[job] = active
	hub.jobs[job] = &hubJobRecord{HubActiveJob: active, Node: machine, LastSeen: now, FencedNodes: map[string]uint64{}}
}

// A daemon-started reviewer that finishes without writing its own job record
// leaves an idle pane and a live hk record the node's ended-job sweep never
// sees: its lane row survives until the cap refuses new lanes (hk 4658). The
// hub's pass ends the job and closes the pane exactly as a reclaiming
// submission does (hk 3315).
func TestReviewerPaneSweepEndsFinishedReviewerJob(t *testing.T) {
	hub, server, _, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("done", "/work/project.test-job"), nil))
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole, "")
	sink := captureHubLog(hub)

	hub.releaseFinishedReviewerSpawns(t.Context())

	if want := []string{"agents.list", "pane.capture", "pane.close", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
	}
	var mark map[string]any
	if err := json.Unmarshal(node.args["job.mark_lost"], &mark); err != nil {
		t.Fatal(err)
	}
	if mark["job"] != "test-job" || mark["pane_id"] != roleHeldPane || mark["label"] != "test-job" || mark["end_reason"] != reviewerPaneFinished {
		t.Fatalf("mark lost args %v", mark)
	}
	logged := sink.String()
	if strings.Count(logged, "closed a finished reviewer pane") != 1 || !strings.Contains(logged, `"branch":"`+reviewerPaneFinished+`"`) || !strings.Contains(logged, `"source":"sweep"`) {
		t.Fatalf("log %q lacks the sweep's release line", logged)
	}
}

// Each case breaks exactly one check of the pass, and the pane, the job record
// and the lane stay as they were.
func TestReviewerPaneSweepKeepsTheHold(t *testing.T) {
	cases := []struct {
		name    string
		role    string
		hkState string
		status  string
		since   time.Duration
		// lastKind is the newest ledger event the node reports for the job.
		lastKind string
		// wantOps is the commands the pass may send; nil means none at all,
		// because the job is ruled out before its pane is probed.
		wantOps []string
	}{
		{name: "a pane inside the grace", role: reviewerRole, hkState: "spawned", status: "done", since: 9 * time.Minute, wantOps: []string{"agents.list"}},
		{name: "a working pane", role: reviewerRole, hkState: "spawned", status: "working", since: 11 * time.Minute, wantOps: []string{"agents.list"}},
		{name: "an already terminal record", role: reviewerRole, hkState: "completed", status: "done", since: 11 * time.Minute},
		{name: "an implementer", role: "implementer", hkState: "spawned", status: "done", since: 11 * time.Minute},
		{name: "a reviewer waiting on an ESC answer", role: reviewerRole, hkState: "spawned", status: "idle", since: 11 * time.Minute, lastKind: "job.waiting_decision", wantOps: []string{"agents.list"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub, server, jobs, held := reviewerReleaseHub(t, tc.role, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
			jobs.mu.Lock()
			jobs.records["test-job"]["state"] = tc.hkState
			jobs.mu.Unlock()
			hub.now = func() time.Time { return held.Deadline.Add(tc.since) }
			node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow(tc.status, "/work/project.test-job"), nil))
			reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, tc.role, tc.lastKind)

			hub.releaseFinishedReviewerSpawns(t.Context())

			got := node.seen()
			if tc.wantOps == nil {
				if len(got) != 0 {
					t.Fatalf("node commands %v, want none", got)
				}
			} else if !reflect.DeepEqual(got, tc.wantOps) {
				t.Fatalf("node commands %v, want %v", got, tc.wantOps)
			}
			if state := jobs.state("test-job"); state != tc.hkState {
				t.Fatalf("hk record %v, want %s", state, tc.hkState)
			}
		})
	}
}

// A reviewer whose question was answered is finished work again: the newest
// ledger event is the resume, not the wait, so the sweep releases it.
func TestReviewerPaneSweepReleasesAfterDecisionAnswer(t *testing.T) {
	hub, server, _, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("done", "/work/project.test-job"), nil))
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole, "job.decision_resumed")

	hub.releaseFinishedReviewerSpawns(t.Context())

	if want := []string{"agents.list", "pane.capture", "pane.close", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
	}
}
