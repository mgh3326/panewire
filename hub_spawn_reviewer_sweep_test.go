package panewire

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// reviewerSweepActiveJob registers the reviewer's job the way a node heartbeat
// does: in the node's active-job list, which liveSpawnTasks reads and whose
// LastEventKind the ESC guard reads. The fake node sends no heartbeat of its
// own, and the hub's job registry is deliberately left alone: the guard must not
// depend on it (an epoch>=2 job has no entry there after a hub restart).
func reviewerSweepActiveJob(t *testing.T, hub *HubServer, machine, job, pane, role string, epoch uint64, lastEventKind string) {
	t.Helper()
	hub.mu.Lock()
	defer hub.mu.Unlock()
	record := hub.nodes[machine]
	if record == nil {
		t.Fatalf("node %s is not registered", machine)
	}
	record.activeJobs[job] = HubActiveJob{JobID: job, Epoch: epoch, AgentLabel: job, OwnerLane: "parent", Pane: pane, Role: role, LastEventKind: lastEventKind}
}

// A daemon-started reviewer that finishes without writing its own job record
// leaves an idle pane and a live hk record the node's ended-job sweep never
// sees: its lane row survives until the cap refuses new lanes (hk 4658). The
// hub's pass ends the job and closes the pane exactly as a reclaiming
// submission does (hk 3315), but only after two consecutive passes see the
// pane finished: a reviewer that just asked a question is idle too, and the
// watcher records that question a few seconds later.
func TestReviewerPaneSweepEndsFinishedReviewerJob(t *testing.T) {
	hub, server, _, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("done", "/work/project.test-job"), nil))
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole, 1, "")
	sink := captureHubLog(hub)

	hub.releaseFinishedReviewerSpawns(t.Context())
	if got := node.seen(); !reflect.DeepEqual(got, []string{"agents.list"}) {
		t.Fatalf("first pass commands %v, want the probe only", got)
	}
	hub.releaseFinishedReviewerSpawns(t.Context())

	if want := []string{"agents.list", "agents.list", "pane.capture", "pane.close", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
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
	if strings.Contains(logged, `"request":"`) {
		t.Fatalf("log %q names a submitting request the sweep does not have", logged)
	}
}

// A reviewer whose question was answered is finished work again: the node
// reports the resume as the newest event, so the second sighting releases it.
func TestReviewerPaneSweepReleasesAfterDecisionAnswer(t *testing.T) {
	hub, server, _, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("done", "/work/project.test-job"), nil))
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole, 1, "job.decision_resumed")

	hub.releaseFinishedReviewerSpawns(t.Context())
	hub.releaseFinishedReviewerSpawns(t.Context())

	if want := []string{"agents.list", "agents.list", "pane.capture", "pane.close", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
	}
}

// Each case breaks exactly one check of the pass, and the pane, the job record
// and the lane stay as they were. passes is how many sweeps run: the hold an
// unanswered question creates is only visible once the watcher has recorded it,
// which the second pass covers.
func TestReviewerPaneSweepKeepsTheHold(t *testing.T) {
	cases := []struct {
		name    string
		role    string
		hkState string
		status  string
		since   time.Duration
		// epoch and lastKind are what the node reports for the job.
		epoch    uint64
		lastKind string
		passes   int
		// wantOps is the commands the pass may send; nil means none at all,
		// because the job is ruled out before its pane is probed.
		wantOps []string
	}{
		{name: "a pane inside the grace", role: reviewerRole, hkState: "spawned", status: "done", since: 9 * time.Minute, epoch: 1, passes: 1, wantOps: []string{"agents.list"}},
		{name: "a working pane", role: reviewerRole, hkState: "spawned", status: "working", since: 11 * time.Minute, epoch: 1, passes: 1, wantOps: []string{"agents.list"}},
		{name: "an already terminal record", role: reviewerRole, hkState: "completed", status: "done", since: 11 * time.Minute, epoch: 1, passes: 1},
		{name: "an implementer", role: "implementer", hkState: "spawned", status: "done", since: 11 * time.Minute, epoch: 1, passes: 1},
		{name: "a reviewer waiting on an ESC answer", role: reviewerRole, hkState: "spawned", status: "idle", since: 11 * time.Minute, epoch: 1, lastKind: "job.waiting_decision", passes: 2, wantOps: []string{"agents.list", "agents.list"}},
		{name: "a waiting reviewer the hub first sees at epoch 2", role: reviewerRole, hkState: "spawned", status: "idle", since: 11 * time.Minute, epoch: 2, lastKind: "job.waiting_decision", passes: 2, wantOps: []string{"agents.list", "agents.list"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub, server, jobs, held := reviewerReleaseHub(t, tc.role, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
			jobs.mu.Lock()
			jobs.records["test-job"]["state"] = tc.hkState
			jobs.mu.Unlock()
			hub.now = func() time.Time { return held.Deadline.Add(tc.since) }
			node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow(tc.status, "/work/project.test-job"), nil))
			reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, tc.role, tc.epoch, tc.lastKind)

			for range max(tc.passes, 1) {
				hub.releaseFinishedReviewerSpawns(t.Context())
			}

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

// The race the two-pass rule closes: the pane is idle and past the grace, but
// the node reports no newest kind yet (the watcher has not written
// job.waiting_decision for a question the reviewer just asked). The first pass
// only records the sighting.
func TestReviewerPaneSweepWaitsOutTheDecisionWindow(t *testing.T) {
	hub, server, _, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("idle", "/work/project.test-job"), nil))
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole, 1, "")

	hub.releaseFinishedReviewerSpawns(t.Context())
	if got := node.seen(); !reflect.DeepEqual(got, []string{"agents.list"}) {
		t.Fatalf("first pass commands %v, want the probe only", got)
	}
	hub.releaseFinishedReviewerSpawns(t.Context())
	if want := []string{"agents.list", "agents.list", "pane.capture", "pane.close", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
	}
}
