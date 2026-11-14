package panewire

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// reviewerSweepActiveJob registers the reviewer's job in the node's active-job
// list, which liveSpawnTasks reads; the fake node sends no heartbeat of its own.
func reviewerSweepActiveJob(t *testing.T, hub *HubServer, machine, job, pane, role string) {
	t.Helper()
	hub.mu.Lock()
	defer hub.mu.Unlock()
	record := hub.nodes[machine]
	if record == nil {
		t.Fatalf("node %s is not registered", machine)
	}
	record.activeJobs[job] = HubActiveJob{JobID: job, Epoch: 1, AgentLabel: job, OwnerLane: "parent", Pane: pane, Role: role}
}

// A daemon-started reviewer that finishes without writing its own job record
// leaves an idle pane and a live hk record the node's ended-job sweep never
// sees: its lane row survives until the cap refuses new lanes (hk 4658). The
// hub's pass ends the job and closes the pane exactly as a reclaiming
// submission does (hk 3315). The close carries the job id, so the node rules
// out an open question before it closes.
func TestReviewerPaneSweepEndsFinishedReviewerJob(t *testing.T) {
	hub, server, _, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("done", "/work/project.test-job"), nil))
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole)
	sink := captureHubLog(hub)

	hub.releaseFinishedReviewerSpawns(t.Context())
	if got := node.seen(); !reflect.DeepEqual(got, []string{"agents.list"}) {
		t.Fatalf("first pass commands %v, want the probe only", got)
	}
	hub.releaseFinishedReviewerSpawns(t.Context())

	if want := []string{"agents.list", "agents.list", "pane.capture", "pane.close", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
	}
	var closeArgs, mark map[string]any
	if err := json.Unmarshal(node.args["pane.close"], &closeArgs); err != nil {
		t.Fatal(err)
	}
	if closeArgs["question_job"] != "test-job" || closeArgs["pane_id"] != roleHeldPane {
		t.Fatalf("pane.close args %v, want the job id so the node can rule out a question", closeArgs)
	}
	if err := json.Unmarshal(node.args["job.mark_lost"], &mark); err != nil {
		t.Fatal(err)
	}
	if mark["job"] != "test-job" || mark["end_reason"] != reviewerPaneFinished {
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

// A pane whose job has an unanswered question is not finished work. The node
// refuses the close and the sweep keeps the pane and the hk record.
func TestReviewerPaneSweepKeepsAnOpenQuestion(t *testing.T) {
	hub, server, jobs, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	replies := unworkedReplies(unworkedRow("done", "/work/project.test-job"), nil)
	replies["pane.close"] = map[string]any{"result": map[string]any{"outcome": nodePaneQuestionOpen}}
	node := startUnworkedNode(t, t14Node(t, hub, server, true), replies)
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole)
	sink := captureHubLog(hub)

	hub.releaseFinishedReviewerSpawns(t.Context())
	hub.releaseFinishedReviewerSpawns(t.Context())

	if want := []string{"agents.list", "agents.list", "pane.capture", "pane.close"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v (no mark_lost)", node.seen(), want)
	}
	logged := sink.String()
	if !strings.Contains(logged, "hub kept a reviewer pane") || !strings.Contains(logged, `"reason":"question_open"`) {
		t.Fatalf("log %q lacks the kept-pane line", logged)
	}
	if strings.Contains(logged, "closed a finished reviewer pane") || strings.Contains(logged, "hub closed") {
		t.Fatalf("log %q claims a close the node refused", logged)
	}
	if state := jobs.state("test-job"); state != "spawned" {
		t.Fatalf("hk record %v, want the live spawned record", state)
	}
}

// A node that cannot decide (an unreadable question record) keeps the pane too:
// the close fails closed, never open.
func TestReviewerPaneSweepKeepsWhenTheNodeCannotDecide(t *testing.T) {
	hub, server, _, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	replies := unworkedReplies(unworkedRow("done", "/work/project.test-job"), nil)
	delete(replies, "pane.close") // the fake node answers herdr_failed
	node := startUnworkedNode(t, t14Node(t, hub, server, true), replies)
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole)

	hub.releaseFinishedReviewerSpawns(t.Context())
	hub.releaseFinishedReviewerSpawns(t.Context())

	if want := []string{"agents.list", "agents.list", "pane.capture", "pane.close"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
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
		// wantOps is the commands the pass may send; nil means none at all,
		// because the job is ruled out before its pane is probed.
		wantOps []string
	}{
		{name: "a pane inside the grace", role: reviewerRole, hkState: "spawned", status: "done", since: 9 * time.Minute, wantOps: []string{"agents.list"}},
		{name: "a working pane", role: reviewerRole, hkState: "spawned", status: "working", since: 11 * time.Minute, wantOps: []string{"agents.list"}},
		{name: "an already terminal record", role: reviewerRole, hkState: "completed", status: "done", since: 11 * time.Minute},
		{name: "an implementer", role: "implementer", hkState: "spawned", status: "done", since: 11 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub, server, jobs, held := reviewerReleaseHub(t, tc.role, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
			jobs.mu.Lock()
			jobs.records["test-job"]["state"] = tc.hkState
			jobs.mu.Unlock()
			hub.now = func() time.Time { return held.Deadline.Add(tc.since) }
			node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow(tc.status, "/work/project.test-job"), nil))
			reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, tc.role)

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

// The submission path applies the same node rule. An open question is not a
// hold that time frees, so the 409 carries no retry_after_s.
func TestReviewerReleaseKeepsAnOpenQuestion(t *testing.T) {
	hub, server, _, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	replies := unworkedReplies(unworkedRow("idle", "/work/project.test-job"), nil)
	replies["pane.close"] = map[string]any{"result": map[string]any{"outcome": nodePaneQuestionOpen}}
	node := startUnworkedNode(t, t14Node(t, hub, server, true), replies)

	result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, reviewerReleaseRequest(reviewerRole, true)))
	if result.status != http.StatusConflict {
		t.Fatalf("status=%d body=%s, want the 409 hold", result.status, result.body)
	}
	var body map[string]any
	if err := json.Unmarshal(result.body, &body); err != nil || body["retry_after_s"] != nil || result.header.Get("Retry-After") != "" {
		t.Fatalf("body=%s header=%v: an open question must not advertise a retry time", result.body, result.header)
	}
	if want := []string{"agents.list", "pane.capture", "pane.close"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
	}
	if owner, _, _, _ := hub.inbox.store.spawnRoleOwner(t.Context(), "42:reviewer:p2"); owner != held.Request.RequestID {
		t.Fatalf("owner=%s, want the holder still to own the key", owner)
	}
}

// A pane that reads finished once while its job still runs is not closed: the
// same status must hold across two passes, minutes apart.
func TestReviewerPaneSweepWaitsForASecondSighting(t *testing.T) {
	hub, server, _, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("done", "/work/project.test-job"), nil))
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole)

	hub.releaseFinishedReviewerSpawns(t.Context())
	if got := node.seen(); !reflect.DeepEqual(got, []string{"agents.list"}) {
		t.Fatalf("first pass commands %v, want the probe only", got)
	}
	hub.releaseFinishedReviewerSpawns(t.Context())
	if got := node.seen(); !reflect.DeepEqual(got, []string{"agents.list", "agents.list", "pane.capture", "pane.close", "job.mark_lost"}) {
		t.Fatalf("second pass commands %v", got)
	}
}

// An idle pane whose job has not posted its result is a reviewer between turns,
// not a finished one: the unattended pass keeps it.
func TestReviewerPaneSweepKeepsAnUnfinishedReviewer(t *testing.T) {
	hub, server, jobs, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("idle", "/work/project.test-job"), unworkedLedger(0)))
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole)
	sink := captureHubLog(hub)

	hub.releaseFinishedReviewerSpawns(t.Context())
	hub.releaseFinishedReviewerSpawns(t.Context())

	if want := []string{"agents.list", "agents.list", "job.result"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v (no close)", node.seen(), want)
	}
	if !strings.Contains(sink.String(), `"reason":"awaiting_result"`) {
		t.Fatalf("log %q lacks the awaiting-result line", sink.String())
	}
	if state := jobs.state("test-job"); state != "spawned" {
		t.Fatalf("hk record %v, want the live spawned record", state)
	}
}

// A reviewer that posted its result is finished work even while its pane reads
// idle, so the unattended pass releases it.
func TestReviewerPaneSweepReleasesAPostedReview(t *testing.T) {
	hub, server, _, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	ledger := unworkedLedger(0)
	ledger["report"] = map[string]any{"present": true, "bytes": 12, "sha256": strings.Repeat("a", 64), "last_line": "review posted"}
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("idle", "/work/project.test-job"), ledger))
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole)

	hub.releaseFinishedReviewerSpawns(t.Context())
	hub.releaseFinishedReviewerSpawns(t.Context())

	if want := []string{"agents.list", "agents.list", "job.result", "pane.capture", "pane.close", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
	}
}

// setReply swaps one node command's answer between passes.
func (n *unworkedNode) setReply(op string, reply map[string]any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.replies[op] = reply
}

// A close whose mark write failed leaves a gone pane and a live record. The
// next pass finds the pane gone and retries the write, so the job still ends.
func TestReviewerPaneSweepRetriesTheMarkOfAClosedPane(t *testing.T) {
	hub, server, jobs, held := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	hub.now = func() time.Time { return held.Deadline.Add(11 * time.Minute) }
	replies := unworkedReplies(unworkedRow("done", "/work/project.test-job"), nil)
	replies["job.mark_lost"] = map[string]any{"error": "mark_lost_timeout"}
	node := startUnworkedNode(t, t14Node(t, hub, server, true), replies)
	reviewerSweepActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole)
	sink := captureHubLog(hub)

	hub.releaseFinishedReviewerSpawns(t.Context())
	hub.releaseFinishedReviewerSpawns(t.Context())
	if !strings.Contains(sink.String(), "could not mark the job") {
		t.Fatalf("log %q lacks the failed-mark warning", sink.String())
	}

	node.setReply("agents.list", map[string]any{"result": map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{}, "panes_ok": true}})
	node.setReply("job.mark_lost", map[string]any{"result": map[string]any{"outcome": "lost"}})
	hub.releaseFinishedReviewerSpawns(t.Context())

	got := node.seen()
	if tail := got[len(got)-2:]; !reflect.DeepEqual(tail, []string{"agents.list", "job.mark_lost"}) {
		t.Fatalf("last pass commands %v, want the retried mark", got)
	}
	if state := jobs.state("test-job"); state != "spawned" {
		t.Fatalf("hk record %v, want the live record the node writes", state)
	}
}
