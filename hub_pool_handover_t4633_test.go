package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4633: the hub hands an implementer on a spent pool over itself. The row
// stores the successor before the close; the close writes the old record lost
// with replaced_by; the stored successor is submitted with one request id; the
// owner gets one info line naming the result.

var handoverPushed = strings.Repeat("b", 40)

var handoverHead = strings.Repeat("5", 40)

func handoverPolicy() quotaPolicy {
	return quotaPolicy{Expected: time.Hour, Urgent: map[int]bool{1: true, 2: true}, MaxWaits: 1, ResumeText: "continue"}
}

func handoverFixture(t *testing.T, job string, priority int, expected time.Duration) *quotaReplaceFixture {
	t.Helper()
	f := newQuotaReplaceFixture(t, "implementer", job)
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.nodes["machine-a"].capabilities[hubCapabilityHandoverKeep] = true
	f.hub.jobs[job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: job, Epoch: 1}, Node: "machine-a"}
	f.hub.mu.Unlock()
	f.hub.handoverInputs = func(context.Context, string) handoverInputs {
		return handoverInputs{policy: handoverPolicy(), policyOK: true, priority: priority, expected: expected}
	}
	return f
}

// window5h is the 5145 case: a 5-hour window whose reset is 1h35m away.
func window5h() poolExhaustion {
	return poolExhaustion{Pool: "claude", Account: "acct", Cause: "window:5h", Reset: time.Now().Add(95 * time.Minute).UTC().Truncate(time.Second)}
}

// handoverReadReply is the node's read before the row: branch and start.
func handoverReadReply(job string) map[string]any {
	return map[string]any{"outcome": nodeHandoverKeepOK, "branch": "wk/task", "top": "/work/" + job + "-checkout", "pushed": handoverPushed, "dirty": false, "kept": false}
}

// handoverSnapReply is the node's snapshot at the close.
func handoverKeepReply(job string, dirty bool) map[string]any {
	reply := handoverReadReply(job)
	reply["dirty"], reply["kept"] = dirty, true
	if dirty {
		reply["head"], reply["uncommitted"] = handoverHead, []string{"a.txt", "runtime.env"}
	}
	return reply
}

func idlePane(job, cwd, status string) poolActionReply {
	return poolActionReply{op: nodeCommandAgentsList, result: map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{{PaneID: roleHeldPane, Name: job, AgentStatus: status, Cwd: cwd}}, "panes_ok": true}}
}

var releasedReply = poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": ""}}

// sweepHandover runs the sweep for f's job, answering the probe and the patch.
func sweepHandover(t *testing.T, f *quotaReplaceFixture, exhaustion poolExhaustion, patch map[string]any) poolJobResult {
	t.Helper()
	f.holder.Outcome.AccountKey = exhaustion.Account
	done := make(chan poolJobResult, 1)
	go func() { done <- f.hub.sweepPoolJob(t.Context(), exhaustion, f.holder) }()
	for patch != nil {
		select {
		case result := <-done:
			return result
		default:
		}
		command := nodeCommandRead(t, f.node)
		switch string(command["op"]) {
		case `"agents.list"`:
			roleReplyPanes(t, f.node, command, []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
		case `"worktree.handover_keep"`:
			var args nodeHandoverKeepArgs
			if err := json.Unmarshal(command["args"], &args); err != nil || args.Keep || args.Job != f.job || args.Branch != f.holder.Request.Repository.Checkout.Branch || args.Cwd != "/work/project.checkout" {
				t.Fatalf("patch args=%s", command["args"])
			}
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": patch})
			patch = nil
		default:
			// Nothing may close or snapshot before the successor is stored.
			t.Fatalf("unexpected node command during the sweep: %s", command["op"])
		}
	}
	select {
	case result := <-done:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("sweep did not finish")
	}
	return poolJobResult{}
}

// closeHandover plays the close-op path to settlement: the idle pane, the
// node's snapshot, the close, and the old record lost with the row's
// end_reason.
func closeHandover(t *testing.T, f *quotaReplaceFixture, action poolAction) poolAction {
	t.Helper()
	return closeHandoverWith(t, f, action, handoverKeepReply(f.job, true))
}

func closeHandoverWith(t *testing.T, f *quotaReplaceFixture, action poolAction, snap map[string]any) poolAction {
	t.Helper()
	if err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		idlePane(f.job, "/work/project.checkout", "idle"),
		poolActionReply{op: nodeCommandHandoverKeep, result: snap},
		idlePane(f.job, "/work/project.checkout", "idle"),
		poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": fleet.CloseOpClosed, "replayed": false}},
	); err != nil {
		t.Fatal(err)
	}
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "lost"
	f.jobs.records[f.job]["end_reason"] = action.Data.EndReason
	f.jobs.mu.Unlock()
	action = readOnlyPoolAction(t, f.hub)
	if err := runPoolActionStep(t, f.hub, f.node, action, poolStatus(fleet.CloseOpClosed, false, true)); err != nil {
		t.Fatal(err)
	}
	action = readOnlyPoolAction(t, f.hub)
	if action.Phase != poolActionSettled {
		t.Fatalf("phase=%s, want settled", action.Phase)
	}
	return action
}

func spawnOperations(t *testing.T, h *HubServer, job string) []string {
	t.Helper()
	rows, err := h.inbox.store.db.QueryContext(t.Context(), h.inbox.store.q(`SELECT request_id FROM spawn_operations WHERE job=?`), job)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func ownerLines(t *testing.T, h *HubServer, prefix string) []string {
	t.Helper()
	items, err := h.inbox.store.list("parent", false)
	if err != nil {
		t.Fatal(err)
	}
	var texts []string
	for _, item := range items {
		if strings.HasPrefix(item.Event.EventID, prefix) {
			texts = append(texts, item.Event.Text)
		}
	}
	return texts
}

func placeSuccessor(t *testing.T, f *quotaReplaceFixture, job string) {
	t.Helper()
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "done", Host: "machine-b", Pane: "w9:p1", Profile: "sonnet"}, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func finishSettled(t *testing.T, f *quotaReplaceFixture) {
	t.Helper()
	if err := f.hub.reconcilePoolAction(t.Context(), readOnlyPoolAction(t, f.hub)); err != nil {
		t.Fatal(err)
	}
}

func assertHandoverSuccessor(t *testing.T, request fleet.Submission, old, next string) {
	t.Helper()
	args := request.Args
	if slices.Contains(args, "-c") || slices.Contains(args, "--task") {
		t.Fatalf("successor must be a spec checkout under --parent-job, not the old worktree: %v", args)
	}
	for flag, want := range map[string]string{"--job": next, "--parent-job": old, "--role": "implementer", "--owner": "parent", "--exclude-pool": "claude"} {
		if got := hubSpawnArg(args, flag); got != want {
			t.Fatalf("%s=%q, want %q (%v)", flag, got, want, args)
		}
	}
	checkout := request.Repository.Checkout
	if checkout == nil || checkout.Mode != fleet.HubCheckoutSuccessor || checkout.Branch != "wk/task" || checkout.Sha != handoverPushed || checkout.Dir != next+"-checkout" || request.Repository.Worktree != checkout.Dir {
		t.Fatalf("checkout=%+v repo=%+v", checkout, request.Repository)
	}
	if !strings.HasPrefix(request.Brief, "Review the change") || !strings.Contains(request.Brief, "Continue branch wk/task from its last pushed commit "+handoverPushed) {
		t.Fatalf("brief=%q", request.Brief)
	}
	if _, err := fleet.ValidateSubmission(request); err != nil {
		t.Fatalf("stored successor is not a valid submission: %v", err)
	}
}

// The 5145 case end to end.
func TestPoolHandoverUrgentImplementerIsHandedOverByTheHub(t *testing.T) {
	const job, next = "pa-device-stutter-5145", "pa-device-stutter-5145-q1"
	f := handoverFixture(t, job, 1, 3*time.Hour)
	exhaustion := window5h()
	result := sweepHandover(t, f, exhaustion, handoverReadReply(job))
	if result.action != poolJobQueued || result.why != "urgent" {
		t.Fatalf("sweep=%+v", result)
	}
	action := readOnlyPoolAction(t, f.hub)
	handover := action.Data.Handover
	if action.Data.Kind != poolActionKindHandover || handover == nil || handover.Job != next || handover.Successor.RequestID == "" || handover.Submitted {
		t.Fatalf("row=%+v", action)
	}
	if action.Data.EndReason != "quota_stop pool=claude replaced_by="+next {
		t.Fatalf("end_reason=%q", action.Data.EndReason)
	}
	assertHandoverSuccessor(t, handover.Successor, job, next)
	if ids := spawnOperations(t, f.hub, next); len(ids) != 0 {
		t.Fatalf("successor submitted before the close: %v", ids)
	}
	if lines := ownerLines(t, f.hub, ""); len(lines) != 0 {
		t.Fatalf("owner told before the hand-over finished: %v", lines)
	}
	action = closeHandover(t, f, action)
	if brief := action.Data.Handover.Successor.Brief; !strings.Contains(brief, "kept at machine-a:/work/"+job+"-checkout") || !strings.Contains(brief, `"runtime.env"`) || !strings.Contains(brief, "HEAD "+handoverHead) {
		t.Fatalf("note does not name the snapshot: %q", brief)
	}
	finishSettled(t, f)
	if ids := spawnOperations(t, f.hub, next); len(ids) != 1 || ids[0] != handover.Successor.RequestID {
		t.Fatalf("successor operations=%v, want the stored request %s", ids, handover.Successor.RequestID)
	}
	if action = readOnlyPoolAction(t, f.hub); action.Phase != poolActionSettled || !action.Data.Handover.Submitted {
		t.Fatalf("row final before the successor settled: %+v", action)
	}
	placeSuccessor(t, f, next)
	finishSettled(t, f)
	if actions, _ := f.hub.inbox.store.activePoolActions(t.Context()); len(actions) != 0 {
		t.Fatalf("row not final after placement: %+v", actions)
	}
	record, _ := f.jobs.Get(t.Context(), job)
	if record["state"] != "lost" || record["end_reason"] != "quota_stop pool=claude replaced_by="+next {
		t.Fatalf("old record=%v", record)
	}
	lines := ownerLines(t, f.hub, "")
	want := "[panewire] handed " + job + " over to " + next + " on machine-b/sonnet (claude window:5h)"
	if len(lines) != 1 || lines[0] != want {
		t.Fatalf("owner lines=%q, want [%q]", lines, want)
	}
}

func TestPoolHandoverWaitsWhenTheResetIsWithinExpected(t *testing.T) {
	const job = "wait-job"
	f := handoverFixture(t, job, 3, 3*time.Hour)
	exhaustion := window5h()
	result := sweepHandover(t, f, exhaustion, nil)
	if result.action != poolJobNotified || result.why != "reset_within_expected" {
		t.Fatalf("sweep=%+v", result)
	}
	if actions, _ := f.hub.inbox.store.activePoolActions(t.Context()); len(actions) != 0 {
		t.Fatalf("a wait made a handover row: %+v", actions)
	}
	lines := ownerLines(t, f.hub, "pool-wait:")
	if len(lines) != 1 || !strings.Contains(lines[0], exhaustion.Reset.Format(time.RFC3339)) {
		t.Fatalf("owner lines=%q, want one naming the reset", lines)
	}
	// The same stop seen again stays a wait and says nothing more.
	if again := sweepHandover(t, f, exhaustion, nil); again.why != "waiting" {
		t.Fatalf("repeat=%+v", again)
	}
	if lines := ownerLines(t, f.hub, ""); len(lines) != 1 {
		t.Fatalf("owner lines after repeat=%q", lines)
	}
	// A second stop (another reset) hands over.
	second := exhaustion
	second.Reset = second.Reset.Add(10 * time.Minute)
	if result := sweepHandover(t, f, second, handoverReadReply(job)); result.action != poolJobQueued || result.why != "resume_limit" {
		t.Fatalf("second stop=%+v", result)
	}
}

func TestPoolHandoverNoteNamesTheKeptWorktree(t *testing.T) {
	never := map[string]any{"outcome": nodeHandoverKeepOK, "branch": "wk/task", "top": "/work/dirty-job-checkout", "base": strings.Repeat("c", 40), "base_ref": "refs/heads/main", "dirty": false, "kept": false}
	neverKept := map[string]any{"outcome": nodeHandoverKeepOK, "branch": "wk/task", "top": "/work/dirty-job-checkout", "base": strings.Repeat("c", 40), "base_ref": "refs/heads/main", "head": handoverHead, "dirty": true, "kept": true}
	for _, tc := range []struct {
		name  string
		read  map[string]any
		keep  map[string]any
		wants []string
	}{
		{"dirty", handoverReadReply("dirty-job"), handoverKeepReply("dirty-job", true), []string{`"a.txt", "runtime.env"`, "nothing of it was pushed", "kept at machine-a:/work/dirty-job-checkout"}},
		{"clean", handoverReadReply("dirty-job"), handoverKeepReply("dirty-job", false), []string{"check it (submodules too)", "kept at machine-a:/work/dirty-job-checkout"}},
		{"never pushed", never, neverKept, []string{"was never pushed", "HEAD " + handoverHead, "kept at machine-a:"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := handoverFixture(t, "dirty-job", 1, 0)
			if result := sweepHandover(t, f, window5h(), tc.read); result.action != poolJobQueued {
				t.Fatalf("sweep=%+v", result)
			}
			action := closeHandoverWith(t, f, readOnlyPoolAction(t, f.hub), tc.keep)
			successor := action.Data.Handover.Successor
			for _, want := range tc.wants {
				if !strings.Contains(successor.Brief, want) {
					t.Fatalf("note=%q, want %q", successor.Brief, want)
				}
			}
			if tc.name == "never pushed" {
				checkout := successor.Repository.Checkout
				if checkout.Mode != "detached" || checkout.Sha != strings.Repeat("c", 40) || checkout.Fetch != "refs/heads/main" {
					t.Fatalf("never-pushed successor must start from the base: %+v", checkout)
				}
			}
			if _, err := fleet.ValidateSubmission(successor); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A hub restart after the stored successor, after the close and after the
// submit places exactly one successor under the stored request id.
func TestPoolHandoverSurvivesHubRestartAtEveryStep(t *testing.T) {
	const job, next = "restart-job", "restart-job-q1"
	f := handoverFixture(t, job, 1, 0)
	if result := sweepHandover(t, f, window5h(), handoverReadReply(job)); result.action != poolJobQueued {
		t.Fatalf("sweep=%+v", result)
	}
	stored := readOnlyPoolAction(t, f.hub).Data.Handover.Successor.RequestID
	restartPoolActionHub(t, f) // after the stored successor
	action := closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	restartPoolActionHub(t, f) // after the close
	finishSettled(t, f)
	// A crash after the claim but before Submitted was saved: the replay
	// claims the same request again, which is the same operation.
	action = readOnlyPoolAction(t, f.hub)
	action.Data.Handover.Submitted = false
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	restartPoolActionHub(t, f) // after the submit
	finishSettled(t, f)
	finishSettled(t, f)
	placeSuccessor(t, f, next)
	restartPoolActionHub(t, f)
	f.hub.reconcilePoolActions(t.Context())
	if ids := spawnOperations(t, f.hub, next); len(ids) != 1 || ids[0] != stored {
		t.Fatalf("successor operations=%v, want exactly the stored %s", ids, stored)
	}
	if actions, _ := f.hub.inbox.store.activePoolActions(t.Context()); len(actions) != 0 {
		t.Fatalf("row not final: %+v", actions)
	}
	if lines := ownerLines(t, f.hub, "pool-handover:"); len(lines) != 1 {
		t.Fatalf("owner lines=%q", lines)
	}
}

func TestPoolHandoverFailedSuccessorEndsTheRowWithTheFallbackNotice(t *testing.T) {
	const job, next = "fail-job", "fail-job-q1"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	finishSettled(t, f)
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 3, Stage: "placement", Reason: "no_pool"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	finishSettled(t, f)
	lines := ownerLines(t, f.hub, "pool-handover:")
	if len(lines) != 1 || !strings.Contains(lines[0], "hand-over of "+job+" failed") || !strings.Contains(lines[0], "hand it over or end it and respawn") || !strings.Contains(lines[0], "no_pool") {
		t.Fatalf("owner lines=%q", lines)
	}
	rows, err := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if err != nil || len(rows) != 1 || rows[0].Phase != poolActionDone || rows[0].Data.Handover.Outcome != handoverFailed {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}

// One path per job: a handover row fences the screen-triggered quota-stop
// replacement, and a quota successor fences the hand-over.
func TestPoolHandoverAndQuotaReplacementFenceEachOther(t *testing.T) {
	t.Run("handover first", func(t *testing.T) {
		const job = "fence-job"
		f := handoverFixture(t, job, 1, 0)
		sweepHandover(t, f, window5h(), handoverReadReply(job))
		action := closeHandover(t, f, readOnlyPoolAction(t, f.hub))
		// The record is lost now, yet the settled row still owns the job.
		f.jobs.mu.Lock()
		f.jobs.records[job]["state"] = "spawned"
		f.jobs.mu.Unlock()
		if next, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(job, "claude")); ok || next != "" {
			t.Fatalf("quota-stop replacement placed %q beside a handover row", next)
		}
		if ids := spawnOperations(t, f.hub, job+"-q1"); len(ids) != 0 {
			t.Fatalf("second successor: %v", ids)
		}
		if f.hub.reconcileQuotaReplacement(t.Context(), nil, hubSpawnTask{Job: job + "-q1"}, job) {
			t.Fatal("quota reconcile acted on a handed-over job")
		}
		_ = action
	})
	t.Run("quota replacement first", func(t *testing.T) {
		const job = "fence-job"
		f := handoverFixture(t, job, 1, 0)
		successor := f.holder.Request
		successor.RequestID = "d676220b-e763-4af1-aefd-0000000000ff"
		successor.Args = slices.Clone(successor.Args)
		successor.Args[slices.Index(successor.Args, "--job")+1] = job + "-q1"
		successor.Args[slices.Index(successor.Args, "-l")+1] = job + "-q1"
		// A successor under --parent-job holds no task role slot.
		i := slices.Index(successor.Args, "--task")
		successor.Args[i], successor.Args[i+1] = "--parent-job", job
		if _, _, err := f.hub.inbox.store.claimSpawn(t.Context(), successor, job+"-q1", "parent", time.Now()); err != nil {
			t.Fatal(err)
		}
		done := make(chan poolJobResult, 1)
		go func() { done <- f.hub.sweepPoolJob(t.Context(), window5h(), f.holder) }()
		result := <-done
		if result.action != poolJobSkipped || result.why != "already_replaced" {
			t.Fatalf("sweep=%+v", result)
		}
		if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 0 {
			t.Fatalf("handover row beside a quota successor: %+v", rows)
		}
	})
	t.Run("one row per job", func(t *testing.T) {
		const job = "fence-job"
		f := handoverFixture(t, job, 1, 0)
		sweepHandover(t, f, window5h(), handoverReadReply(job))
		closeHandover(t, f, readOnlyPoolAction(t, f.hub))
		f.jobs.mu.Lock()
		f.jobs.records[job]["state"] = "spawned"
		f.jobs.mu.Unlock()
		f.hub.poolSweep.release(job)
		result := f.hub.sweepPoolJob(t.Context(), window5h(), f.holder)
		if result.action != poolJobSkipped || result.why != "action_exists" {
			t.Fatalf("the settled row did not own the job: %+v", result)
		}
		if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 1 {
			t.Fatalf("rows=%+v", rows)
		}
	})
}

func TestPoolHandoverStopsAtTheReplacementLimit(t *testing.T) {
	const job = "limit-job-q1-q2"
	f := handoverFixture(t, job, 1, 0)
	result := f.hub.sweepPoolJob(t.Context(), window5h(), f.holder)
	if result.action != poolJobNotified || result.why != "replacement_limit" {
		t.Fatalf("sweep=%+v", result)
	}
	if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 0 {
		t.Fatalf("rows=%+v", rows)
	}
	lines := ownerLines(t, f.hub, "pool-exhausted:")
	if len(lines) != 1 || !strings.Contains(lines[0], "hand it over or end it and respawn") {
		t.Fatalf("owner lines=%q", lines)
	}
}

// Review round 1 (PR #687): what must be refused before anything closes.
func TestPoolHandoverFallsBackBeforeTheClose(t *testing.T) {
	for _, tc := range []struct {
		name   string
		job    string
		mutate func(*fleet.Submission)
		patch  bool
		why    string
	}{
		{"pinned host", "host-job", func(s *fleet.Submission) {
			s.Args[slices.Index(s.Args, "--host")+1] = "machine-a"
			s.Args = append(s.Args, "--override-reason", "pinned by the owner", "--containment", "hk:42")
		}, false, "pinned"},
		{"end_reason over hk's limit", strings.Repeat("e", 100), nil, false, "end_reason_too_long"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newQuotaReplaceFixtureWith(t, "implementer", tc.job, tc.mutate)
			f.hub.mu.Lock()
			f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
			f.hub.nodes["machine-a"].capabilities[hubCapabilityHandoverKeep] = true
			f.hub.jobs[tc.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: tc.job, Epoch: 1}, Node: "machine-a"}
			f.hub.mu.Unlock()
			f.hub.handoverInputs = func(context.Context, string) handoverInputs {
				return handoverInputs{policy: handoverPolicy(), policyOK: true, priority: 1}
			}
			var patch map[string]any
			if tc.patch {
				patch = handoverReadReply(tc.job)
			}
			if tc.mutate == nil || !tc.patch {
				// Refused before the probe: no node command at all.
				result := f.hub.sweepPoolJob(t.Context(), window5h(), f.holder)
				if result.action != poolJobNotified || result.why != tc.why {
					t.Fatalf("sweep=%+v, want the owner notice for %s", result, tc.why)
				}
			} else if result := sweepHandover(t, f, window5h(), patch); result.action != poolJobNotified || result.why != tc.why {
				t.Fatalf("sweep=%+v, want the owner notice for %s", result, tc.why)
			}
			if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), tc.job); len(rows) != 0 {
				t.Fatalf("rows=%+v", rows)
			}
		})
	}
}

func TestPoolHandoverSuccessorArguments(t *testing.T) {
	old := quotaHolderRequest("implementer", "chain-q1")
	// A first successor handed over again: it already names a parent and
	// carries no report; the original request also had a start condition.
	i := slices.Index(old.Args, "--task")
	old.Args[i], old.Args[i+1] = "--parent-job", "chain"
	j := slices.Index(old.Args, "--report-relative")
	old.Args = slices.Delete(old.Args, j, j+2)
	old.StartConditions = []fleet.StartCondition{{}}
	next, err := handoverSuccessorRequest(old, "chain-q1", "chain-q1-q2", "claude", window5h(), nodeHandoverKeepResult{Branch: "wk/task", Pushed: handoverPushed})
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(strings.Join(next.Args, " "), "--parent-job"); n != 1 || hubSpawnArg(next.Args, "--parent-job") != "chain-q1" {
		t.Fatalf("parent-job must name only the stopped job: %v", next.Args)
	}
	if got := hubSpawnArg(next.Args, "--report-relative"); got != "chain-q1-q2/report.md" {
		t.Fatalf("report=%q", got)
	}
	if next.StartConditions != nil {
		t.Fatalf("start conditions carried: %+v", next.StartConditions)
	}
	if _, err := fleet.ValidateSubmission(next); err != nil {
		t.Fatal(err)
	}
}

func TestPoolHandoverExcludesTheSpentBand(t *testing.T) {
	for _, tc := range []struct {
		exhaustion poolExhaustion
		want       string
	}{
		{poolExhaustion{Pool: "devin", Cause: poolCauseFreeLimit}, fleet.BandKey("devin-free")},
		{poolExhaustion{Pool: "devin", Cause: "window:7d"}, fleet.BandKey("devin")},
		{poolExhaustion{Pool: "claude", Cause: poolCauseRetired}, "claude"},
		{poolExhaustion{Pool: "commandcode", Account: "a", Cause: poolCauseCreditExhausted}, ""},
	} {
		if got := handoverExclude(tc.exhaustion); got != tc.want {
			t.Fatalf("%+v: exclude=%q, want %q", tc.exhaustion, got, tc.want)
		}
	}
	if fleet.BandKey("devin") == "devin" {
		t.Fatal("the paid band must not exclude the whole catalog pool")
	}
}

func TestPoolHandoverReadsTheEffectiveTaskAndStart(t *testing.T) {
	const job = "inherit-job"
	f := handoverFixture(t, job, 3, 2*time.Hour)
	var asked string
	f.hub.handoverInputs = func(_ context.Context, task string) handoverInputs {
		asked = task
		return handoverInputs{policy: handoverPolicy(), policyOK: true, priority: 3, expected: 2 * time.Hour}
	}
	f.jobs.mu.Lock()
	f.jobs.records[job]["task_id"] = float64(77)
	f.jobs.mu.Unlock()
	// Accepted three hours ago, started just now: the reset is within the
	// expected time from the real start.
	f.holder.Created = time.Now().Add(-3 * time.Hour)
	f.hub.mu.Lock()
	f.hub.jobs[job].StartedAt = time.Now().UTC().Format(time.RFC3339)
	f.hub.mu.Unlock()
	if result := sweepHandover(t, f, window5h(), nil); result.why != poolWaitReason {
		t.Fatalf("sweep=%+v, want a wait from the job's start", result)
	}
	if asked != "77" {
		t.Fatalf("task read=%q, want the record's 77", asked)
	}
}

func TestPoolHandoverSkipsWhatItMustNotTouch(t *testing.T) {
	t.Run("rollback drain", func(t *testing.T) {
		f := handoverFixture(t, "drain-job", 1, 0)
		f.hub.spawnRollbackDrain = true
		if result := f.hub.sweepPoolJob(t.Context(), window5h(), f.holder); result.why != "draining" {
			t.Fatalf("sweep=%+v", result)
		}
	})
	t.Run("task bound to another job", func(t *testing.T) {
		f := handoverFixture(t, "rebound-job", 1, 0)
		f.hub.handoverInputs = func(context.Context, string) handoverInputs {
			return handoverInputs{policy: handoverPolicy(), policyOK: true, priority: 1, bound: "someone-else"}
		}
		if result := f.hub.sweepPoolJob(t.Context(), window5h(), f.holder); result.why != "task_rebound" {
			t.Fatalf("sweep=%+v", result)
		}
		if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), "rebound-job"); len(rows) != 0 {
			t.Fatalf("rows=%+v", rows)
		}
	})
}

// A wait row stored without its notice (a crash between the two) gets the
// notice on the next pass.
func TestPoolHandoverReplaysAMissedWaitNotice(t *testing.T) {
	const job = "wait-replay"
	f := handoverFixture(t, job, 3, 3*time.Hour)
	exhaustion := window5h()
	row := poolAction{ID: "wait-row", Job: job, Node: "machine-a", Epoch: 1, Phase: poolActionDone, Data: poolActionData{Owner: "parent", Kind: poolActionKindWait, Handover: &poolHandover{Pool: "claude", Cause: exhaustion.Cause, Reset: exhaustion.Reset.Unix()}}}
	if err := f.hub.inbox.store.insertPoolAction(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	sweepHandover(t, f, exhaustion, nil)
	sweepHandover(t, f, exhaustion, nil)
	if lines := ownerLines(t, f.hub, "pool-wait:"); len(lines) != 1 {
		t.Fatalf("owner lines=%q, want the one missed notice", lines)
	}
}

// Just before the first close command the hub captures again: a changed
// worktree, or a stop that no longer holds, releases the row unclosed.
func TestPoolHandoverWithdrawsBeforeTheClose(t *testing.T) {
	for _, tc := range []struct {
		name    string
		replies []poolActionReply
		gone    bool
	}{
		{"branch moved", []poolActionReply{
			poolStatus(nodeCloseOpNone, false, false),
			idlePane("withdraw-job", "/work/project.checkout", "idle"),
			{op: nodeCommandHandoverKeep, result: func() map[string]any {
				p := handoverKeepReply("withdraw-job", true)
				p["pushed"] = strings.Repeat("e", 40)
				return p
			}()},
			releasedReply,
		}, false},
		{"stop cleared", []poolActionReply{
			poolStatus(nodeCloseOpNone, false, false),
			{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": ""}},
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const job = "withdraw-job"
			f := handoverFixture(t, job, 1, 0)
			sweepHandover(t, f, window5h(), handoverReadReply(job))
			action := readOnlyPoolAction(t, f.hub)
			if tc.gone {
				action.Data.Handover.Reset = time.Now().Add(-time.Minute).Unix()
				if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
					t.Fatal(err)
				}
			}
			if err := runPoolActionStep(t, f.hub, f.node, action, tc.replies...); err != nil {
				t.Fatal(err)
			}
			rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
			if len(rows) != 1 || rows[0].Phase != poolActionRefused {
				t.Fatalf("rows=%+v, want the hand-over released", rows)
			}
		})
	}
}

// The snapshot is the pane's final work: a working pane holds the close, and
// nothing is snapshot or closed until it idles.
func TestPoolHandoverSnapshotsOnlyAnIdlePane(t *testing.T) {
	const job = "busy-job"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub),
		poolStatus(nodeCloseOpNone, false, false),
		idlePane(job, "/work/project.checkout", "working"),
	)
	if err == nil || !strings.Contains(err.Error(), "idle") {
		t.Fatalf("err=%v, want the close held until the pane idles", err)
	}
	if action := readOnlyPoolAction(t, f.hub); action.Phase != poolActionPending || action.Data.Handover.Keep != nil || action.Data.Args != nil {
		t.Fatalf("row=%+v", action)
	}
}

// A settled hand-over whose old job moved before the submit ends
// handover_failed with the owner's notice; it is not dropped silently.
func TestPoolHandoverSettledRowSurvivesAnOwnershipMove(t *testing.T) {
	const job = "moved-job"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	f.hub.mu.Lock()
	f.hub.jobs[job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: job, Epoch: 2}, Node: "machine-b"}
	f.hub.mu.Unlock()
	finishSettled(t, f)
	if ids := spawnOperations(t, f.hub, job+"-q1"); len(ids) != 0 {
		t.Fatalf("successor placed beside the moved job: %v", ids)
	}
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if len(rows) != 1 || rows[0].Phase != poolActionDone || rows[0].Data.Handover.Outcome != handoverFailed {
		t.Fatalf("rows=%+v", rows)
	}
	if lines := ownerLines(t, f.hub, "pool-handover:"); len(lines) != 1 || !strings.Contains(lines[0], "moved") {
		t.Fatalf("owner lines=%q", lines)
	}
}

// Review round 2 (PR #687 head cc0b943).

func TestPoolHandoverSettledRowFencesReassignment(t *testing.T) {
	const job = "fence-settled"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	action := closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	f.hub.mu.Lock()
	f.hub.jobs[job].Orphaned = true
	f.hub.mu.Unlock()
	if _, err := f.hub.reassignJob(job, "machine-b"); err == nil || !strings.Contains(err.Error(), "hand-over") {
		t.Fatalf("settled hand-over did not fence reassignment: %v (row %s)", err, action.Phase)
	}
}

func TestPoolHandoverMovedJobKeepsAClaimedSuccessor(t *testing.T) {
	const job, next = "moved-claimed", "moved-claimed-q1"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	finishSettled(t, f)
	// The claim committed; a crash lost the Submitted flag; then the job moved.
	action := readOnlyPoolAction(t, f.hub)
	action.Data.Handover.Submitted = false
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	f.hub.mu.Lock()
	f.hub.jobs[job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: job, Epoch: 2}, Node: "machine-b"}
	f.hub.mu.Unlock()
	finishSettled(t, f)
	if action = readOnlyPoolAction(t, f.hub); action.Data.Handover.Outcome != "" || !action.Data.Handover.Submitted {
		t.Fatalf("a claimed successor was abandoned: %+v", action.Data.Handover)
	}
	placeSuccessor(t, f, next)
	finishSettled(t, f)
	if lines := ownerLines(t, f.hub, "pool-handover:"); len(lines) != 1 || !strings.Contains(lines[0], "handed "+job+" over to "+next) {
		t.Fatalf("owner lines=%q", lines)
	}
}

func TestPoolHandoverRewritesAnInheritedReport(t *testing.T) {
	old := quotaHolderRequest("implementer", "b-job")
	i := slices.Index(old.Args, "--task")
	old.Args[i], old.Args[i+1] = "--parent-job", "a-job"
	old.Args[slices.Index(old.Args, "--report-relative")+1] = "a-job/report.md"
	next, err := handoverSuccessorRequest(old, "b-job", "b-job-q1", "claude", window5h(), nodeHandoverKeepResult{Branch: "wk/task", Pushed: handoverPushed})
	if err != nil {
		t.Fatal(err)
	}
	if got := hubSpawnArg(next.Args, "--report-relative"); got != "b-job-q1/report.md" || strings.Count(strings.Join(next.Args, " "), "--report-relative") != 1 {
		t.Fatalf("report=%q args=%v", got, next.Args)
	}
}

func TestPoolHandoverRechecksBeforeAnyCloseIsRegistered(t *testing.T) {
	const job = "args-saved"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	action := readOnlyPoolAction(t, f.hub)
	// Args were saved, then the hub restarted before sending the close.
	action.Data.Args = &nodePaneCloseOpArgs{RequestID: action.ID, Job: job, Epoch: 1, Attempt: "00002-job.spawned.json", PaneID: roleHeldPane, Name: job, AgentStatus: "idle", Cwd: "/work/project.checkout", EndReason: action.Data.EndReason}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	changed := handoverKeepReply(job, true)
	changed["pushed"] = strings.Repeat("e", 40)
	if err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		idlePane(job, "/work/project.checkout", "idle"),
		poolActionReply{op: nodeCommandHandoverKeep, result: changed},
		releasedReply,
	); err != nil {
		t.Fatal(err)
	}
	if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 1 || rows[0].Phase != poolActionRefused {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestPoolHandoverAppliesMaxWaitToAFreeLimit(t *testing.T) {
	const job = "free-job"
	f := handoverFixture(t, job, 3, 3*time.Hour)
	f.hub.handoverInputs = func(context.Context, string) handoverInputs {
		p := handoverPolicy()
		p.MaxWait = 15 * time.Minute
		return handoverInputs{policy: p, policyOK: true, priority: 3, expected: 3 * time.Hour}
	}
	free := poolExhaustion{Pool: "claude", Account: "acct", Cause: poolCauseFreeLimit, Reset: time.Now().Add(45 * time.Minute).UTC().Truncate(time.Second)}
	if result := sweepHandover(t, f, free, handoverReadReply(job)); result.action != poolJobQueued || result.why != "reset_beyond_max_wait" {
		t.Fatalf("sweep=%+v, want a hand-over past max_wait", result)
	}
}

func TestPoolHandoverContinuesADetachedSuccessor(t *testing.T) {
	const job = "detached-job-q1"
	f := newQuotaReplaceFixtureWith(t, "implementer", job, func(s *fleet.Submission) {
		s.Repository.Checkout.Mode, s.Repository.Checkout.Branch, s.Repository.Checkout.Fetch = "detached", "", "refs/heads/main"
	})
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.nodes["machine-a"].capabilities[hubCapabilityHandoverKeep] = true
	f.hub.jobs[job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: job, Epoch: 1}, Node: "machine-a"}
	f.hub.mu.Unlock()
	f.hub.handoverInputs = func(context.Context, string) handoverInputs {
		return handoverInputs{policy: handoverPolicy(), policyOK: true, priority: 1}
	}
	patch := handoverReadReply(job)
	patch["branch"] = "wk/made-later"
	if result := sweepHandover(t, f, window5h(), patch); result.action != poolJobQueued {
		t.Fatalf("sweep=%+v", result)
	}
	if checkout := readOnlyPoolAction(t, f.hub).Data.Handover.Successor.Repository.Checkout; checkout.Branch != "wk/made-later" {
		t.Fatalf("checkout=%+v", checkout)
	}
}

func TestPoolHandoverWithdrawsWhenARetiredPoolReturns(t *testing.T) {
	const job = "retired-job"
	f := handoverFixture(t, job, 1, 0)
	retired := poolExhaustion{Pool: "claude", Cause: poolCauseRetired}
	sweepHandover(t, f, retired, handoverReadReply(job))
	action := readOnlyPoolAction(t, f.hub)
	f.hub.poolSweep.forget(retired.key()) // the pool came back
	if err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": ""}},
	); err != nil {
		t.Fatal(err)
	}
	if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 1 || rows[0].Phase != poolActionRefused {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestPoolHandoverBindsTheCloseToTheCapturedWorktree(t *testing.T) {
	t.Run("pane moved to another directory", func(t *testing.T) {
		const job = "cwd-job"
		f := handoverFixture(t, job, 1, 0)
		sweepHandover(t, f, window5h(), handoverReadReply(job))
		if err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub),
			poolStatus(nodeCloseOpNone, false, false),
			idlePane(job, "/work/other", "idle"),
			releasedReply,
		); err != nil {
			t.Fatal(err)
		}
		if rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job); len(rows) != 1 || rows[0].Phase != poolActionRefused {
			t.Fatalf("rows=%+v", rows)
		}
	})
	t.Run("pane in another checkout", func(t *testing.T) {
		const job = "top-job"
		f := handoverFixture(t, job, 1, 0)
		patch := handoverReadReply(job)
		patch["top"] = "/work/someone-elses-clone"
		if result := sweepHandover(t, f, window5h(), patch); result.action != poolJobNotified || result.why != "worktree_mismatch" {
			t.Fatalf("sweep=%+v", result)
		}
	})

}

func TestPoolHandoverWaitReplayReleasesTheSweepClaim(t *testing.T) {
	const job = "wait-release"
	f := handoverFixture(t, job, 3, 3*time.Hour)
	exhaustion := window5h()
	row := poolAction{ID: "wait-row", Job: job, Node: "machine-a", Epoch: 1, Phase: poolActionDone, Data: poolActionData{Owner: "parent", Kind: poolActionKindWait, Handover: &poolHandover{Pool: "claude", Cause: exhaustion.Cause, Reset: exhaustion.Reset.Unix()}}}
	if err := f.hub.inbox.store.insertPoolAction(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	// sweepExhaustedPool releases a notified implementer only for this reason.
	if result := sweepHandover(t, f, exhaustion, nil); result.action != poolJobNotified || result.why != poolWaitReason {
		t.Fatalf("replay=%+v, want the wait reason so the claim is released", result)
	}
}

// Decision for PR #687 (evidence/2026-11-18/pw-handover-4633/decision-root-causes.md).

func taskRoleOf(t *testing.T, h *HubServer, request string) string {
	t.Helper()
	var role sql.NullString
	if err := h.inbox.store.db.QueryRowContext(t.Context(), h.inbox.store.q(`SELECT task_role FROM spawn_reservations WHERE request_id=?`), request).Scan(&role); err != nil {
		t.Fatal(err)
	}
	return role.String
}

// A: a non-final handover row owns the job. Reassignment, role release and
// a rival spawn for the task are refused while it is settled and owes its
// successor, and admitted once the row is final.
func TestPoolHandoverRowOwnsTheJobUntilFinal(t *testing.T) {
	const job, next = "own-job", "own-job-q1"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	action := closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	if owner, err := f.hub.inbox.store.poolActionOwns(t.Context(), job); err != nil || owner != action.ID {
		t.Fatalf("owner=%q err=%v, want the settled row", owner, err)
	}
	if role := taskRoleOf(t, f.hub, f.holder.Request.RequestID); role == "" {
		t.Fatal("the settle released the old job's task role before a successor holds it")
	}
	rival := f.holder.Request
	rival.RequestID = "d676220b-e763-4af1-aefd-0000000000aa"
	rival.Args = slices.Clone(rival.Args)
	rival.Args[slices.Index(rival.Args, "--job")+1] = "rival-job"
	rival.Args[slices.Index(rival.Args, "-l")+1] = "rival-job"
	rival.Args[slices.Index(rival.Args, "--report-relative")+1] = "rival-job/report.md"
	if _, _, err := f.hub.inbox.store.claimSpawn(t.Context(), rival, "rival-job", "parent", time.Now()); !errors.Is(err, errSpawnClaimed) {
		t.Fatalf("a rival spawn for the task was admitted while the row owns the job: %v", err)
	}
	if unlock, free := f.hub.lockWithoutPoolAction(t.Context(), job); free {
		unlock()
		t.Fatal("role release ran while the hand-over owns the job")
	}
	// The successor's claim takes the role over in one transaction.
	finishSettled(t, f)
	successor := readOnlyPoolAction(t, f.hub).Data.Handover.Successor.RequestID
	if old, got := taskRoleOf(t, f.hub, f.holder.Request.RequestID), taskRoleOf(t, f.hub, successor); old != "" || got == "" {
		t.Fatalf("task role old=%q successor=%q, want it moved to the successor", old, got)
	}
	// The successor fails for good: the row is final and frees the task.
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 3, Stage: "placement", Reason: "no_pool"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	finishSettled(t, f)
	if owner, err := f.hub.inbox.store.poolActionOwns(t.Context(), job); err != nil || owner != "" {
		t.Fatalf("owner=%q err=%v after the row ended", owner, err)
	}
	if _, _, err := f.hub.inbox.store.claimSpawn(t.Context(), rival, "rival-job", "parent", time.Now()); err != nil {
		t.Fatalf("the task stayed fenced after the row ended: %v", err)
	}
}

// A crash after the successor's claim and before Submitted was saved: the
// replay claims the same request again, which answers the same operation.
func TestPoolHandoverClaimReplaysAfterACrash(t *testing.T) {
	const job, next = "crash-job", "crash-job-q1"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	finishSettled(t, f)
	action := readOnlyPoolAction(t, f.hub)
	action.Data.Handover.Submitted = false
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	finishSettled(t, f)
	if action = readOnlyPoolAction(t, f.hub); !action.Data.Handover.Submitted || action.Data.Handover.Outcome != "" {
		t.Fatalf("handover=%+v, want the replayed claim recorded", action.Data.Handover)
	}
	if ids := spawnOperations(t, f.hub, next); len(ids) != 1 || ids[0] != action.Data.Handover.Successor.RequestID {
		t.Fatalf("operations=%v", ids)
	}
}

// A rollback drain holds the close of a stored hand-over: nothing is
// snapshot or closed while no successor could be claimed.
func TestPoolHandoverDrainHoldsTheClose(t *testing.T) {
	const job = "drain-close"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	f.hub.inbox.store.spawnFrozen = true
	defer func() { f.hub.inbox.store.spawnFrozen = false }()
	if err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub), poolStatus(nodeCloseOpNone, false, false)); err == nil || !strings.Contains(err.Error(), "drain") {
		t.Fatalf("err=%v, want the close held", err)
	}
	if action := readOnlyPoolAction(t, f.hub); action.Phase != poolActionPending || action.Data.Handover.Keep != nil {
		t.Fatalf("row=%+v", action)
	}
}

// hk binds the task to another job after the row was stored: the close is
// withdrawn, nothing is snapshot.
func TestPoolHandoverRebindingWithdrawsTheClose(t *testing.T) {
	const job = "rebind-close"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	f.hub.handoverInputs = func(context.Context, string) handoverInputs { return handoverInputs{bound: "someone-else"} }
	if err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub), poolStatus(nodeCloseOpNone, false, false), releasedReply); err != nil {
		t.Fatal(err)
	}
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if len(rows) != 1 || rows[0].Phase != poolActionRefused || rows[0].Data.Handover.Reason != "task_rebound" {
		t.Fatalf("rows=%+v", rows)
	}
}

// Smaller items: a pinned profile is dropped, not refused; an inherited label
// is cut to herdr's 32 characters and keeps its -q<n>.
func TestPoolHandoverDropsAPinnedProfileAndBoundsTheLabel(t *testing.T) {
	const job = "pin-job"
	f := newQuotaReplaceFixtureWith(t, "implementer", job, func(s *fleet.Submission) {
		s.Args[slices.Index(s.Args, "-m")+1] = "opus"
		s.Args[slices.Index(s.Args, "-l")+1] = strings.Repeat("l", 31)
		s.Args = append(s.Args, "--override-reason", "pinned by the owner", "--containment", "hk:42")
	})
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.nodes["machine-a"].capabilities[hubCapabilityHandoverKeep] = true
	f.hub.jobs[job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: job, Epoch: 1}, Node: "machine-a"}
	f.hub.mu.Unlock()
	f.hub.handoverInputs = func(context.Context, string) handoverInputs {
		return handoverInputs{policy: handoverPolicy(), policyOK: true, priority: 1}
	}
	if result := sweepHandover(t, f, window5h(), handoverReadReply(job)); result.action != poolJobQueued {
		t.Fatalf("sweep=%+v", result)
	}
	args := readOnlyPoolAction(t, f.hub).Data.Handover.Successor.Args
	if slices.Contains(args, "-m") {
		t.Fatalf("pinned profile kept: %v", args)
	}
	if label := hubSpawnArg(args, "-l"); len(label) != 32 || !strings.HasSuffix(label, "-q1") || !fleet.ValidAgentName(label) {
		t.Fatalf("label=%q", label)
	}
}

// Round 4 (PR #687 head 22178c8d).

// A final note that pushes the brief past its cap withdraws the row before
// the close: no pane closes for a successor that could never be claimed.
func TestPoolHandoverValidatesTheFinalSuccessorBeforeTheClose(t *testing.T) {
	const job = "big-brief"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	action := readOnlyPoolAction(t, f.hub)
	pad := 64<<10 - len(action.Data.Handover.Successor.Brief) - 10
	action.Data.Handover.Brief += strings.Repeat("x", pad)
	action.Data.Handover.Successor.Brief += strings.Repeat("x", pad)
	if _, err := fleet.ValidateSubmission(action.Data.Handover.Successor); err != nil {
		t.Fatalf("fixture: the provisional successor must be valid: %v", err)
	}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	if err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		idlePane(job, "/work/project.checkout", "idle"),
		poolActionReply{op: nodeCommandHandoverKeep, result: handoverKeepReply(job, true)},
		releasedReply,
	); err != nil {
		t.Fatal(err)
	}
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if len(rows) != 1 || rows[0].Phase != poolActionRefused || rows[0].Data.Handover.Reason != "invalid_successor" {
		t.Fatalf("rows=%+v", rows)
	}
}

// A sibling row (a close retry) claimed the same successor request with
// another brief: this row tracks that successor and never frees its role.
func TestPoolHandoverSiblingClaimIsTheSameSuccessor(t *testing.T) {
	const job, next = "sibling-job", "sibling-job-q1"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverReadReply(job))
	closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	action := readOnlyPoolAction(t, f.hub)
	sibling := action.Data.Handover.Successor
	sibling.Brief += "\nanother snapshot\n"
	if _, _, err := f.hub.inbox.store.claimHandoverSuccessor(t.Context(), sibling, next, action.Data.Owner, action.Data.SpawnRequest, time.Now()); err != nil {
		t.Fatal(err)
	}
	placeSuccessor(t, f, next)
	finishSettled(t, f)
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if len(rows) != 1 || rows[0].Data.Handover.Outcome != handoverPlaced {
		t.Fatalf("rows=%+v, want the sibling's successor tracked as placed", rows)
	}
	if role := taskRoleOf(t, f.hub, sibling.RequestID); role == "" {
		t.Fatal("the live successor's task role was released")
	}
}
