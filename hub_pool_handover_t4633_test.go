package panewire

import (
	"context"
	"encoding/json"
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

var handoverDigest = strings.Repeat("d", 64)

func handoverPolicy() quotaPolicy {
	return quotaPolicy{Expected: time.Hour, Urgent: map[int]bool{1: true, 2: true}, MaxWaits: 1, ResumeText: "continue"}
}

func handoverFixture(t *testing.T, job string, priority int, expected time.Duration) *quotaReplaceFixture {
	t.Helper()
	f := newQuotaReplaceFixture(t, "implementer", job)
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.nodes["machine-a"].capabilities[hubCapabilityHandoverPatch] = true
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

func handoverPatchReply(job string, dirty bool) map[string]any {
	return map[string]any{"outcome": nodeHandoverPatchSaved, "branch": "wk/task", "top": "/work/" + job + "-checkout", "pushed": handoverPushed, "dirty": dirty,
		"saved": "/jobs/" + job + "/" + fleet.HandoverPatchFile, "digest": handoverDigest, "doc": fleet.HandoverPatchDocKey(job)}
}

// finishSettledCapture reconciles the settled row, answering the final
// capture the hub takes after the close and before the submit.
func finishSettledCapture(t *testing.T, f *quotaReplaceFixture, reply map[string]any) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f.hub.reconcilePoolAction(t.Context(), readOnlyPoolAction(t, f.hub)) }()
	command := nodeCommandRead(t, f.node)
	if string(command["op"]) != `"`+nodeCommandHandoverPatch+`"` {
		t.Fatalf("command=%s, want the final capture", command["op"])
	}
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": reply})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

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
		case `"worktree.handover_patch"`:
			var args nodeHandoverPatchArgs
			if err := json.Unmarshal(command["args"], &args); err != nil || args.Job != f.job || args.Branch != f.holder.Request.Repository.Checkout.Branch || args.Cwd != "/work/project.checkout" {
				t.Fatalf("patch args=%s", command["args"])
			}
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": patch})
			patch = nil
		default:
			// Nothing may close before the successor is stored.
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

// closeHandover plays the close-op path to settlement: the node closes the
// pane and writes the old record lost with the row's end_reason.
func closeHandover(t *testing.T, f *quotaReplaceFixture, action poolAction) poolAction {
	t.Helper()
	if err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		poolActionReply{op: nodeCommandHandoverPatch, result: handoverPatchReply(f.job, false)},
		poolActionReply{op: nodeCommandAgentsList, result: map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, "panes_ok": true}},
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
	result := sweepHandover(t, f, exhaustion, handoverPatchReply(job, true))
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
	if !strings.Contains(handover.Successor.Brief, "hk evidence "+fleet.HandoverPatchDocKey(job)) {
		t.Fatalf("note does not name the patch: %q", handover.Successor.Brief)
	}
	if ids := spawnOperations(t, f.hub, next); len(ids) != 0 {
		t.Fatalf("successor submitted before the close: %v", ids)
	}
	if lines := ownerLines(t, f.hub, ""); len(lines) != 0 {
		t.Fatalf("owner told before the hand-over finished: %v", lines)
	}
	action = closeHandover(t, f, action)
	finishSettledCapture(t, f, handoverPatchReply(f.job, false))
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
	if result := sweepHandover(t, f, second, handoverPatchReply(job, false)); result.action != poolJobQueued || result.why != "resume_limit" {
		t.Fatalf("second stop=%+v", result)
	}
}

func TestPoolHandoverDirtyWorktreeNamesTheSavedPatch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		patch map[string]any
		want  string
	}{
		{"uploaded", handoverPatchReply("dirty-job", true), "hk evidence " + fleet.HandoverPatchDocKey("dirty-job")},
		{"never pushed", map[string]any{"outcome": nodeHandoverPatchSaved, "branch": "wk/task", "top": "/work/dirty-job-checkout", "base": strings.Repeat("c", 40), "base_ref": "refs/heads/main", "dirty": true, "saved": "/jobs/dirty-job/" + fleet.HandoverPatchFile, "doc": fleet.HandoverPatchDocKey("dirty-job"), "digest": handoverDigest}, "was never pushed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := handoverFixture(t, "dirty-job", 1, 0)
			if result := sweepHandover(t, f, window5h(), tc.patch); result.action != poolJobQueued {
				t.Fatalf("a dirty worktree blocked the hand-over: %+v", result)
			}
			successor := readOnlyPoolAction(t, f.hub).Data.Handover.Successor
			if !strings.Contains(successor.Brief, tc.want) {
				t.Fatalf("note=%q, want %q", successor.Brief, tc.want)
			}
			if tc.name == "never pushed" {
				checkout := successor.Repository.Checkout
				if checkout.Mode != "detached" || checkout.Sha != strings.Repeat("c", 40) || checkout.Fetch != "refs/heads/main" {
					t.Fatalf("never-pushed successor must start from the base: %+v", checkout)
				}
				if _, err := fleet.ValidateSubmission(successor); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// A hub restart after the stored successor, after the close and after the
// submit places exactly one successor under the stored request id.
func TestPoolHandoverSurvivesHubRestartAtEveryStep(t *testing.T) {
	const job, next = "restart-job", "restart-job-q1"
	f := handoverFixture(t, job, 1, 0)
	if result := sweepHandover(t, f, window5h(), handoverPatchReply(job, false)); result.action != poolJobQueued {
		t.Fatalf("sweep=%+v", result)
	}
	stored := readOnlyPoolAction(t, f.hub).Data.Handover.Successor.RequestID
	restartPoolActionHub(t, f) // after the stored successor
	action := closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	restartPoolActionHub(t, f) // after the close
	finishSettledCapture(t, f, handoverPatchReply(f.job, false))
	// A crash after the claim but before Submitted was saved: the replay
	// claims the same request again, which is the same operation.
	action = readOnlyPoolAction(t, f.hub)
	action.Data.Handover.Submitted = false
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	restartPoolActionHub(t, f) // after the submit
	finishSettledCapture(t, f, handoverPatchReply(f.job, false))
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
	sweepHandover(t, f, window5h(), handoverPatchReply(job, false))
	closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	finishSettledCapture(t, f, handoverPatchReply(f.job, false))
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
		sweepHandover(t, f, window5h(), handoverPatchReply(job, false))
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
		sweepHandover(t, f, window5h(), handoverPatchReply(job, false))
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
		{"pinned profile", "pin-job", func(s *fleet.Submission) {
			s.Args[slices.Index(s.Args, "-m")+1] = "opus"
			s.Args = append(s.Args, "--override-reason", "pinned by the owner", "--containment", "hk:42")
		}, false, "pinned"},
		{"pinned host", "host-job", func(s *fleet.Submission) {
			s.Args[slices.Index(s.Args, "--host")+1] = "machine-a"
			s.Args = append(s.Args, "--override-reason", "pinned by the owner", "--containment", "hk:42")
		}, false, "pinned"},
		{"end_reason over hk's limit", strings.Repeat("e", 100), nil, false, "end_reason_too_long"},
		{"label over herdr's limit", "label-job", func(s *fleet.Submission) { s.Args[slices.Index(s.Args, "-l")+1] = strings.Repeat("l", 31) }, true, "label_too_long"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newQuotaReplaceFixtureWith(t, "implementer", tc.job, tc.mutate)
			f.hub.mu.Lock()
			f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
			f.hub.nodes["machine-a"].capabilities[hubCapabilityHandoverPatch] = true
			f.hub.jobs[tc.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: tc.job, Epoch: 1}, Node: "machine-a"}
			f.hub.mu.Unlock()
			f.hub.handoverInputs = func(context.Context, string) handoverInputs {
				return handoverInputs{policy: handoverPolicy(), policyOK: true, priority: 1}
			}
			var patch map[string]any
			if tc.patch {
				patch = handoverPatchReply(tc.job, false)
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
	next, err := handoverSuccessorRequest(old, "chain-q1", "chain-q1-q2", "claude", window5h(), nodeHandoverPatchResult{Pushed: handoverPushed, Digest: handoverDigest})
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
		{"worktree changed", []poolActionReply{
			poolStatus(nodeCloseOpNone, false, false),
			{op: nodeCommandHandoverPatch, result: func() map[string]any {
				p := handoverPatchReply("withdraw-job", false)
				p["digest"] = strings.Repeat("e", 64)
				return p
			}()},
			{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": ""}},
		}, false},
		{"stop cleared", []poolActionReply{
			poolStatus(nodeCloseOpNone, false, false),
			{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": ""}},
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const job = "withdraw-job"
			f := handoverFixture(t, job, 1, 0)
			sweepHandover(t, f, window5h(), handoverPatchReply(job, false))
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

func TestPoolHandoverCapturesOnlyAnIdlePane(t *testing.T) {
	const job = "busy-job"
	f := handoverFixture(t, job, 1, 0)
	done := make(chan poolJobResult, 1)
	go func() { done <- f.hub.sweepPoolJob(t.Context(), window5h(), f.holder) }()
	command := nodeCommandRead(t, f.node)
	roleReplyPanes(t, f.node, command, []nodeAgentRow{{PaneID: roleHeldPane, Name: job, AgentStatus: "working", Cwd: "/work/project.checkout"}}, true)
	if result := <-done; result.why != "pane_not_idle" {
		t.Fatalf("sweep=%+v", result)
	}
}

// A settled hand-over whose old job moved before the submit ends
// handover_failed with the owner's notice; it is not dropped silently.
func TestPoolHandoverSettledRowSurvivesAnOwnershipMove(t *testing.T) {
	const job = "moved-job"
	f := handoverFixture(t, job, 1, 0)
	sweepHandover(t, f, window5h(), handoverPatchReply(job, false))
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
	sweepHandover(t, f, window5h(), handoverPatchReply(job, false))
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
	sweepHandover(t, f, window5h(), handoverPatchReply(job, false))
	closeHandover(t, f, readOnlyPoolAction(t, f.hub))
	finishSettledCapture(t, f, handoverPatchReply(job, false))
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
	next, err := handoverSuccessorRequest(old, "b-job", "b-job-q1", "claude", window5h(), nodeHandoverPatchResult{Branch: "wk/task", Pushed: handoverPushed})
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
	sweepHandover(t, f, window5h(), handoverPatchReply(job, false))
	action := readOnlyPoolAction(t, f.hub)
	// Args were saved, then the hub restarted before sending the close.
	action.Data.Args = &nodePaneCloseOpArgs{RequestID: action.ID, Job: job, Epoch: 1, Attempt: "00002-job.spawned.json", PaneID: roleHeldPane, Name: job, AgentStatus: "idle", Cwd: "/work/project.checkout", EndReason: action.Data.EndReason}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	changed := handoverPatchReply(job, false)
	changed["digest"] = strings.Repeat("e", 64)
	if err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		poolActionReply{op: nodeCommandHandoverPatch, result: changed},
		poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": ""}},
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
	if result := sweepHandover(t, f, free, handoverPatchReply(job, false)); result.action != poolJobQueued || result.why != "reset_beyond_max_wait" {
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
	f.hub.nodes["machine-a"].capabilities[hubCapabilityHandoverPatch] = true
	f.hub.jobs[job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: job, Epoch: 1}, Node: "machine-a"}
	f.hub.mu.Unlock()
	f.hub.handoverInputs = func(context.Context, string) handoverInputs {
		return handoverInputs{policy: handoverPolicy(), policyOK: true, priority: 1}
	}
	patch := handoverPatchReply(job, false)
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
	sweepHandover(t, f, retired, handoverPatchReply(job, false))
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
		sweepHandover(t, f, window5h(), handoverPatchReply(job, false))
		if err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub),
			poolStatus(nodeCloseOpNone, false, false),
			poolActionReply{op: nodeCommandHandoverPatch, result: handoverPatchReply(job, false)},
			poolActionReply{op: nodeCommandAgentsList, result: map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{{PaneID: roleHeldPane, Name: job, AgentStatus: "idle", Cwd: "/work/other"}}, "panes_ok": true}},
			poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": ""}},
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
		patch := handoverPatchReply(job, false)
		patch["top"] = "/work/someone-elses-clone"
		if result := sweepHandover(t, f, window5h(), patch); result.action != poolJobNotified || result.why != "worktree_mismatch" {
			t.Fatalf("sweep=%+v", result)
		}
	})
	t.Run("final capture after the close moved", func(t *testing.T) {
		const job, next = "final-job", "final-job-q1"
		f := handoverFixture(t, job, 1, 0)
		sweepHandover(t, f, window5h(), handoverPatchReply(job, false))
		closeHandover(t, f, readOnlyPoolAction(t, f.hub))
		moved := handoverPatchReply(job, true)
		moved["pushed"] = strings.Repeat("f", 40)
		finishSettledCapture(t, f, moved)
		if ids := spawnOperations(t, f.hub, next); len(ids) != 0 {
			t.Fatalf("successor claimed on a stale capture: %v", ids)
		}
		if lines := ownerLines(t, f.hub, "pool-handover:"); len(lines) != 1 || !strings.Contains(lines[0], "failed") {
			t.Fatalf("owner lines=%q", lines)
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
