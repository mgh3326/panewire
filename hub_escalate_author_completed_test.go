package panewire

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// An author's first READY completes its hk record and frees its task role
// (releaseEndedJobRole), and the author goes on fixing the PR's findings in the
// same pane for every later review round (hk 5521: pw-attempt-grant-5310
// completed at 23:55:57 and sent READY for rounds 2-6 until 00:38). The round-5
// step must still replace it: its pane is the author, and a completed record
// is not proof it is gone.
//
// completeAuthorRecord is what hk and the hub hold after that first READY.
func (f *escalateFixture) completeAuthorRecord(t *testing.T) {
	t.Helper()
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "completed"
	f.jobs.mu.Unlock()
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.hub.inbox.store.releaseCompletedSpawnRole(t.Context(), holder.Request.RequestID, "42:implementer"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err == nil {
		t.Fatal("the completed author's role must be free, as the job-end release leaves it")
	}
}

func TestEscalateAuthorStepsAnAuthorWhoseRecordCompleted(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-60")
	f.completeAuthorRecord(t)
	placed := placedAnywhere
	result, commands := f.escalate(t, &placed)
	if result.Code != "" || result.Job != "pw-task-60-l1" || result.Rung != "sonnet@xhigh" || result.AuthorEnded {
		t.Fatalf("a completed record with a live pane must step: %+v", result)
	}
	// hk holds the completed record terminal: nothing writes lost over it.
	if ops := commandOps(commands); !slices.Equal(ops, []string{"agents.list", "worktree.inspect", "worktree.inspect", "pane.capture", "pane.close"}) {
		t.Fatalf("node commands %v", ops)
	}
	if state := f.jobs.records[f.job]["state"]; state != "completed" {
		t.Fatalf("the completed record is not rewritten: %v", state)
	}
	_, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	if err != nil || job != "pw-task-60-l1" {
		t.Fatalf("the successor must hold the task role: job=%q err=%v", job, err)
	}
	f.awaitInfo(t, "ladder step pw-task-60 -> pw-task-60-l1 on sonnet@xhigh")
}

// A completed record whose pane is gone is an author that ended on its own
// (the PR's merge reap, a closed pane): there is nothing to replace, and the
// refusal must not tell the lane the step ended an author it never touched.
func TestEscalateAuthorCompletedAuthorWithoutPaneIsNotAnEndedAuthor(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-61")
	f.completeAuthorRecord(t)
	f.paneAbsent = true
	result, commands := f.escalate(t, nil)
	if result.Code != "job_not_live" || result.AuthorEnded {
		t.Fatalf("result=%+v", result)
	}
	if ops := commandOps(commands); !slices.Equal(ops, []string{"agents.list"}) {
		t.Fatalf("only the pane probe may run: %v", ops)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("no successor may be staged for an author that is gone")
	}
}

// A refusal before the close leaves a completed author as it was: the staged
// successor is discarded and its role is released, and the completed record's
// role is not handed back to a terminal job.
func TestEscalateAuthorRefusalBeforeCloseKeepsACompletedAuthorsPane(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-62")
	f.completeAuthorRecord(t)
	f.prepareOutcome = nodeOutcomeDirty
	result, commands := f.escalate(t, nil)
	if result.Code != "worktree_dirty" || result.AuthorEnded {
		t.Fatalf("result=%+v", result)
	}
	if slices.Contains(commandOps(commands), "pane.close") {
		t.Fatalf("the pane must stay open: %v", commandOps(commands))
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("refused staged successor remains")
	}
	if _, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err == nil {
		t.Fatalf("no job may hold the role after the refusal, got %q", job)
	}
}

// A replay of a step that staged the successor of a completed author and died
// before the close: the pane is still there and the record is still completed,
// which is no independent end. The reconciler cancels the expired stage and
// leaves the author, and the role the stage held is free again.
func TestExpiredStagedLadderStageOfACompletedAuthorKeepsThePane(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-63")
	f.completeAuthorRecord(t)
	next, err := escalateAuthorSubmission(escalateHolderRequest(f.job), f.request(), f.job+"-l1")
	if err != nil {
		t.Fatal(err)
	}
	setEscalateAuthorHost(&next, "machine-a")
	next.LadderStep = &fleet.LadderStepIdentity{Parent: f.job, Round: 5, PR: f.request().PR, Profile: "sonnet", Effort: "xhigh"}
	if _, _, err := f.hub.inbox.store.claimSpawnReplacing(t.Context(), next, f.job+"-l1", "parent", time.Now().Add(-10*time.Minute), "", "42:implementer", "staged", ""); err != nil {
		t.Fatal(err)
	}
	f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
	f.hub.expireStagedLadderSteps(t.Context())
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("expired stage remains")
	}
	if _, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err == nil {
		t.Fatalf("the cancelled stage left the role held by %q", job)
	}
	f.awaitInfo(t, "staged_expired")
}

// The CLI no longer refuses a job bound to another task before the hub: the
// hub refuses it, and its refusal reaches the owner lane.
func TestEscalateAuthorRefusesARecordBoundToAnotherTask(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-64")
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["task_id"] = "77"
	f.jobs.mu.Unlock()
	result, commands := f.escalate(t, nil)
	if result.Code != "job_mismatch" || result.AuthorEnded || slices.Contains(commandOps(commands), "pane.close") {
		t.Fatalf("result=%+v commands=%v", result, commandOps(commands))
	}
	f.awaitInfo(t, "job_mismatch", "bound to task 77")
}

// stageCompletedSuccessor stages the successor of a completed author by hand,
// as a step that died after staging leaves it; marked adds the close marker
// the step writes before it asks for the close.
func (f *escalateFixture) stageCompletedSuccessor(t *testing.T, marked bool) {
	t.Helper()
	next, err := escalateAuthorSubmission(escalateHolderRequest(f.job), f.request(), f.job+"-l1")
	if err != nil {
		t.Fatal(err)
	}
	setEscalateAuthorHost(&next, "machine-a")
	next.Repository.Checkout.Sha = strings.Repeat("b", 40)
	next.Repository.Checkout.VerifyReuse = true
	next.LadderStep = &fleet.LadderStepIdentity{Parent: f.job, Round: 5, PR: f.request().PR, Profile: "sonnet", Effort: "xhigh"}
	if _, _, err := f.hub.inbox.store.claimSpawnReplacing(t.Context(), next, f.job+"-l1", "parent", time.Now().Add(-10*time.Minute), "", "42:implementer", "staged", ""); err != nil {
		t.Fatal(err)
	}
	if marked {
		if err := f.hub.inbox.store.markStagedClose(t.Context(), next.RequestID, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}

// A task that no longer names the author is not the author's step: a
// replacement started with --parent-job rebinds the task, holds no task role
// and leaves the old record and pane behind (mergewake review of #750).
func TestEscalateAuthorRefusesWhenTheTaskNamesAnotherJob(t *testing.T) {
	for _, completed := range []bool{false, true} {
		f := newEscalateFixture(t, "pw-task-65")
		if completed {
			f.completeAuthorRecord(t)
		}
		f.taskBound = "pw-task-65-q1"
		result, commands := f.escalate(t, nil)
		if result.Code != "job_mismatch" || result.AuthorEnded || slices.Contains(commandOps(commands), "pane.close") {
			t.Fatalf("completed=%v result=%+v commands=%v", completed, result, commandOps(commands))
		}
		if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
			t.Fatalf("completed=%v: no successor may be staged", completed)
		}
	}
}

// The binding is read again right before the close: a rebind between staging
// and the close refuses the step with the pane open and the stage released.
func TestEscalateAuthorRechecksTheTaskBindingBeforeTheClose(t *testing.T) {
	for _, completed := range []bool{false, true} {
		f := newEscalateFixture(t, "pw-task-66")
		if completed {
			f.completeAuthorRecord(t)
		}
		f.rebindTo, f.rebindAfterReads = "pw-task-66-q1", 1
		result, commands := f.escalate(t, nil)
		if result.Code != "job_mismatch" || result.AuthorEnded || slices.Contains(commandOps(commands), "pane.close") {
			t.Fatalf("completed=%v result=%+v commands=%v", completed, result, commandOps(commands))
		}
		if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
			t.Fatalf("completed=%v: the stage must be released", completed)
		}
		_, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
		if completed && err == nil || !completed && (err != nil || job != f.job) {
			t.Fatalf("completed=%v: role job=%q err=%v", completed, job, err)
		}
	}
}

// The node closed a completed author's pane and its reply was lost: the first
// call cannot confirm the close, and the retry finds the pane gone. The close
// marker says this step asked for it, so the retry places the successor instead
// of reporting an independent end (mergewake review of #750).
func TestEscalateAuthorResumesAfterALostCloseReplyForACompletedAuthor(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-67")
	f.completeAuthorRecord(t)
	f.closeLost = true
	result, commands := f.escalate(t, nil)
	if result.Code != "pending" || !slices.Contains(commandOps(commands), "pane.close") {
		t.Fatalf("result=%+v commands=%v", result, commandOps(commands))
	}
	if staged, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err != nil || staged.Status != "staged" {
		t.Fatalf("the stage must be kept: %+v err=%v", staged, err)
	}
	f.closeLost = false
	placed := placedAnywhere
	result, commands = f.escalate(t, &placed)
	if result.Code != "" || result.Job != "pw-task-67-l1" {
		t.Fatalf("the retry must place the successor: %+v", result)
	}
	if slices.Contains(commandOps(commands), "pane.close") {
		t.Fatalf("the pane is gone already: %v", commandOps(commands))
	}
}

// A stage no close was ever asked for, whose completed author's pane is gone,
// lost its author independently.
func TestEscalateAuthorUnmarkedStageOfACompletedAuthorWithoutPaneEndedIndependently(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-68")
	f.completeAuthorRecord(t)
	f.stageCompletedSuccessor(t, false)
	f.paneAbsent = true
	result, _ := f.escalate(t, nil)
	if result.Code != "parent_ended" {
		t.Fatalf("result=%+v", result)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("the stage must be released")
	}
}

// The expiry reconciler follows the same marker: a marked stage whose pane is
// gone is finished, a marked stage with no readable pane list is kept and the
// owner is asked once, an unmarked one is released.
func TestExpiredStagedLadderStageOfACompletedAuthorFollowsTheCloseMarker(t *testing.T) {
	cases := []struct {
		name            string
		marked, absent  bool
		unknown         bool
		wantStatus      string
		wantStageKept   bool
		wantInfoContain string
	}{
		{name: "marked and gone is finished", marked: true, absent: true, wantStatus: "accepted", wantStageKept: true},
		{name: "marked and unreadable is kept with a decision", marked: true, unknown: true, wantStatus: "staged", wantStageKept: true, wantInfoContain: "unresolved"},
		{name: "unmarked and gone is released", absent: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEscalateFixture(t, "pw-task-69")
			f.completeAuthorRecord(t)
			f.stageCompletedSuccessor(t, tc.marked)
			f.paneAbsent, f.panesUnknown = tc.absent, tc.unknown
			f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
			f.hub.expireStagedLadderSteps(t.Context())
			successor, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
			if tc.wantStageKept != (err == nil) || err == nil && successor.Status != tc.wantStatus {
				t.Fatalf("successor=%+v err=%v", successor, err)
			}
			if tc.wantInfoContain != "" {
				f.awaitInfo(t, tc.wantInfoContain)
			}
		})
	}
}
