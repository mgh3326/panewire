package panewire

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A lost close reply leaves the stage for the replay. The replay is the resume
// path (a lost record carrying the step's own end reason), and it re-reads the
// task binding immediately before it activates the stage: a replacement that
// rebound the task since the close releases the stage instead of dispatching
// it (hk 5631 item 1).
func TestEscalateAuthorReplayOfALostAuthorReleasesAStageTheTaskMovedPast_t5631(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-80")
	f.closeLost = true
	if result, _ := f.escalate(t, nil); result.Code != "pending" {
		t.Fatalf("the lost close reply must leave the stage: %+v", result)
	}
	if staged, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err != nil || staged.Status != "staged" {
		t.Fatalf("stage=%+v err=%v", staged, err)
	}
	// The node closed the pane and wrote the lost record; its reply was lost.
	f.endHolder(fleet.LadderStepEndReason(5, f.job+"-l1"))
	// A replacement started with --parent-job rebinds the task before the retry.
	f.taskBound = "pw-task-80-q1"
	result, commands := f.escalate(t, nil)
	if result.Code != "job_mismatch" {
		t.Fatalf("result=%+v", result)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("a superseded stage is released, never activated")
	}
	if slices.Contains(commandOps(commands), "pane.close") {
		t.Fatalf("the author pane is gone already: %v", commandOps(commands))
	}
	f.awaitInfo(t, "job_mismatch")
}

// A refusal for a live author with a staged successor releases the stage at
// once: the staged row and its task-role reservation go, the live holder gets
// its role back, the refusal is filed once, and the expiry pass finds nothing
// to report (hk 5631 item 2).
func TestEscalateAuthorRefusalForALiveAuthorReleasesTheStagedSuccessor_t5631(t *testing.T) {
	cases := []struct {
		name     string
		set      func(f *escalateFixture)
		wantCode string
	}{
		{name: "task moved to another job", set: func(f *escalateFixture) { f.taskBound = "pw-task-81-q1" }, wantCode: "job_mismatch"},
		{name: "task closed", set: func(f *escalateFixture) { f.taskState = "merged" }, wantCode: "task_closed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEscalateFixture(t, "pw-task-81")
			f.closeLost = true
			if result, _ := f.escalate(t, nil); result.Code != "pending" {
				t.Fatalf("the lost close reply must leave the stage: %+v", result)
			}
			if staged, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err != nil || staged.Status != "staged" {
				t.Fatalf("stage=%+v err=%v", staged, err)
			}
			tc.set(f)
			result, _ := f.escalate(t, nil)
			if result.Code != tc.wantCode {
				t.Fatalf("result=%+v", result)
			}
			if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
				t.Fatal("the refused stage must be released with its refusal")
			}
			if _, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || job != f.job {
				t.Fatalf("the live holder's role must be restored: job=%q err=%v", job, err)
			}
			texts := f.infoTexts()
			if len(texts) != 1 || !strings.Contains(texts[0], tc.wantCode) || !strings.Contains(texts[0], "round 5") {
				t.Fatalf("the refusal must be filed once: %v", texts)
			}
			// The expiry pass has nothing left to report for the stage: no
			// second, different event (staged_expired). The read above drained
			// the relay, so anything the pass files is new.
			f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
			f.hub.expireStagedLadderSteps(t.Context())
			if texts := f.infoTexts(); len(texts) != 0 {
				t.Fatalf("the expiry pass filed for a released stage: %v", texts)
			}
		})
	}
}
