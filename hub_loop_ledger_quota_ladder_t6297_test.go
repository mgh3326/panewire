package panewire

import (
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The quota_ladder ledger makes a staged ladder step once the expiry pass sees
// it past its deadline, and counts it done when it leaves staged by any path
// (hk 6297). These tests drive the exits the expiry pass does not run, the
// fresh handoff that never went stale, and a stuck step the pass keeps.

// staleLadderClock moves the hub's clock ten minutes on, past the deadline the
// fixtures give a staged step (the expiry tests use the same offset).
func staleLadderClock(h *HubServer) {
	h.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
}

func assertQuotaLadderLedger(t *testing.T, h *HubServer, made, done int64) {
	t.Helper()
	snap := h.loopSnapshot()[loopQuotaLadder]
	if snap.Made != made || snap.Done != done {
		t.Fatalf("quota_ladder made=%d done=%d, want made=%d done=%d", snap.Made, snap.Done, made, done)
	}
}

// stagedSuccessorStatus is the status of the job's latest spawn row, or "" when it has none.
func stagedSuccessorStatus(t *testing.T, h *HubServer, job string) string {
	t.Helper()
	task, err := h.inbox.store.latestSpawnForJob(t.Context(), job)
	if err != nil {
		return ""
	}
	return task.Status
}

// A stale staged step that leaves staged on a path outside the expiry pass
// counts made and done for the day, whichever path takes it: the operator's
// release, a replay that activates it, or the task moving on, which supersedes
// it. The pass never runs in these cases.
func TestQuotaLadderStaleStageLeavingOutsideThePassCountsDone_t6297(t *testing.T) {
	t.Run("operator release", func(t *testing.T) {
		f := newLadderCloseFixture(t, "pw-task-6297-release", true)
		stageID := f.stageLiveSuccessorWithClose(t, nil, true)
		f.take()
		staleLadderClock(f.hub)
		recorder := releaseLadderHTTP(t, f.hub, stageID, t14OperatorToken, "", "stale step released by the operator")
		if recorder.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		assertQuotaLadderLedger(t, f.hub, 1, 1)
	})

	t.Run("replay activates the stage", func(t *testing.T) {
		f := newEscalateFixture(t, "pw-task-6297-activate")
		f.closeLost = true
		if result, _ := f.escalate(t, nil); result.Code != "pending" {
			t.Fatalf("the lost close reply must leave the stage: %+v", result)
		}
		f.endHolder(fleet.LadderStepEndReason(5, f.job+"-l1"))
		staleLadderClock(f.hub)
		placed := placedAnywhere
		f.escalate(t, &placed)
		if status := stagedSuccessorStatus(t, f.hub, f.job+"-l1"); status == "staged" {
			t.Fatal("the replay left the stale stage staged")
		}
		assertQuotaLadderLedger(t, f.hub, 1, 1)
	})

	for _, tc := range []struct {
		name     string
		set      func(f *escalateFixture)
		wantCode string
	}{
		{name: "task moved to another job", set: func(f *escalateFixture) { f.taskBound = "pw-task-6297-supersede-q1" }, wantCode: "job_mismatch"},
		{name: "task closed", set: func(f *escalateFixture) { f.taskState = "merged" }, wantCode: "task_closed"},
	} {
		t.Run("supersedes the stage: "+tc.name, func(t *testing.T) {
			f := newEscalateFixture(t, "pw-task-6297-supersede")
			f.closeLost = true
			if result, _ := f.escalate(t, nil); result.Code != "pending" {
				t.Fatalf("the lost close reply must leave the stage: %+v", result)
			}
			tc.set(f)
			staleLadderClock(f.hub)
			if result, _ := f.escalate(t, nil); result.Code != tc.wantCode {
				t.Fatalf("result=%+v, want %s", result, tc.wantCode)
			}
			if status := stagedSuccessorStatus(t, f.hub, f.job+"-l1"); status != "" {
				t.Fatalf("the superseded stage is still present with status %q", status)
			}
			assertQuotaLadderLedger(t, f.hub, 1, 1)
		})
	}
}

// A step that leaves before its deadline never went stale. Its handoff adds
// neither made nor done, so it cannot dilute the done ratio of the stuck steps.
func TestQuotaLadderFreshHandoffCountsNothing_t6297(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-6297-fresh")
	f.closeLost = true
	if result, _ := f.escalate(t, nil); result.Code != "pending" {
		t.Fatalf("the lost close reply must leave the stage: %+v", result)
	}
	f.endHolder(fleet.LadderStepEndReason(5, f.job+"-l1"))
	placed := placedAnywhere
	f.escalate(t, &placed)
	if status := stagedSuccessorStatus(t, f.hub, f.job+"-l1"); status == "staged" {
		t.Fatal("the replay left the fresh stage staged")
	}
	assertQuotaLadderLedger(t, f.hub, 0, 0)
}

// A stale stage whose holder pane is gone, with the record still claimed, stays
// staged: every expiry pass sees it, the day counts it made once and not done,
// and the progress rule fires once the day has the floor of made items.
func TestQuotaLadderStuckStageStaysMadeNotDone_t6297(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-6297-stuck")
	f.closeLost = true
	if result, _ := f.escalate(t, nil); result.Code != "pending" {
		t.Fatalf("the lost close reply must leave the stage: %+v", result)
	}
	staleLadderClock(f.hub)
	for range 3 {
		f.hub.expireStagedLadderSteps(t.Context())
	}
	if status := stagedSuccessorStatus(t, f.hub, f.job+"-l1"); status != "staged" {
		t.Fatalf("the stuck stage left staged: status=%q", status)
	}
	assertQuotaLadderLedger(t, f.hub, 1, 0)
	if slices.Contains(f.hub.loopsProgressHits(), loopQuotaLadder) {
		t.Fatal("one stuck step fired the progress rule below the floor")
	}
	for i := range 19 {
		f.hub.loopNoteMade(loopQuotaLadder, fmt.Sprintf("stuck-filler-%d", i))
	}
	if !slices.Contains(f.hub.loopsProgressHits(), loopQuotaLadder) {
		t.Fatalf("made=20 done=0 has no progress hit: %+v", f.hub.loopsProgressHits())
	}
}
