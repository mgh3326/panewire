package panewire

import (
	"testing"
	"time"
)

// Acceptance tests for hk 5113: liveAccountJobs tracks running seats per account
// and excludes terminal/completed jobs (mutant 3 verification).
func TestHubLiveAccountJobs_t5113(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	const acctA = "commandcode:5c7a09bd00000001"
	const acctB = "commandcode:5c7a09bd00000002"

	complete := func(n int, job, account string) hubSpawnTask {
		t.Helper()
		task := liveOp(t, h, n, job, "auto", now)
		outcome := hubSpawnOutcome{RC: 0, Stage: "done", Profile: "cmd-dsflash41", Pool: "commandcode", AccountKey: account}
		if changed, err := h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("complete %s: %v %v", task.Job, changed, err)
		}
		return task
	}

	// 1. A running job on acctA (succeeded spawn, not completed in h.jobs)
	runningTask := complete(1, "job-running-1", acctA)

	// 2. A completed job on acctB (succeeded spawn, but completed in h.jobs)
	complete(2, "job-completed-2", acctB)
	h.mu.Lock()
	if h.jobs["job-completed-2"] == nil {
		h.jobs["job-completed-2"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-completed-2"}}
	}
	h.jobs["job-completed-2"].Completed = true
	h.mu.Unlock()

	// 3. A failed job on acctA (terminal status)
	complete(3, "job-failed-3", acctA)
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='failed' WHERE job='job-failed-3'`); err != nil {
		t.Fatal(err)
	}

	got, err := h.liveAccountJobs(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}

	// acctA should map to "job-running-1"
	if got[acctA] != "job-running-1" {
		t.Fatalf("liveAccountJobs[%s] = %q, want %q", acctA, got[acctA], "job-running-1")
	}

	// acctB should NOT be in got because its job is completed (Mutant 3 verification)
	if job, ok := got[acctB]; ok {
		t.Fatalf("liveAccountJobs[%s] = %q, want omitted (terminal completed job)", acctB, job)
	}

	// exceptRequest leaves out runningTask
	gotExcept, err := h.liveAccountJobs(t.Context(), runningTask.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if job, ok := gotExcept[acctA]; ok {
		t.Fatalf("liveAccountJobs except %s = %q, want omitted", runningTask.Request.RequestID, job)
	}
}
