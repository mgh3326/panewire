package panewire

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
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

// TestHubLiveAccountJobs_DispatchingInFlight_t5113 proves that an operation in
// 'dispatching' state with a placed account key is counted by liveAccountJobs
// while the node is still executing, preventing a second commandcode spawn
// judged on a nearly spent account (7d >= 75%) from admitting a second seat
// (hk 5113 review finding fix).
func TestHubLiveAccountJobs_DispatchingInFlight_t5113(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	const acctA = "commandcode:5c7a09bd00000001"
	const acctB = "commandcode:5c7a09bd00000002"

	// 1. Operation 1 is in-flight: claimed, placed on acctA, and dispatching.
	// Its terminal node result has NOT arrived yet.
	task1 := liveOp(t, h, 1, "job-dispatching-1", "auto", now)
	if err := h.inbox.store.recordPlacedAccount(t.Context(), task1.Request.RequestID, task1.Attempt, "commandcode", acctA); err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='dispatching' WHERE request_id=?`, task1.Request.RequestID); err != nil {
		t.Fatal(err)
	}

	// 2. Operation 2 is in-flight: placed on acctB via placement snapshot fallback.
	task2 := liveOp(t, h, 2, "job-dispatching-2", "auto", now)
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='dispatching' WHERE request_id=?`, task2.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	tr := fleet.PlacementTrace{Chosen: "machine-b", Profile: "cmd-dsflash41", Pool: "commandcode", AccountKey: acctB}
	trBytes, err := json.Marshal(tr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.db.Exec(`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,delivery,recorded) VALUES(?,?,?,?,0)`, task2.Request.RequestID, task2.Attempt, string(trBytes), "{}"); err != nil {
		t.Fatal(err)
	}

	// Verify liveAccountJobs counts both in-flight operations.
	got, err := h.liveAccountJobs(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got[acctA] != "job-dispatching-1" {
		t.Fatalf("liveAccountJobs[%s] = %q, want %q", acctA, got[acctA], "job-dispatching-1")
	}
	if got[acctB] != "job-dispatching-2" {
		t.Fatalf("liveAccountJobs[%s] = %q, want %q", acctB, got[acctB], "job-dispatching-2")
	}

	// Verify exceptRequest excludes task1 when judging task1's retry.
	gotExcept1, err := h.liveAccountJobs(t.Context(), task1.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if job, ok := gotExcept1[acctA]; ok {
		t.Fatalf("liveAccountJobs except task1 returned %q for %s, want omitted", job, acctA)
	}
	if gotExcept1[acctB] != "job-dispatching-2" {
		t.Fatalf("liveAccountJobs except task1[%s] = %q, want %q", acctB, gotExcept1[acctB], "job-dispatching-2")
	}

	// Verify that when judging a second spawn request (requestID="req-2"),
	// liveAccountJobs reports acctA busy with job-dispatching-1.
	secondJobs, err := h.liveAccountJobs(t.Context(), "req-2")
	if err != nil {
		t.Fatal(err)
	}
	if secondJobs[acctA] != "job-dispatching-1" {
		t.Fatalf("liveAccountJobs for second request[%s] = %q, want %q", acctA, secondJobs[acctA], "job-dispatching-1")
	}
}
