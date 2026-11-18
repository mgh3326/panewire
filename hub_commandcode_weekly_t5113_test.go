package panewire

import (
	"encoding/json"
	"fmt"
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
// (hk 5113 review finding fix, hk 5124 columns).
func TestHubLiveAccountJobs_DispatchingInFlight_t5113(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	const acctA = "commandcode:5c7a09bd00000001"
	const acctB = "commandcode:5c7a09bd00000002"
	const acctC = "commandcode:5c7a09bd00000003"

	bindDispatching := func(n int, job, account string) hubSpawnTask {
		t.Helper()
		op := liveOp(t, h, n, job, "auto", now)
		node := hubSpawnRequest{RequestID: fmt.Sprintf("d676220b-e763-4af1-aefd-%012d", 5000+n), Machine: "machine-a", Args: []string{"-m", "cmd-dsflash41"}}
		bound, err := h.inbox.store.bindSpawnPlaced(t.Context(), op.Request.RequestID, op.Attempt, node, account, now)
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := h.inbox.store.beginSpawnSend(t.Context(), bound); err != nil || !ok {
			t.Fatalf("begin send %s: %v %v", job, ok, err)
		}
		return bound
	}

	// 1. Operation 1 is in-flight: bound on acctA in the placed columns, dispatching.
	// Its terminal node result has NOT arrived yet.
	task1 := bindDispatching(1, "job-dispatching-1", acctA)

	// 2. Operation 2 is in-flight on acctB the same way.
	_ = bindDispatching(2, "job-dispatching-2", acctB)

	// 3. Operation 3 is dispatching with only a placement snapshot and no
	// columns: the snapshot is a delivery record, not occupancy, so op3 is
	// not counted (hk 5124).
	task3 := liveOp(t, h, 3, "job-dispatching-3", "auto", now)
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='dispatching' WHERE request_id=?`, task3.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	tr := fleet.PlacementTrace{Chosen: "machine-b", Profile: "cmd-dsflash41", Pool: "commandcode", AccountKey: acctC}
	trBytes, err := json.Marshal(tr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.db.Exec(`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,delivery,recorded) VALUES(?,?,?,?,0)`, task3.Request.RequestID, task3.Attempt, string(trBytes), "{}"); err != nil {
		t.Fatal(err)
	}
	// Verify liveAccountJobs counts the two column-placed operations, and not
	// the snapshot-only one.
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
	if job, ok := got[acctC]; ok {
		t.Fatalf("liveAccountJobs[%s] = %q, want omitted (snapshot without placed columns)", acctC, job)
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

// TestRepeatSpawnClearsStaleAccountKey_t5113 proves that the retryable and
// repeat paths clear the placed columns in the same write that leaves the
// attempt, and that the bind of the next attempt records its own seat
// (hk 5113 review finding fix, hk 5124 columns).
func TestRepeatSpawnClearsStaleAccountKey_t5113(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	const acctA = "commandcode:5c7a09bd00000001"
	const acctB = "commandcode:5c7a09bd00000002"

	task := liveOp(t, h, 1, "job-repeat-1", "auto", now)
	node := hubSpawnRequest{RequestID: "d676220b-e763-4af1-aefd-0000000000c1", Machine: "machine-a", Args: []string{"-m", "cmd-dsflash41"}}
	bound, err := h.inbox.store.bindSpawnPlaced(t.Context(), task.Request.RequestID, task.Attempt, node, acctA, now)
	if err != nil {
		t.Fatal(err)
	}
	if bound.PlacedAccountKey != acctA || bound.PlacedAttempt != 1 {
		t.Fatalf("bound placed = %q/%d, want %q/1", bound.PlacedAccountKey, bound.PlacedAttempt, acctA)
	}
	// Complete attempt 1 with a retryable outcome: the status change clears
	// the columns in the same write.
	failed := hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: "checkout_refused:busy", Host: "desktop", Profile: "cmd-dsflash41", Retryable: true, Pool: "commandcode"}
	if _, err := h.inbox.store.completeSpawn(t.Context(), bound, failed, now); err != nil {
		t.Fatal(err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.PlacedAccountKey != "" || saved.PlacedAttempt != 0 {
		t.Fatalf("completed placed = %q/%d, want cleared", saved.PlacedAccountKey, saved.PlacedAttempt)
	}

	// Repeat the task -> status becomes 'accepted', attempt becomes 2, still
	// holding no seat.
	repeated, err := h.inbox.store.resumeCleanedSpawn(t.Context(), saved, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if repeated.PlacedAccountKey != "" || repeated.PlacedAttempt != 0 {
		t.Fatalf("repeated placed = %q/%d, want cleared", repeated.PlacedAccountKey, repeated.PlacedAttempt)
	}

	// liveAccountJobs should not report acctA for this repeated accepted task
	jobs, err := h.liveAccountJobs(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if job, ok := jobs[acctA]; ok {
		t.Fatalf("liveAccountJobs reported stale %s=%s for repeated task", acctA, job)
	}

	// When the bind places attempt 2 on acctB, the columns name acctB. The
	// retry stays pinned to the failed attempt's host (verified-cleanup
	// retry), so the bind honors that pin with a new account.
	node2 := hubSpawnRequest{RequestID: "d676220b-e763-4af1-aefd-0000000000c2", Machine: "desktop", Args: []string{"-m", "cmd-dsflash41"}}
	rebound, err := h.inbox.store.bindSpawnPlaced(t.Context(), repeated.Request.RequestID, repeated.Attempt, node2, acctB, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if rebound.PlacedAccountKey != acctB || rebound.PlacedAttempt != 2 {
		t.Fatalf("rebound placed = %q/%d, want %q/2", rebound.PlacedAccountKey, rebound.PlacedAttempt, acctB)
	}
	jobs, err = h.liveAccountJobs(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if jobs[acctB] != "job-repeat-1" {
		t.Fatalf("liveAccountJobs[%s] = %q, want %q", acctB, jobs[acctB], "job-repeat-1")
	}
}

// TestRecordPlacedAccountPreservesLegacyHostClaim_t5113 proves that the
// occupancy writes never touch result: a legacy-pinned row keeps byte-identical
// result across the bind and the unbind, while the seat lives in the placed
// columns (hk 5113 review finding fix, hk 5124 columns).
func TestRecordPlacedAccountPreservesLegacyHostClaim_t5113(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	const acctA = "commandcode:5c7a09bd00000001"
	const legacyHost = "mac-pinned-host"

	task := liveOp(t, h, 1, "job-legacy-pin-1", "auto", now)
	// Seed a legacy claim in result (Retryable + Host, no claimed_host or claim_result)
	legacyOutcome := hubSpawnOutcome{Host: legacyHost, Retryable: true, Stage: "launch"}
	rawLegacy, _ := json.Marshal(legacyOutcome)
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET result=? WHERE request_id=?`, string(rawLegacy), task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	readResult := func() string {
		t.Helper()
		var raw string
		if err := h.inbox.store.db.QueryRow(`SELECT result FROM spawn_operations WHERE request_id=?`, task.Request.RequestID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	before := readResult()

	// The bind records the seat in the placed columns, leaving result alone.
	node := hubSpawnRequest{RequestID: "d676220b-e763-4af1-aefd-0000000000c1", Machine: "machine-a", Args: []string{"-m", "cmd-dsflash41"}}
	bound, err := h.inbox.store.bindSpawnPlaced(t.Context(), task.Request.RequestID, task.Attempt, node, acctA, now)
	if err != nil {
		t.Fatal(err)
	}
	if bound.PlacedAccountKey != acctA || bound.PlacedAttempt != 1 {
		t.Fatalf("bound placed = %q/%d, want %q/1", bound.PlacedAccountKey, bound.PlacedAttempt, acctA)
	}
	if got := readResult(); got != before {
		t.Fatalf("bind rewrote result: before %q, after %q", before, got)
	}
	if bound.Outcome.Host != legacyHost {
		t.Fatalf("bound.Outcome.Host = %q, want %q", bound.Outcome.Host, legacyHost)
	}

	// The unbind clears the seat in the same write, still leaving result alone.
	unbound, err := h.inbox.store.unbindSpawn(t.Context(), task.Request.RequestID, task.Attempt, node.RequestID, "bound")
	if err != nil {
		t.Fatal(err)
	}
	if unbound.PlacedAccountKey != "" || unbound.PlacedAttempt != 0 {
		t.Fatalf("unbound placed = %q/%d, want cleared", unbound.PlacedAccountKey, unbound.PlacedAttempt)
	}
	if got := readResult(); got != before {
		t.Fatalf("unbind rewrote result: before %q, after %q", before, got)
	}
}
