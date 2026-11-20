package panewire

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5776: a reviewer's replaced_by pointer is written only on a start proof,
// read afresh: the successor is a node-bound sent attempt (or succeeded) that
// carries the claim receipt, the hub's own sighting of its bound node reporting
// the job claimed (quotaStartProof). These tests are the four windows hk 5690 left
// open; each one fails on the code before the change.

// holdsClaim records the claim receipt as the node does: a grant-required attempt
// is granted its start, then the hub sees the bound node report the job claimed
// (claimed_ms).
func (f *quotaReplaceFixture) holdsClaim(t *testing.T, job string) {
	t.Helper()
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job)
	if err != nil || task.Node == nil {
		t.Fatalf("holdsClaim: %+v %v", task, err)
	}
	if task.Node.GrantRequired {
		if granted, err := f.hub.inbox.store.decideSpawnGrant(t.Context(), task.Node.RequestID, task.Node.Machine, time.Now()); err != nil || !granted {
			t.Fatalf("grant: %v %v", granted, err)
		}
	}
	f.sawClaim(t, job, task.Node.Machine, time.Now())
}

// sawClaim is the hub reading, at observed, a heartbeat of machine that lists the job.
func (f *quotaReplaceFixture) sawClaim(t *testing.T, job, machine string, observed time.Time) {
	t.Helper()
	if err := f.hub.inbox.store.markSpawnClaimed(t.Context(), job, machine, observed, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// independentHKRecord is a record a node-local launch left under the successor's
// generated name: hk has it, no operation of the hub's claimed it.
func (f *quotaReplaceFixture) independentHKRecord(job string) {
	f.jobs.mu.Lock()
	defer f.jobs.mu.Unlock()
	f.jobs.records[job] = map[string]any{"state": "claimed", "pool": "claude"}
}

// bindGrantSuccessor binds and sends the successor as a grant-required attempt.
func (f *quotaReplaceFixture) bindGrantSuccessor(t *testing.T, successor hubSpawnTask) hubSpawnTask {
	t.Helper()
	bound, err := f.hub.inbox.store.bindSpawn(t.Context(), successor.Request.RequestID, successor.Attempt, hubSpawnRequest{RequestID: t14RequestID(333402), Machine: "machine-a", GrantRequired: true}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return sentSpawnFixture(t, f.hub.inbox.store, bound)
}

// refuseBeforeClaim is a node refusal before the job claim: the attempt goes back
// to accepted with its destination released.
func (f *quotaReplaceFixture) refuseBeforeClaim(t *testing.T, sent hubSpawnTask) {
	t.Helper()
	exclusion := hubSpawnExclusion{Machine: sent.Node.Machine, Reason: "refused"}
	if _, err := f.hub.inbox.store.unbindRefusedSpawn(t.Context(), sent, exclusion, nil, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if got, err := f.hub.inbox.store.spawnTask(t.Context(), sent.Request.RequestID); err != nil || got.Status != "accepted" || got.Node != nil {
		t.Fatalf("setup: the refusal left %q node=%v %v, want accepted and unbound", got.Status, got.Node, err)
	}
}

func (f *quotaReplaceFixture) spawnApp(t *testing.T) *fleet.App {
	t.Helper()
	app, err := f.hub.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return app
}

// expectNoNodeCommand fails if the hub sends the node anything while done runs:
// the pane probe comes before any close or write, so a quiet node means no
// pointer was attempted.
func (f *quotaReplaceFixture) expectNoNodeCommand(t *testing.T, done <-chan bool, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
	defer cancel()
	read := make(chan []byte, 1)
	go func() {
		if _, payload, err := f.node.Read(ctx); err == nil {
			read <- payload
		}
	}()
	select {
	case payload := <-read:
		t.Fatalf("%s: the hub sent the node %s", what, payload)
	case wrote := <-done:
		if wrote {
			t.Fatalf("%s: a pointer was written", what)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatalf("%s: the pass did not finish", what)
	}
}

func (f *quotaReplaceFixture) requireRecordUntouched(t *testing.T) {
	t.Helper()
	f.jobs.mu.Lock()
	defer f.jobs.mu.Unlock()
	if state := f.jobs.records[f.job]["state"]; state != "spawned" {
		t.Fatalf("the predecessor's record was written: state %v", state)
	}
}

// Window 1 (hk 5776): the finalizer holds a snapshot that reads the successor as
// dispatching, and a pre-claim refusal parked it back in accepted since. The
// pointer is decided on the row read afresh, so none is written.
func TestQuotaReplaceWindow1StaleSnapshotOfAParkedSuccessorGetsNoPointer(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-51-p1")
	successor := f.quotaClaimedSuccessor(t, "accepted")
	sent := f.bindSuccessor(t, successor, true)
	f.holdsClaim(t, successor.Job) // even a claim stamp does not make an unbound row a start
	f.refuseBeforeClaim(t, sent)
	f.paneClosed = true
	app := f.spawnApp(t)
	done := make(chan bool, 1)
	go func() { done <- f.hub.finishQuotaReplacement(t.Context(), app, sent, f.job, nil) }()
	f.expectNoNodeCommand(t, done, "window 1")
	f.requireRecordUntouched(t)
}

// Window 1, later in the same call: the successor is proven when the finalizer
// starts, and a refusal parks it while the finalizer probes the predecessor's
// pane. The proof is taken again right before the write.
func TestQuotaReplaceWindow1RefusalDuringThePaneProbeGetsNoPointer(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-52-p1")
	successor := f.quotaClaimedSuccessor(t, "accepted")
	sent := f.bindSuccessor(t, successor, true)
	f.holdsClaim(t, successor.Job)
	f.paneClosed = true
	app := f.spawnApp(t)
	done := make(chan bool, 1)
	go func() { done <- f.hub.finishQuotaReplacement(t.Context(), app, sent, f.job, nil) }()
	probe := nodeCommandRead(t, f.node) // the proof held: the finalizer is at the pane probe
	f.refuseBeforeClaim(t, sent)
	roleReplyPanes(t, f.node, probe, []nodeAgentRow{}, true)
	f.expectNoNodeCommand(t, done, "window 1 during the probe")
	f.requireRecordUntouched(t)
}

// Window 2 (hk 5776): the start fence is committed and the hub exits before the
// row settles, so recovery reads a dispatching row. A fenced attempt can never
// be granted, so it never starts: no pointer.
func TestQuotaReplaceWindow2FencedSuccessorGetsNoPointer(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-53-p1")
	successor := f.quotaClaimedSuccessor(t, "accepted")
	sent := f.bindGrantSuccessor(t, successor)
	fenced, err := f.hub.inbox.store.fenceSpawnAttempt(t.Context(), sent.Request.RequestID, sent.Node.RequestID, time.Now())
	if err != nil || !fenced {
		t.Fatalf("setup: fence=%v %v", fenced, err)
	}
	if got, _ := f.hub.inbox.store.spawnTask(t.Context(), sent.Request.RequestID); got.Status != "dispatching" {
		t.Fatalf("setup: the hub exit left %q, want dispatching", got.Status)
	}
	f.paneClosed = true
	done := make(chan bool, 1)
	go func() { done <- f.hub.reconcileQuotaReplacements(t.Context()) > 0 }()
	f.expectNoNodeCommand(t, done, "window 2")
	f.requireRecordUntouched(t)
}

// Window 3 (hk 5776): a sent attempt is unbound by a node refusal while the
// pointer is being written, after the pass re-read the row under the lock.
func TestQuotaReplaceWindow3RefusalWhileThePointerIsBeingWrittenGetsNoPointer(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-54-p1")
	successor := f.quotaClaimedSuccessor(t, "accepted")
	sent := f.bindGrantSuccessor(t, successor)
	f.holdsClaim(t, successor.Job)
	f.paneClosed = true
	app := f.spawnApp(t)
	done := make(chan bool, 1)
	go func() { done <- f.hub.reconcileQuotaReplacement(t.Context(), app, sent, f.job) }()
	probe := nodeCommandRead(t, f.node)
	f.refuseBeforeClaim(t, sent)
	roleReplyPanes(t, f.node, probe, []nodeAgentRow{}, true)
	f.expectNoNodeCommand(t, done, "window 3")
	f.requireRecordUntouched(t)
}

// Window 4 (hk 5776): the live worker's wait ends with the successor only bound,
// never sent. It is neither withdrawn nor pointed to: the stop stays with its
// node, which delivers it again.
func TestQuotaReplaceWindow4BoundSuccessorAtTheEndOfTheWaitGetsNoPointer(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-55-p1")
	f.hub.quotaWait = 150 * time.Millisecond
	acked := make(chan struct{}, 1)
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", quotaStopEvent(f.job, "claude"), func() { acked <- struct{}{} }, nil)
		done <- quotaReplaceResult{job, ok}
	}()
	f.serveCommands(t, 3) // probe, capture, close
	var successor hubSpawnTask
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), "mwr-55-p1-q1")
		return err == nil
	})
	f.bindSuccessor(t, successor, false)
	f.holdsClaim(t, successor.Job) // not even a claim stamp turns a bound row into a start
	select {
	case result := <-done:
		if result.ok {
			t.Fatalf("a bound successor got a pointer: %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the replacement did not finish")
	}
	select {
	case <-acked:
		t.Fatal("the stop was acknowledged with its successor unproven")
	default:
	}
	f.requireRecordUntouched(t)
}

// Window 4, the sent case: the wait ends with the successor sent but with no hk
// claim yet. It may still start, so it is not given up either; it gets its pointer
// once its claim exists.
func TestQuotaReplaceWindow4SentSuccessorWithoutAClaimIsPointedToOnceItIsClaimed(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-56-p1")
	f.hub.quotaWait = 150 * time.Millisecond
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job, "claude"))
		done <- quotaReplaceResult{job, ok}
	}()
	f.serveCommands(t, 3)
	var successor hubSpawnTask
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), "mwr-56-p1-q1")
		return err == nil
	})
	f.bindSuccessor(t, successor, true)
	select {
	case result := <-done:
		if result.ok {
			t.Fatalf("an unclaimed sent successor got a pointer: %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the replacement did not finish")
	}
	f.requireRecordUntouched(t)
	f.holdsClaim(t, successor.Job)
	f.paneClosed = true
	wrote := make(chan int, 1)
	go func() { wrote <- f.hub.reconcileQuotaReplacements(t.Context()) }()
	commands := f.serveCommands(t, 2)
	if reason := endReasonOf(t, commands[1]); reason != "quota_stop pool=claude replaced_by="+successor.Job {
		t.Fatalf("end_reason=%q, want the pointer to the claimed successor", reason)
	}
	if n := <-wrote; n != 1 {
		t.Fatalf("wrote %d records, want 1", n)
	}
}

// A successor that is node-bound and sent, with its claim receipt in hk, gets
// its pointer exactly once: a second pass finds the predecessor's record ended.
func TestQuotaReplaceStartProofWritesThePointerExactlyOnce(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-57-p1")
	successor := f.quotaClaimedSuccessor(t, "accepted")
	sent := f.bindGrantSuccessor(t, successor)
	f.holdsClaim(t, successor.Job)
	f.paneClosed = true
	if _, proven := f.hub.quotaStartProof(t.Context(), sent.Request.RequestID); !proven {
		t.Fatal("a sent, claimed attempt has no proof")
	}
	wrote := make(chan int, 1)
	go func() { wrote <- f.hub.reconcileQuotaReplacements(t.Context()) }()
	commands := f.serveCommands(t, 2) // probe (pane gone), mark_lost
	if reason := endReasonOf(t, commands[1]); reason != "quota_stop pool=claude replaced_by="+successor.Job {
		t.Fatalf("end_reason=%q, want the pointer to the successor", reason)
	}
	if n := <-wrote; n != 1 {
		t.Fatalf("wrote %d records, want 1", n)
	}
	// The hk fake applied the write; the next pass sees the ended record and sends
	// nothing.
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "lost"
	f.jobs.mu.Unlock()
	second := make(chan bool, 1)
	go func() { second <- f.hub.reconcileQuotaReplacements(t.Context()) > 0 }()
	f.expectNoNodeCommand(t, second, "second pass")
}

// The proof's own table: every state a successor can be in, read from the store.
func TestQuotaStartProofByState(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-58-p1")
	successor := f.quotaClaimedSuccessor(t, "accepted")
	proof := func() bool {
		_, ok := f.hub.quotaStartProof(t.Context(), successor.Request.RequestID)
		return ok
	}
	if proof() {
		t.Fatal("an accepted successor was proven")
	}
	sent := f.bindSuccessor(t, successor, false)
	f.independentHKRecord(successor.Job)
	f.holdsClaim(t, successor.Job)
	if proof() {
		t.Fatal("a bound, unsent successor was proven")
	}
	sent = sentSpawnFixture(t, f.hub.inbox.store, sent)
	if !proof() {
		t.Fatal("a sent successor with the claim receipt was not proven")
	}
	f.refuseBeforeClaim(t, sent)
	if proof() {
		t.Fatal("a refused, unbound successor was proven")
	}
	f.setSuccessorStatus(t, successor, "succeeded")
	if !proof() {
		t.Fatal("a succeeded successor was not proven")
	}
	for _, status := range []string{"failed", "retryable", "cancelled", "waiting"} {
		if _, err := f.hub.inbox.store.db.Exec(`UPDATE spawn_operations SET status=? WHERE request_id=?`, status, successor.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		if proof() {
			t.Fatalf("a %s successor was proven", status)
		}
	}
}

// The write is retried inside the watcher's grace, and the proof is taken again
// before every attempt: a first write that failed, and a refusal that unbinds the
// successor during the back-off, leave no second write.
func TestQuotaReplaceRetryOfTheWriteTakesTheProofAgain(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-59-p1")
	f.hub.quotaWait = 150 * time.Millisecond
	f.hub.quotaMarkBackoff = 400 * time.Millisecond
	acked := make(chan struct{}, 1)
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStoppedAck(t.Context(), "machine-a", quotaStopEvent(f.job, "claude"), func() { acked <- struct{}{} }, nil)
		done <- quotaReplaceResult{job, ok}
	}()
	f.serveCommands(t, 3) // probe, capture, close
	var successor hubSpawnTask
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), "mwr-59-p1-q1")
		return err == nil
	})
	sent := f.bindSuccessor(t, successor, true)
	f.holdsClaim(t, successor.Job)
	first := nodeCommandRead(t, f.node)
	if op := strings.Trim(string(first["op"]), `"`); op != "job.mark_lost" {
		t.Fatalf("first command after the proof is %s, want job.mark_lost", op)
	}
	nodeCommandReplyTo(t, f.node, first, map[string]any{"error": "herdr_failed"})
	f.refuseBeforeClaim(t, sent) // lands inside the back-off
	f.expectNoNodeCommandResult(t, done)
	f.requireRecordUntouched(t)
	// The stop stays with its node: acknowledged, no later delivery would reach the
	// give-up if the requeued successor then fails (hk 5776 review).
	select {
	case <-acked:
		t.Fatal("the stop was acknowledged after the proof was lost during the write")
	default:
	}
}

// expectNoNodeCommandResult waits for the replacement to end without a pointer and
// without another node command.
func (f *quotaReplaceFixture) expectNoNodeCommandResult(t *testing.T, done <-chan quotaReplaceResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2500*time.Millisecond)
	defer cancel()
	read := make(chan []byte, 1)
	go func() {
		if _, payload, err := f.node.Read(ctx); err == nil {
			read <- payload
		}
	}()
	select {
	case payload := <-read:
		t.Fatalf("the hub sent the node %s", payload)
	case result := <-done:
		if result.ok {
			t.Fatalf("a pointer was written: %+v", result)
		}
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("the replacement did not finish")
	}
}

// hk 5776 review: an hk record found under the successor's name is no evidence. A
// node-local launch can leave one without any operation of the hub's, and the node
// then refuses the hub's own claim as a duplicate. A sent successor with such a
// record and no claim receipt gets no pointer, here and at the end of a live wait.
func TestQuotaReplaceIndependentHKRecordIsNoStartProof(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-60-p1")
	successor := f.quotaClaimedSuccessor(t, "accepted")
	sent := f.bindSuccessor(t, successor, true)
	f.independentHKRecord(successor.Job)
	f.paneClosed = true
	if _, proven := f.hub.quotaStartProof(t.Context(), sent.Request.RequestID); proven {
		t.Fatal("an independent hk record was taken for the successor's claim")
	}
	done := make(chan bool, 1)
	go func() { done <- f.hub.reconcileQuotaReplacements(t.Context()) > 0 }()
	f.expectNoNodeCommand(t, done, "independent record")
	f.requireRecordUntouched(t)
}

// hk 5776 review: a heartbeat read for attempt A must not stamp attempt B. A node
// refusal unbinds A and the coordinator binds B (here on another host, and on the
// same one) before the late store write of A's claim runs. B is not claimed: no
// receipt, no pointer.
func TestQuotaReplaceStaleHeartbeatDoesNotStampTheNextAttempt(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-61-p1")
	successor := f.quotaClaimedSuccessor(t, "accepted")
	sent := f.bindSuccessor(t, successor, true)
	observed := time.Now()            // the heartbeat of attempt A on machine-a
	time.Sleep(20 * time.Millisecond) // fixture pacing: B's bind is later than the reading
	f.refuseBeforeClaim(t, sent)
	proof := func() bool {
		_, ok := f.hub.quotaStartProof(t.Context(), successor.Request.RequestID)
		return ok
	}
	// B on another host with the reading older than its bind, B on the same host
	// with the reading older than its bind, and B on another host with a reading
	// taken after its bind but from A's host.
	for _, c := range []struct {
		machine   string
		afterBind bool
	}{{"machine-b", false}, {"machine-a", false}, {"machine-b", true}} {
		machine := c.machine
		if _, err := f.hub.inbox.store.db.Exec(`UPDATE spawn_operations SET park_reason='',next_check_ms=0 WHERE request_id=?`, successor.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		current, err := f.hub.inbox.store.spawnTask(t.Context(), successor.Request.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		bound, err := f.hub.inbox.store.bindSpawn(t.Context(), current.Request.RequestID, current.Attempt, hubSpawnRequest{RequestID: t14RequestID(333410), Machine: machine}, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		sentB := sentSpawnFixture(t, f.hub.inbox.store, bound)
		// The late write of A's claim: wrong host, or a binding made after the reading.
		reading := observed
		if c.afterBind {
			reading = time.Now()
		}
		f.sawClaim(t, successor.Job, "machine-a", reading)
		if proof() {
			t.Fatalf("attempt B on %s (reading after bind: %v) was stamped with attempt A's heartbeat", machine, c.afterBind)
		}
		f.refuseBeforeClaim(t, sentB)
	}
}

// hk 5776 review: a grant-required attempt needs its grant as well as the claim
// stamp, and the state is read with the receipt: a row a failure settled keeps its
// node and its stamp but is no start.
func TestQuotaStartReceiptNeedsTheGrantAndASentState(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-62-p1")
	successor := f.quotaClaimedSuccessor(t, "accepted")
	sent := f.bindGrantSuccessor(t, successor)
	receipt := func() bool {
		ok, err := f.hub.inbox.store.spawnStartReceipt(t.Context(), sent.Request.RequestID, sent.Attempt, sent.Node.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	f.sawClaim(t, successor.Job, "machine-a", time.Now())
	if receipt() {
		t.Fatal("a stamped grant-required attempt with no grant was a receipt")
	}
	if granted, err := f.hub.inbox.store.decideSpawnGrant(t.Context(), sent.Node.RequestID, "machine-a", time.Now()); err != nil || !granted {
		t.Fatalf("grant %v %v", granted, err)
	}
	if !receipt() {
		t.Fatal("a granted, stamped, sent attempt was no receipt")
	}
	// A node failure settles the row; it keeps attempt, node id and stamp.
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), sent, hubSpawnOutcome{RC: 81, Stage: "landing", Reason: "failed", Host: "machine-a", NodeResult: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.hub.inbox.store.spawnTask(t.Context(), sent.Request.RequestID); got.Node == nil || got.Status == "dispatching" {
		t.Fatalf("setup: the failure left %q node=%v, want a settled row that keeps its node", got.Status, got.Node)
	}
	if receipt() {
		t.Fatal("a row settled by a failure was a receipt")
	}
}
