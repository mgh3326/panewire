package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/jobowner"
)

// The node half of the ladder close operation (hk 5676): the node writes its
// result before it replies, and a repeat of the request answers that result.

const ladderRequest = "5c1a2b3c-0000-4000-8000-000000000001"

func ladderNode(t *testing.T, fake *nodeFakeHerdr, root string) *HubClient {
	t.Helper()
	return &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: root}
}

func ladderArgs(id string) map[string]any {
	return map[string]any{"request_id": id, "job": "job-1", "node": "machine-a", "epoch": 1, "attempt": nodeLadderNoAttempt, "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false}
}

func wantLadder(t *testing.T, result map[string]any, code, state string, replayed bool) {
	t.Helper()
	if code != "" || result["state"] != state || (result["replayed"] == true) != replayed {
		t.Fatalf("code=%q result=%v, want state %q replayed %v", code, result, state, replayed)
	}
}

// Acceptance 2: a replay of the same operation, even after a node restart,
// closes the pane once and answers the stored result.
func TestNodeLadderCloseReplayClosesOnceAndAnswersTheStoredResult(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, false)
	// The pane is gone now; the same request must not look at it again.
	fake.agents = []string{nodeAgentList()}
	result, code = nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, true)
	restarted := ladderNode(t, fake, root)
	result, code = nodeRun(t, restarted, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, true)
	result, code = nodeRun(t, restarted, nodeCommandLadderCloseStatus, closeOpRef(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, false)
	if closes := fake.called("pane", "close"); closes != 1 {
		t.Fatalf("herdr closed the pane %d times: %v", closes, fake.calls)
	}
}

// The record is on disk, in its final state, before the node replies: a reply
// the hub never saw is still readable.
func TestNodeLadderCloseStoresTheResultBeforeItReplies(t *testing.T) {
	root := t.TempDir()
	client := ladderNode(t, idlePaneHerdr(), root)
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "" {
		t.Fatal(code)
	}
	raw, err := os.ReadFile(filepath.Join(root, ladderCloseDir, ladderRequest+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var record nodeLadderRecord
	if err := json.Unmarshal(raw, &record); err != nil || record.State != nodeLadderClosed || record.Epoch != 1 || record.PaneID != "w1:p1" {
		t.Fatalf("record=%s err=%v", raw, err)
	}
	if info, err := os.Stat(filepath.Join(root, ladderCloseDir, ladderRequest+".json")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v err=%v", info, err)
	}
}

// A pane absent when the operation looks was not closed by it.
func TestNodeLadderClosePaneAbsentIsGoneNotClosed(t *testing.T) {
	fake := goneHerdr()
	client := ladderNode(t, fake, t.TempDir())
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderGone, false)
	result, code = nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderGone, true)
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed an absent pane: %v", fake.calls)
	}
}

// A close the node began and did not finish is unknown on repeat, without
// touching herdr: its close may have run.
func TestNodeLadderCloseInterruptedIntentIsUnknownOnRepeat(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	rootDir, err := client.ladderCloseRoot()
	if err != nil {
		t.Fatal(err)
	}
	defer rootDir.Close()
	if err := writeNodeLadderRecord(rootDir, nodeLadderRecord{RequestID: ladderRequest, Job: "job-1", Node: "machine-a", Epoch: 1, Attempt: nodeLadderNoAttempt, PaneID: "w1:p1", State: nodeLadderClosing}); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderUnknown, true)
	if len(fake.calls) != 0 {
		t.Fatalf("a settled-unknown repeat touched herdr: %v", fake.calls)
	}
}

// A fenced request never acts, however late its close arrives.
func TestNodeLadderFenceStopsALateClose(t *testing.T) {
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, t.TempDir())
	result, code := nodeRun(t, client, nodeCommandLadderCloseFence, closeOpRef(ladderRequest))
	wantLadder(t, result, code, nodeLadderFenced, false)
	result, code = nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderFenced, true)
	if len(fake.calls) != 0 {
		t.Fatalf("a fenced request touched herdr: %v", fake.calls)
	}
}

// Fencing a request that already finished answers what it stored.
func TestNodeLadderFenceAnswersAFinalResultAsStored(t *testing.T) {
	client := ladderNode(t, idlePaneHerdr(), t.TempDir())
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "" {
		t.Fatal(code)
	}
	result, code := nodeRun(t, client, nodeCommandLadderCloseFence, closeOpRef(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, false)
	result, code = nodeRun(t, client, nodeCommandLadderCloseStatus, closeOpRef("5c1a2b3c-0000-4000-8000-0000000000ff"))
	wantLadder(t, result, code, nodeLadderNone, false)
}

// A repeat names its binding: another epoch, attempt, pane or job is refused.
func TestNodeLadderCloseRepeatMustNameTheSameBinding(t *testing.T) {
	client := ladderNode(t, idlePaneHerdr(), t.TempDir())
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "" {
		t.Fatal(code)
	}
	for field, value := range map[string]any{"epoch": 2, "attempt": "spawn-2", "pane_id": "w1:p9", "job": "job-2"} {
		args := ladderArgs(ladderRequest)
		args[field] = value
		if _, code := nodeRun(t, client, nodeCommandLadderClose, args); code != "invalid_args" {
			t.Fatalf("%s: code=%q", field, code)
		}
	}
}

// The three ops are hub-issued: the public endpoint refuses them.
func TestLadderCloseOpsAreHubOnly(t *testing.T) {
	hub := &HubServer{}
	for _, op := range []string{nodeCommandLadderClose, nodeCommandLadderCloseStatus, nodeCommandLadderCloseFence} {
		if !knownNodeCommandOp(op) {
			t.Fatalf("%s is not a node op", op)
		}
		if access := hub.authorizeNodeCommand(nil, op, "machine-a"); access != hubAccessForbidden {
			t.Fatalf("%s access=%v", op, access)
		}
	}
}

// A node offers the op only when it has a jobs root to keep results in.
func TestNodeAdvertisesLadderCloseOnlyWithAJobsRoot(t *testing.T) {
	with := &HubClient{jobsInboxRoot: t.TempDir()}
	without := &HubClient{}
	has := func(c *HubClient) bool {
		for _, capability := range c.spawnHelloCapabilities(t.Context()) {
			if capability == hubCapabilityLadderClose {
				return true
			}
		}
		return false
	}
	if !has(with) || has(without) {
		t.Fatalf("with=%v without=%v", has(with), has(without))
	}
}

// A restarted node has not loaded the epoch a job.assigned recorded; the close
// reads it from disk, so a request from an older owner closes nothing.
func TestNodeLadderCloseRefusesAStaleEpochReadFromDisk(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "jobs", "job-1", "events"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := jobowner.Assign(filepath.Join(root, "jobs"), "job-1", 3); err != nil {
		t.Fatal(err)
	}
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "epoch_mismatch" {
		t.Fatalf("code=%q", code)
	}
	if fake.called("pane", "close") != 0 {
		t.Fatalf("a stale owner closed the pane: %v", fake.calls)
	}
	args := ladderArgs(ladderRequest)
	args["epoch"] = 3
	if result, code := nodeRun(t, client, nodeCommandLadderClose, args); code != "" || result["state"] == nil {
		t.Fatalf("the current owner must close: %v %q", result, code)
	}
}

// The close names the attempt the job's own ledger holds, and the spawn
// receipt of that attempt must name this pane; a completed attempt is allowed,
// because an author's first READY completes its record and the pane goes on.
func TestNodeLadderCloseIsBoundToTheJobsCurrentAttempt(t *testing.T) {
	root := t.TempDir()
	dir := seedCloseOpJob(t, root)
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	status, code := nodeRun(t, client, nodeCommandLadderCloseStatus, closeOpRef(ladderRequest))
	if code != "" || status["state"] != nodeLadderNone || status["attempt"] != closeOpAttempt {
		t.Fatalf("status=%v code=%q", status, code)
	}
	// The hub decided on another attempt: nothing is closed, and the refusal is stored.
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderRefused, false)
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed a pane for a stale attempt: %v", fake.calls)
	}
	// The same attempt, after the author completed: still closed.
	if err := os.WriteFile(filepath.Join(dir, "events", "00003-job.completed.json"), []byte(`{"kind":"job.completed","created_at":"2026-11-14T00:00:02Z","payload":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	args := ladderArgs("5c1a2b3c-0000-4000-8000-000000000002")
	args["attempt"] = closeOpAttempt
	result, code = nodeRun(t, client, nodeCommandLadderClose, args)
	wantLadder(t, result, code, nodeLadderClosed, false)
	// A receipt for another pane is not this close's.
	other := ladderArgs("5c1a2b3c-0000-4000-8000-000000000003")
	other["attempt"], other["pane_id"] = closeOpAttempt, "w1:p9"
	result, code = nodeRun(t, client, nodeCommandLadderClose, other)
	if code != "" && code != "herdr_failed" || result != nil && result["state"] == nodeLadderClosed {
		t.Fatalf("result=%v code=%q", result, code)
	}
}

// The journal directory is created and made durable before it is relied on.
func TestNodeLadderCloseJournalDirectoryIsPrivate(t *testing.T) {
	root := t.TempDir()
	client := ladderNode(t, idlePaneHerdr(), root)
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "" {
		t.Fatal(code)
	}
	info, err := os.Stat(filepath.Join(root, ladderCloseDir))
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("info=%v err=%v", info, err)
	}
}

// An attempt started while the final look runs leaves the pane open under the
// superseded request, and the refusal is stored. (A grant or revocation cannot
// land during the final look: the ownership lock is held across it, which
// TestNodeLadderCloseHoldsTheOwnershipLockAcrossTheClose pins.)
func TestNodeLadderCloseRechecksOwnershipAndAttemptAtTheFinalLook(t *testing.T) {
	for name, change := range map[string]func(t *testing.T, root string){
		"reprompt": func(t *testing.T, root string) {
			body := `{"kind":"job.reprompted","created_at":"2026-11-14T00:00:05Z","payload":{}}`
			if err := os.WriteFile(filepath.Join(root, "jobs", closeOpJob, "events", "00003-job.reprompted.json"), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			seedCloseOpJob(t, root)
			fake := idlePaneHerdr()
			client := ladderNode(t, fake, root)
			run := fake.run
			client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
				if len(args) >= 2 && args[0] == "pane" && args[1] == "read" && fake.called("pane", "read") == 1 {
					change(t, root)
				}
				return run(ctx, args...)
			}
			args := ladderArgs(ladderRequest)
			args["attempt"] = closeOpAttempt
			result, code := nodeRun(t, client, nodeCommandLadderClose, args)
			wantLadder(t, result, code, nodeLadderRefused, false)
			if fake.called("pane", "close") != 0 {
				t.Fatalf("closed under a superseded request: %v", fake.calls)
			}
			again, code := nodeRun(t, client, nodeCommandLadderClose, args)
			wantLadder(t, again, code, nodeLadderRefused, true)
		})
	}
}

// The ownership check and the close are one step against a revocation: the
// job's ownership lock is held from the last check until herdr's close returns,
// so a revocation arriving then waits, and lands after the close.
func TestNodeLadderCloseHoldsTheOwnershipLockAcrossTheClose(t *testing.T) {
	root := t.TempDir()
	seedCloseOpJob(t, root)
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	run := fake.run
	revoked := make(chan struct{})
	earlyRevocation := false
	client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) >= 2 && strings.Join(args[:2], " ") == "pane close" {
			go func() {
				_ = jobowner.Revoke(filepath.Join(root, "jobs"), closeOpJob, 3)
				close(revoked)
			}()
			// A window that proves nothing more happens: the revocation must not
			// complete while the close is in flight.
			select {
			case <-revoked:
				earlyRevocation = true
			case <-time.After(150 * time.Millisecond):
			}
		}
		return run(ctx, args...)
	}
	args := ladderArgs(ladderRequest)
	args["attempt"] = closeOpAttempt
	result, code := nodeRun(t, client, nodeCommandLadderClose, args)
	wantLadder(t, result, code, nodeLadderClosed, false)
	if earlyRevocation {
		t.Fatal("a revocation landed between the last ownership check and the close")
	}
	select {
	case <-revoked:
	case <-time.After(2 * time.Second):
		t.Fatal("the revocation never completed after the close")
	}
}

// job.revoked leaves the grant file as it was, so the grant alone cannot say
// the node lost the job: the ownership check reads the revocation too, at the
// request, before the intent and at the last check. (A revocation that lands
// before the question scan stops the close there; the check covers the window
// after it.)
func TestNodeLadderOwnershipCheckReadsAHubRevocation(t *testing.T) {
	root := t.TempDir()
	seedCloseOpJob(t, root)
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	args := nodeLadderCloseArgs{Job: closeOpJob, Epoch: 1}
	if reason, err := client.ladderOwnerStale(args); err != nil || reason != "" {
		t.Fatalf("a fresh job: %q %v", reason, err)
	}
	if err := jobowner.Revoke(filepath.Join(root, "jobs"), closeOpJob, 3); err != nil {
		t.Fatal(err)
	}
	if reason, err := client.ladderOwnerStale(args); err != nil || reason != "revoked" {
		t.Fatalf("a revoked job: %q %v", reason, err)
	}
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "epoch_mismatch" || fake.called("pane", "close") != 0 {
		t.Fatalf("a revoked request closed: code=%q %v", code, fake.calls)
	}
	// A later grant at the revocation's epoch supersedes it.
	if err := jobowner.Assign(filepath.Join(root, "jobs"), closeOpJob, 3); err != nil {
		t.Fatal(err)
	}
	if reason, err := client.ladderOwnerStale(nodeLadderCloseArgs{Job: closeOpJob, Epoch: 3}); err != nil || reason != "" {
		t.Fatalf("epoch 3 after its own grant: %q %v", reason, err)
	}
}

// A writer holding the job's event lock stalls the close. The pane may go back
// to work meanwhile: a close that finds the lock busy waits for it and repeats
// the final pane, composer and question checks, so they see the pane as it is
// when the wait ends.
func TestNodeLadderCloseRepeatsThePaneChecksAfterALockWait(t *testing.T) {
	root := t.TempDir()
	seedCloseOpJob(t, root)
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	release, err := jobowner.Lock(filepath.Join(root, "jobs"), closeOpJob)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	type answer struct {
		result map[string]any
		code   string
	}
	done := make(chan answer, 1)
	args := ladderArgs(ladderRequest)
	args["attempt"] = closeOpAttempt
	go func() {
		result, code := nodeRun(t, client, nodeCommandLadderClose, args)
		done <- answer{result, code}
	}()
	eventually(t, 3*time.Second, "the close to read the pane once", func() bool { return fake.called("agent", "list") >= 1 })
	// Fixture pacing: let the call reach its wait on the lock before the pane changes.
	time.Sleep(200 * time.Millisecond)
	fake.mu.Lock()
	fake.agents = []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "working", "/work/a"))}
	fake.mu.Unlock()
	release()
	released = true
	got := <-done
	wantLadder(t, got.result, got.code, nodeLadderRefused, false)
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed a pane that went back to work during the lock wait: %v", fake.calls)
	}
}

// The lock wait is bounded by the command's context, so a stalled writer cannot
// hold the node's acting lock past the deadline: the close gives up without
// closing, and later commands (status, fence) are served.
func TestNodeLadderCloseLockWaitEndsWithTheCommandContext(t *testing.T) {
	root := t.TempDir()
	seedCloseOpJob(t, root)
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	release, err := jobowner.Lock(filepath.Join(root, "jobs"), closeOpJob)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	args := ladderArgs(ladderRequest)
	args["attempt"] = closeOpAttempt
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	type answer struct {
		result any
		code   string
	}
	finished := make(chan answer, 1)
	go func() {
		result, code := client.runNodeCommand(ctx, nodeCommandLadderClose, raw)
		finished <- answer{result, code}
	}()
	select {
	case got := <-finished:
		// The close never got the lock, so it closed nothing and says so: a
		// stored refusal, not an unknown close.
		if result, ok := got.result.(nodeLadderCloseResult); got.code != "" || !ok || result.State != nodeLadderRefused || result.Reason != "lock_busy" {
			t.Fatalf("result=%+v code=%q", got.result, got.code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the close kept waiting on the ownership lock past its context")
	}
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed without the ownership lock: %v", fake.calls)
	}
	statusDone := make(chan struct{})
	go func() {
		defer close(statusDone)
		nodeRun(t, client, nodeCommandLadderCloseFence, closeOpRef(ladderRequest))
	}()
	select {
	case <-statusDone:
	case <-time.After(3 * time.Second):
		t.Fatal("the node's acting lock stayed held after the timed-out close")
	}
}

// An author's completion is written under the job's event lock. A slow final
// pane look must not hold that lock: the close takes it only after the final
// pane, composer and question checks.
func TestNodeLadderCloseDoesNotHoldTheEventLockDuringTheFinalPaneLook(t *testing.T) {
	root := t.TempDir()
	seedCloseOpJob(t, root)
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	run := fake.run
	heldDuringLook := false
	client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "agent" && args[1] == "list" && fake.called("agent", "list") == 1 {
			unlock, err := jobowner.TryLock(filepath.Join(root, "jobs"), closeOpJob)
			if err != nil {
				heldDuringLook = true
			} else {
				unlock()
			}
		}
		return run(ctx, args...)
	}
	args := ladderArgs(ladderRequest)
	args["attempt"] = closeOpAttempt
	result, code := nodeRun(t, client, nodeCommandLadderClose, args)
	wantLadder(t, result, code, nodeLadderClosed, false)
	if heldDuringLook {
		t.Fatal("the close held the job's event lock during the final pane look")
	}
}

// A revocation writer creates the job directory and takes its lock; a request
// for a job the node holds no ledger for (attempt none) must not close under a
// revocation that lands after the ownership read.
func TestNodeLadderCloseWaitsForARevocationWriterOnAMissingJobDirectory(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	run := fake.run
	jobs := filepath.Join(root, "jobs")
	var once bool
	client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "agent" && args[1] == "list" && fake.called("agent", "list") == 1 && !once {
			once = true
			// The writer creates the directory and takes the lock now, and
			// publishes its marker a little later (fixture pacing).
			unlock, err := jobowner.Lock(jobs, closeOpJob)
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				defer unlock()
				time.Sleep(250 * time.Millisecond)
				events := filepath.Join(jobs, closeOpJob, "events")
				if err := os.MkdirAll(events, 0o700); err != nil {
					return
				}
				_ = os.WriteFile(filepath.Join(events, "00001-job.revoked.json"), []byte(`{"type":"job.revoked","job_id":"`+closeOpJob+`","epoch":3}`), 0o600)
			}()
		}
		return run(ctx, args...)
	}
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderRefused, false)
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed under a revocation: %v", fake.calls)
	}
}

// A job granted back to this node keeps its old hub revocation marker in the
// ledger (type job.revoked, no kind). A fresh attempt under the new grant must
// still close: the question scan reads that marker.
func TestNodeLadderCloseAfterARegrantReadsThePastRevocation(t *testing.T) {
	root := t.TempDir()
	dir := seedCloseOpJob(t, root)
	events := filepath.Join(dir, "events")
	for name, body := range map[string]string{
		"00003-job.revoked.json": `{"type":"job.revoked","job_id":"` + closeOpJob + `","epoch":2}`,
		"00004-job.spawned.json": `{"kind":"job.spawned","created_at":"2026-11-14T00:00:09Z","payload":{"pane_id":"w1:p1","label":"job-a","role":"implementer","herdr_session":"fleet"}}`,
	} {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := jobowner.Assign(filepath.Join(root, "jobs"), closeOpJob, 3); err != nil {
		t.Fatal(err)
	}
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	args := ladderArgs(ladderRequest)
	args["epoch"], args["attempt"] = 3, "00004-job.spawned.json"
	result, code := nodeRun(t, client, nodeCommandLadderClose, args)
	wantLadder(t, result, code, nodeLadderClosed, false)
	if fake.called("pane", "close") != 1 {
		t.Fatalf("closes=%d %v", fake.called("pane", "close"), fake.calls)
	}
}

// A lock that frees with less than the settle interval left must not leave the
// first pass's intent as an unknown close: neither pass reached `herdr pane
// close`, so the refusal is stored.
func TestNodeLadderCloseStoresARefusalWhenTheRetryRunsOutBeforeTheClose(t *testing.T) {
	root := t.TempDir()
	seedCloseOpJob(t, root)
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	client.nodeCloseSettle = 300 * time.Millisecond
	release, err := jobowner.Lock(filepath.Join(root, "jobs"), closeOpJob)
	if err != nil {
		t.Fatal(err)
	}
	// Fixture pacing: the writer lets go after the first pass has reached the
	// lock (about one settle) and well inside the second settle's window.
	go func() {
		time.Sleep(400 * time.Millisecond)
		release()
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer cancel()
	args := ladderArgs(ladderRequest)
	args["attempt"] = closeOpAttempt
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	result, code := client.runNodeCommand(ctx, nodeCommandLadderClose, raw)
	got, ok := result.(nodeLadderCloseResult)
	if code != "" || !ok || got.State != nodeLadderRefused {
		t.Fatalf("result=%+v code=%q", result, code)
	}
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed: %v", fake.calls)
	}
	fenced, code := nodeRun(t, client, nodeCommandLadderCloseFence, closeOpRef(ladderRequest))
	wantLadder(t, fenced, code, nodeLadderRefused, false)
}

// A grant back to this node (A/1 -> B/2 -> A/3) does not return the revoked
// pane: until a fresh claim or spawn receipt, the old receipt does not bind a
// close.
func TestNodeLadderCloseAfterARegrantNeedsAFreshReceipt(t *testing.T) {
	root := t.TempDir()
	dir := seedCloseOpJob(t, root)
	body := `{"type":"job.revoked","job_id":"` + closeOpJob + `","epoch":2}`
	if err := os.WriteFile(filepath.Join(dir, "events", "00003-job.revoked.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := jobowner.Assign(filepath.Join(root, "jobs"), closeOpJob, 3); err != nil {
		t.Fatal(err)
	}
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	args := ladderArgs(ladderRequest)
	args["epoch"], args["attempt"] = 3, closeOpAttempt
	result, code := nodeRun(t, client, nodeCommandLadderClose, args)
	wantLadder(t, result, code, nodeLadderRefused, false)
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed a revoked attempt's pane: %v", fake.calls)
	}
}
