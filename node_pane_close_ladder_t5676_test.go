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

// The node half of the ladder close operation (hk 5676): one durable record per
// (job, node, epoch, attempt, pane); a stored result is replayed and never
// re-run; the close is made under one composite fence; every ambiguity is stored
// as unknown.

const (
	ladderRequest  = "5c1a2b3c-0000-4000-8000-000000000001"
	ladderRequest2 = "5c1a2b3c-0000-4000-8000-000000000002"
)

// ladderNode is a node with a seeded job directory (pane w1:p1, label job-a,
// receipt closeOpAttempt) and a fake herdr.
func ladderNode(t *testing.T, fake *nodeFakeHerdr, root string) *HubClient {
	t.Helper()
	seedCloseOpJob(t, root)
	return &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: root}
}

func ladderArgs(id string) map[string]any {
	return map[string]any{"request_id": id, "job": closeOpJob, "node": "machine-a", "epoch": 1, "attempt": closeOpAttempt, "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false}
}

func ladderFenceArgs(id string) map[string]any {
	return map[string]any{"request_id": id, "job": closeOpJob}
}

func wantLadder(t *testing.T, result map[string]any, code, state string, replayed bool) {
	t.Helper()
	if code != "" || result["state"] != state || (result["replayed"] == true) != replayed {
		t.Fatalf("code=%q result=%v, want state %q replayed %v", code, result, state, replayed)
	}
}

func wantRefused(t *testing.T, result map[string]any, code, reason string) {
	t.Helper()
	wantLadder(t, result, code, nodeLadderRefused, false)
	if result["reason"] != reason {
		t.Fatalf("reason=%v, want %q", result["reason"], reason)
	}
}

func ladderRecords(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, ladderCloseDir))
	if err != nil {
		return nil
	}
	var names []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "k-") {
			names = append(names, entry.Name())
		}
	}
	return names
}

// Acceptance 2: a replay of the same operation, even after a node restart, and a
// second request id for the same (job, epoch, attempt, pane), close the pane
// once and answer the stored result.
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
	result, code = nodeRun(t, restarted, nodeCommandLadderClose, ladderArgs(ladderRequest2))
	wantLadder(t, result, code, nodeLadderClosed, true)
	if closes := fake.called("pane", "close"); closes != 1 {
		t.Fatalf("herdr closed the pane %d times: %v", closes, fake.calls)
	}
	if names := ladderRecords(t, root); len(names) != 1 {
		t.Fatalf("one record per binding, got %v", names)
	}
}

// The record is on disk, in its final state, before the node replies.
func TestNodeLadderCloseStoresTheResultBeforeItReplies(t *testing.T) {
	root := t.TempDir()
	client := ladderNode(t, idlePaneHerdr(), root)
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "" {
		t.Fatal(code)
	}
	names := ladderRecords(t, root)
	if len(names) != 1 {
		t.Fatalf("records=%v", names)
	}
	path := filepath.Join(root, ladderCloseDir, names[0])
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record nodeLadderRecord
	if err := json.Unmarshal(raw, &record); err != nil || record.State != nodeLadderClosed || record.Epoch != 1 || record.PaneID != "w1:p1" || record.Attempt != closeOpAttempt {
		t.Fatalf("record=%s err=%v", raw, err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v err=%v", info, err)
	}
	if info, err := os.Stat(filepath.Join(root, ladderCloseDir)); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("dir mode=%v err=%v", info, err)
	}
}

// A pane absent when the node looks was not closed by it.
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
	store, err := client.openLadderStore()
	if err != nil {
		t.Fatal(err)
	}
	args := nodeLadderCloseArgs{Job: closeOpJob, Node: "machine-a", Epoch: 1, Attempt: closeOpAttempt, PaneID: "w1:p1"}
	if err := store.write(ladderKName(ladderKey(args)), nodeLadderRecord{Job: closeOpJob, Node: "machine-a", Epoch: 1, Attempt: closeOpAttempt, PaneID: "w1:p1", State: nodeLadderClosing}); err != nil {
		t.Fatal(err)
	}
	store.close()
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
	result, code := nodeRun(t, client, nodeCommandLadderCloseFence, ladderFenceArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderFenced, false)
	result, code = nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderFenced, true)
	if len(fake.calls) != 0 {
		t.Fatalf("a fenced request touched herdr: %v", fake.calls)
	}
	// The fence is the request's, not the binding's: another request for the
	// same binding is judged afresh.
	result, code = nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest2))
	wantLadder(t, result, code, nodeLadderClosed, false)
}

// Fencing a request that already finished answers what it stored.
func TestNodeLadderFenceAnswersAFinalResultAsStored(t *testing.T) {
	client := ladderNode(t, idlePaneHerdr(), t.TempDir())
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "" {
		t.Fatal(code)
	}
	result, code := nodeRun(t, client, nodeCommandLadderCloseFence, ladderFenceArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, false)
}

// A request id first bound to one binding does not act under another: some close
// under the first may have run.
func TestNodeLadderCloseRepeatNamingAnotherBindingIsUnknown(t *testing.T) {
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, t.TempDir())
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "" {
		t.Fatal(code)
	}
	args := ladderArgs(ladderRequest)
	args["epoch"] = 2
	result, code := nodeRun(t, client, nodeCommandLadderClose, args)
	wantLadder(t, result, code, nodeLadderUnknown, true)
	if fake.called("pane", "close") != 1 {
		t.Fatalf("closes=%d", fake.called("pane", "close"))
	}
}

// A refusal closed nothing and stores nothing: the next request for the same
// binding is judged afresh.
func TestNodeLadderCloseRefusalStoresNothing(t *testing.T) {
	root := t.TempDir()
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "working", "/work/a"))}, read: nodeReapScreen("❯ ")}
	client := ladderNode(t, fake, root)
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantRefused(t, result, code, nodePaneChanged)
	if names := ladderRecords(t, root); len(names) != 0 {
		t.Fatalf("a refusal left a record: %v", names)
	}
	fake.mu.Lock()
	fake.agents = []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"))}
	fake.agentRead = 0
	fake.mu.Unlock()
	result, code = nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest2))
	wantLadder(t, result, code, nodeLadderClosed, false)
}

// The composite fence: the grant equals the request's epoch, no revocation
// stands, the attempt's receipt names this pane, written after the grant. Each
// failure closes nothing and stores nothing.
func TestNodeLadderCloseFenceRefusals(t *testing.T) {
	revoke := func(t *testing.T, root string) {
		body := `{"type":"job.revoked","job_id":"` + closeOpJob + `","epoch":3}`
		if err := os.WriteFile(filepath.Join(root, "jobs", closeOpJob, "events", "00003-job.revoked.json"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for name, tc := range map[string]struct {
		prepare func(t *testing.T, root string)
		args    func(map[string]any)
		reason  string
	}{
		"a newer grant": {prepare: func(t *testing.T, root string) {
			if err := jobowner.Assign(filepath.Join(root, "jobs"), closeOpJob, 3); err != nil {
				t.Fatal(err)
			}
		}, reason: "epoch_changed"},
		"a request epoch above the grant":    {args: func(a map[string]any) { a["epoch"] = 2 }, reason: "epoch_changed"},
		"a hub revocation above the grant":   {prepare: revoke, reason: "revoked"},
		"another attempt":                    {args: func(a map[string]any) { a["attempt"] = "00009-job.spawned.json" }, reason: "attempt_changed"},
		"a job the node holds no ledger for": {args: func(a map[string]any) { a["job"] = "job-other" }, reason: "no_provenance"},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			fake := idlePaneHerdr()
			client := ladderNode(t, fake, root)
			if tc.prepare != nil {
				tc.prepare(t, root)
			}
			args := ladderArgs(ladderRequest)
			if tc.args != nil {
				tc.args(args)
			}
			result, code := nodeRun(t, client, nodeCommandLadderClose, args)
			wantRefused(t, result, code, tc.reason)
			if fake.called("pane", "close") != 0 {
				t.Fatalf("closed under a failed fence: %v", fake.calls)
			}
			if names := ladderRecords(t, root); len(names) != 0 {
				t.Fatalf("a refusal left a record: %v", names)
			}
		})
	}
}

// An attempt that ended (the author's first READY) is still the author's pane.
func TestNodeLadderCloseAllowsAnEndedAttempt(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	body := `{"kind":"job.completed","created_at":"2026-11-14T00:00:02Z","payload":{}}`
	if err := os.WriteFile(filepath.Join(root, "jobs", closeOpJob, "events", "00003-job.completed.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, false)
}

// A grant back to this node (A/1 -> B/2 -> A/3), with the reassignment's
// revocation marker deleted: the grant is 3, no revocation stands, and the
// epoch-1 receipt is the old attempt's. Only its age shows, so a receipt older
// than the grant is refused, and one written after it closes.
func TestNodeLadderCloseAfterARegrantNeedsAReceiptWrittenAfterTheGrant(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	// seedCloseOpJob's receipt is older than this grant.
	time.Sleep(20 * time.Millisecond) // fixture pacing: a clock tick between receipt and grant
	if err := jobowner.Assign(filepath.Join(root, "jobs"), closeOpJob, 3); err != nil {
		t.Fatal(err)
	}
	args := ladderArgs(ladderRequest)
	args["epoch"] = 3
	result, code := nodeRun(t, client, nodeCommandLadderClose, args)
	wantRefused(t, result, code, "stale_receipt")
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed an old attempt's pane under the new grant: %v", fake.calls)
	}
	// A spawn under the new grant: its receipt is newer than the grant. The
	// question scan reads past the old revocation marker.
	events := filepath.Join(root, "jobs", closeOpJob, "events")
	for name, body := range map[string]string{
		"00003-job.revoked.json": `{"type":"job.revoked","job_id":"` + closeOpJob + `","epoch":2}`,
		"00004-job.spawned.json": `{"kind":"job.spawned","created_at":"2026-11-14T00:00:09Z","payload":{"pane_id":"w1:p1","label":"job-a","role":"implementer","herdr_session":"fleet"}}`,
	} {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	args = ladderArgs(ladderRequest2)
	args["epoch"], args["attempt"] = 3, "00004-job.spawned.json"
	result, code = nodeRun(t, client, nodeCommandLadderClose, args)
	wantLadder(t, result, code, nodeLadderClosed, false)
}

// The ownership lock is held from the composite check to the close's return, so
// a revocation writer lands after the close.
func TestNodeLadderCloseHoldsTheOwnershipLockAcrossTheClose(t *testing.T) {
	root := t.TempDir()
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
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, false)
	if earlyRevocation {
		t.Fatal("a revocation landed between the composite check and the close")
	}
	select {
	case <-revoked:
	case <-time.After(2 * time.Second):
		t.Fatal("the revocation never completed after the close")
	}
}

// An author's completion is written under the job's event lock. A slow final
// pane look must not hold that lock: the close tries it only after the final
// pane, composer and question checks.
func TestNodeLadderCloseDoesNotHoldTheEventLockDuringTheFinalPaneLook(t *testing.T) {
	root := t.TempDir()
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
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, false)
	if heldDuringLook {
		t.Fatal("the close held the job's event lock during the final pane look")
	}
}

// A job event writer holding the lock is contention: one try, no wait, no
// retry. The close is unknown (stored), nothing was closed, and the node's
// acting lock is free at once.
func TestNodeLadderCloseContentionIsUnknown(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	release, err := jobowner.LockContext(context.Background(), filepath.Join(root, "jobs"), closeOpJob)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderUnknown, false)
	if result["reason"] != "lock_busy" || fake.called("pane", "close") != 0 {
		t.Fatalf("result=%v closes=%d", result, fake.called("pane", "close"))
	}
	again, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, again, code, nodeLadderUnknown, true)
}

// A herdr failure after the intent is unknown, not a refusal: the node cannot
// say the close did not run.
func TestNodeLadderCloseFailureAfterTheIntentIsUnknown(t *testing.T) {
	fake := idlePaneHerdr()
	fake.closeErr = os.ErrClosed
	client := ladderNode(t, fake, t.TempDir())
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderUnknown, false)
	again, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, again, code, nodeLadderUnknown, true)
	if fake.called("pane", "close") != 1 {
		t.Fatalf("a failed close ran again: %v", fake.calls)
	}
}

// Nothing is closed without a durable intent: a journal that cannot be written
// closes nothing.
func TestNodeLadderCloseWithoutADurableIntentClosesNothing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("a read-only directory does not stop root")
	}
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	dir := filepath.Join(root, ladderCloseDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o700)
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "journal_failed" {
		t.Fatalf("code=%q", code)
	}
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed without a durable intent: %v", fake.calls)
	}
}

// The ops are hub-issued: the public endpoint refuses them.
func TestLadderCloseOpsAreHubOnly(t *testing.T) {
	hub := &HubServer{}
	for _, op := range []string{nodeCommandLadderClose, nodeCommandLadderCloseFence} {
		if !knownNodeCommandOp(op) {
			t.Fatalf("%s is not a node op", op)
		}
		if access := hub.authorizeNodeCommand(nil, op, "machine-a"); access != hubAccessForbidden {
			t.Fatalf("%s access=%v", op, access)
		}
	}
	if knownNodeCommandOp("pane.ladder_close_status") {
		t.Fatal("the status op is gone")
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

// pane.capture carries the attempt token the close names, for a job the node
// holds a spawn receipt for, and only to a hub that advertised it.
func TestNodePaneCaptureReportsTheAttemptForALadderClose(t *testing.T) {
	root := t.TempDir()
	client := ladderNode(t, idlePaneHerdr(), root)
	if got := client.ladderAttempt(closeOpJob); got != closeOpAttempt {
		t.Fatalf("attempt=%q", got)
	}
	if got := client.ladderAttempt("job-other"); got != "" {
		t.Fatalf("a job with no ledger has no provenance: %q", got)
	}
	peer := &hubClientConnection{}
	capture := nodePaneCaptureResult{Outcome: nodeCaptureScreen, Saved: "/x/pane-screen.txt", Attempt: closeOpAttempt}
	if got := peer.nodeCommandWire(capture).(nodePaneCaptureResult); got.Attempt != "" {
		t.Fatalf("an older hub must not see the attempt: %+v", got)
	}
	peer.acceptsPaneCaptureAttempt.Store(true)
	if got := peer.nodeCommandWire(capture).(nodePaneCaptureResult); got.Attempt != closeOpAttempt {
		t.Fatalf("a hub that advertised it must: %+v", got)
	}
}
