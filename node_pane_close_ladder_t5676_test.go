package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The node half of the ladder close operation (hk 5676): the node writes its
// result before it replies, and a repeat of the request answers that result.

const ladderRequest = "5c1a2b3c-0000-4000-8000-000000000001"

func ladderNode(t *testing.T, fake *nodeFakeHerdr, root string) *HubClient {
	t.Helper()
	return &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: root}
}

func ladderArgs(id string) map[string]any {
	return map[string]any{"request_id": id, "job": "job-1", "node": "machine-a", "epoch": 1, "attempt": "spawn-1", "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false}
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
	if err := writeNodeLadderRecord(rootDir, nodeLadderRecord{RequestID: ladderRequest, Job: "job-1", Node: "machine-a", Epoch: 1, Attempt: "spawn-1", PaneID: "w1:p1", State: nodeLadderClosing}); err != nil {
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
