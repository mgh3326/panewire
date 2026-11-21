package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The node half of the ladder close operation (hk 5863): one durable record
// per K = (job, node, epoch, attempt, pane); a stored result is replayed and
// never re-run; an interrupted closing is unknown and never runs again;
// nothing is closed without a durable intent.

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

func wantLadder(t *testing.T, result map[string]any, code, state string, replayed bool) {
	t.Helper()
	if code != "" || result["state"] != state || (result["replayed"] == true) != replayed {
		t.Fatalf("code=%q result=%v, want state %q replayed %v", code, result, state, replayed)
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

// Two request ids for one K share the record: the pane is closed once, and the
// second request id answers the first one's stored result. A replay of the
// same request id, even after a node restart, answers the stored result too.
func TestNodeLadderCloseTwoRequestIDsShareOneRecord(t *testing.T) {
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
		t.Fatalf("one record per K, got %v", names)
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

// A record left closing means a close may have run and its result was never
// recorded: the repeat settles it unknown and never touches herdr, whatever
// now holds the pane id.
func TestNodeLadderCloseInterruptedClosingIsUnknownOnReplay(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	store, err := client.openLadderStore()
	if err != nil {
		t.Fatal(err)
	}
	args := nodeLadderCloseArgs{Job: closeOpJob, Node: "machine-a", Epoch: 1, Attempt: closeOpAttempt, PaneID: "w1:p1"}
	record := nodeLadderRecord{Job: closeOpJob, Node: "machine-a", Epoch: 1, Attempt: closeOpAttempt, PaneID: "w1:p1", State: nodeLadderClosing}
	if err := store.write(ladderKName(ladderKey(args)), record); err != nil {
		t.Fatal(err)
	}
	store.close()
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderUnknown, true)
	if len(fake.calls) != 0 {
		t.Fatalf("a settled-unknown repeat touched herdr: %v", fake.calls)
	}
	// Once settled it is final: a second request id for the same K replays
	// the unknown.
	result, code = nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest2))
	wantLadder(t, result, code, nodeLadderUnknown, true)
}

// A journal that cannot be written closes nothing. The store-open failure is
// covered everywhere; the intent-write and settle-write failures need a
// read-only directory, which does not stop root.
func TestNodeLadderCloseWithoutADurableIntentClosesNothing(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	// ladder-close exists as a regular file: the store cannot open.
	if err := os.WriteFile(filepath.Join(root, ladderCloseDir), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "journal_failed" {
		t.Fatalf("code=%q", code)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("closed without a journal: %v", fake.calls)
	}
	if os.Geteuid() == 0 {
		t.Skip("a read-only directory does not stop root")
	}
	if err := os.Remove(filepath.Join(root, ladderCloseDir)); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, ladderCloseDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "journal_failed" {
		t.Fatalf("code=%q", code)
	}
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed without a durable intent: %v", fake.calls)
	}
	// A record left closing whose settle cannot be written is reported as a
	// failure too: there is no durable result, no further close.
	args := nodeLadderCloseArgs{Job: closeOpJob, Node: "machine-a", Epoch: 2, Attempt: closeOpAttempt, PaneID: "w1:p2"}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := client.openLadderStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.write(ladderKName(ladderKey(args)), nodeLadderRecord{Job: closeOpJob, Node: "machine-a", Epoch: 2, Attempt: closeOpAttempt, PaneID: "w1:p2", State: nodeLadderClosing}); err != nil {
		t.Fatal(err)
	}
	store.close()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o700)
	bad := ladderArgs(ladderRequest2)
	bad["epoch"], bad["pane_id"] = 2, "w1:p2"
	if _, code := nodeRun(t, client, nodeCommandLadderClose, bad); code != "journal_failed" {
		t.Fatalf("settle code=%q", code)
	}
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed while the settle could not be written: %v", fake.calls)
	}
}

// A request with no epoch or attempt binding has no provenance and is refused
// before anything is looked at; there is no permissive fallback.
func TestNodeLadderCloseMissingProvenanceRefused(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	for name, mutate := range map[string]func(map[string]any){
		"no epoch":   func(a map[string]any) { a["epoch"] = 0 },
		"no attempt": func(a map[string]any) { a["attempt"] = "" },
		"no node":    func(a map[string]any) { a["node"] = "" },
	} {
		t.Run(name, func(t *testing.T) {
			args := ladderArgs(ladderRequest)
			mutate(args)
			if _, code := nodeRun(t, client, nodeCommandLadderClose, args); code != "invalid_args" {
				t.Fatalf("code=%q", code)
			}
		})
	}
	if len(fake.calls) != 0 {
		t.Fatalf("a provenance-less request touched herdr: %v", fake.calls)
	}
	if names := ladderRecords(t, root); len(names) != 0 {
		t.Fatalf("a refused request left a record: %v", names)
	}
}

// A refusal closed nothing and stores nothing: the next request for the same
// K is judged afresh.
func TestNodeLadderCloseRefusalStoresNothing(t *testing.T) {
	root := t.TempDir()
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "working", "/work/a"))}, read: nodeReapScreen("❯ ")}
	client := ladderNode(t, fake, root)
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderRefused, false)
	if result["reason"] != nodePaneChanged {
		t.Fatalf("reason=%v", result["reason"])
	}
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

// The op is hub-issued: the public endpoint refuses it.
func TestLadderCloseIsHubOnly(t *testing.T) {
	hub := &HubServer{}
	if !knownNodeCommandOp(nodeCommandLadderClose) {
		t.Fatal("pane.ladder_close is not a node op")
	}
	if access := hub.authorizeNodeCommand(nil, nodeCommandLadderClose, "machine-a"); access != hubAccessForbidden {
		t.Fatalf("access=%v", access)
	}
}
