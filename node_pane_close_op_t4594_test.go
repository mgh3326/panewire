package panewire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The job directory is jobs/job-1; herdr labels its pane job-a, as a spawn
// with --job job-1 -l job-a does.
const closeOpJob = "job-1"

// seedCloseOpJob makes the job directory with one claim event (its attempt)
// and, when aware, the marker a journal-aware watcher leaves.
func seedCloseOpJob(t *testing.T, inbox string, aware bool) string {
	t.Helper()
	dir := filepath.Join(inbox, "jobs", closeOpJob)
	if err := os.MkdirAll(filepath.Join(dir, "events"), 0o700); err != nil {
		t.Fatal(err)
	}
	claim := filepath.Join(dir, "events", "00001-job.claim.json")
	if _, err := os.Stat(claim); err != nil {
		if err := os.WriteFile(claim, []byte(`{"kind":"job.claim","created_at":"2026-11-14T00:00:00Z","payload":{}}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if aware {
		if err := fleet.MarkJournalAwareWatcher(dir); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// closeOpNode is a node with a journal-aware job directory and a fake herdr.
func closeOpNode(t *testing.T, inbox string, fake *nodeFakeHerdr) *HubClient {
	t.Helper()
	seedCloseOpJob(t, inbox, true)
	return &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox}
}

func closeOpArgs(id string) map[string]any {
	return map[string]any{"request_id": id, "job": closeOpJob, "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false}
}

func closeOpRef(id string) map[string]any { return map[string]any{"request_id": id, "job": closeOpJob} }

func idlePaneHerdr() *nodeFakeHerdr {
	return &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"))}, read: nodeReapScreen("❯ ")}
}

func goneHerdr() *nodeFakeHerdr {
	return &nodeFakeHerdr{agents: []string{nodeAgentList()}, read: nodeReapScreen("❯ ")}
}

func seedNodeCloseOp(t *testing.T, inbox, id, state string) {
	t.Helper()
	root, err := os.OpenRoot(seedCloseOpJob(t, inbox, true))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	attempt, err := fleet.JobDirAttemptKey(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := fleet.WriteCloseOp(root, fleet.CloseOp{RequestID: id, PaneID: "w1:p1", Attempt: attempt, State: state, Created: now, Updated: now}); err != nil {
		t.Fatal(err)
	}
}

func wantCloseOp(t *testing.T, client *HubClient, id, outcome string, replayed bool) {
	t.Helper()
	result, code := nodeRun(t, client, nodeCommandPaneCloseOp, closeOpArgs(id))
	if code != "" || result["outcome"] != outcome || result["replayed"] != replayed {
		t.Fatalf("close_op %s: result=%v code=%q, want %s replayed=%v", id, result, code, outcome, replayed)
	}
}

func wantCloseStatus(t *testing.T, client *HubClient, op, id, state string, released bool) {
	t.Helper()
	result, code := nodeRun(t, client, op, closeOpRef(id))
	if code != "" || result["state"] != state || result["released"] != released {
		t.Fatalf("%s %s: result=%v code=%q, want %s released=%v", op, id, result, code, state, released)
	}
}

// A lost acknowledgement: the hub repeats the request id and reads the
// recorded outcome; herdr is not asked to close a second time.
func TestCloseOpLostAckReplaysTheRecordedClose_t4594(t *testing.T) {
	inbox := t.TempDir()
	fake := idlePaneHerdr()
	client := closeOpNode(t, inbox, fake)
	wantCloseOp(t, client, "op-1", fleet.CloseOpClosed, false)
	// The node restarts; the pane is gone because this operation closed it.
	restarted := closeOpNode(t, inbox, goneHerdr())
	wantCloseOp(t, restarted, "op-1", fleet.CloseOpClosed, true)
	if closes := fake.called("pane", "close"); closes != 1 {
		t.Fatalf("closes=%d", closes)
	}
	wantCloseStatus(t, restarted, nodeCommandPaneCloseStatus, "op-1", fleet.CloseOpClosed, false)
	wantCloseStatus(t, restarted, nodeCommandPaneCloseRelease, "op-1", fleet.CloseOpClosed, true)
	wantCloseStatus(t, restarted, nodeCommandPaneCloseStatus, "op-1", fleet.CloseOpClosed, true)
	wantCloseStatus(t, restarted, nodeCommandPaneCloseStatus, "op-2", nodeCloseOpNone, false)
}

// A refused close is recorded as refused, never as gone, and replays so.
func TestCloseOpRefusedIsNotGone_t4594(t *testing.T) {
	inbox := t.TempDir()
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "working", "/work/a"))}, read: nodeReapScreen("❯ ")}
	client := closeOpNode(t, inbox, fake)
	wantCloseOp(t, client, "op-1", fleet.CloseOpRefused, false)
	wantCloseOp(t, closeOpNode(t, inbox, goneHerdr()), "op-1", fleet.CloseOpRefused, true)
	if fake.called("pane", "close") != 0 {
		t.Fatalf("refused close ran herdr close: %v", fake.calls)
	}
}

// A pane absent before the operation ever reached its close is gone: it
// vanished on its own, whether the node first looked now or after a restart
// that interrupted the operation before closing.
func TestCloseOpPaneVanishedIndependentlyIsGone_t4594(t *testing.T) {
	inbox := t.TempDir()
	wantCloseOp(t, closeOpNode(t, inbox, goneHerdr()), "op-1", fleet.CloseOpGone, false)

	seedNodeCloseOp(t, inbox, "op-2", fleet.CloseOpRegistered)
	wantCloseOp(t, closeOpNode(t, inbox, goneHerdr()), "op-2", fleet.CloseOpGone, false)
}

// A node restart between the close and its record: the journal says closing,
// and the outcome is unknown — not gone, not closed, not refused — without
// touching herdr, whatever now holds the pane id: the original may be closed
// and the id reused by another pane, even one with the same name and cwd.
func TestCloseOpRestartBetweenCloseAndRecordIsUnknown_t4594(t *testing.T) {
	inbox := t.TempDir()
	seedNodeCloseOp(t, inbox, "op-1", fleet.CloseOpClosing)
	wantCloseOp(t, closeOpNode(t, inbox, goneHerdr()), "op-1", fleet.CloseOpUnknown, false)
	wantCloseOp(t, closeOpNode(t, inbox, goneHerdr()), "op-1", fleet.CloseOpUnknown, true)
	for _, occupant := range []string{
		nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"), // same identity
		nodeAgentJSON("w1:p1", "other", "idle", "/work/b"), // reused by another
	} {
		seedNodeCloseOp(t, inbox, "op-reuse", fleet.CloseOpClosing)
		fake := &nodeFakeHerdr{agents: []string{nodeAgentList(occupant)}, read: nodeReapScreen("❯ ")}
		wantCloseOp(t, closeOpNode(t, inbox, fake), "op-reuse", fleet.CloseOpUnknown, false)
		if len(fake.calls) != 0 {
			t.Fatalf("replay of an interrupted close touched herdr: %v", fake.calls)
		}
		if err := os.Remove(filepath.Join(inbox, "jobs", closeOpJob, fleet.CloseOpDir, "op-reuse.json")); err != nil {
			t.Fatal(err)
		}
	}

	// A herdr close that failed leaves closing; a pane found absent after it
	// is likewise unknown.
	failing := idlePaneHerdr()
	failing.closeErr = errors.New("close failed")
	if _, code := nodeRun(t, closeOpNode(t, inbox, failing), nodeCommandPaneCloseOp, closeOpArgs("op-2")); code != "close_failed" {
		t.Fatalf("failed close code=%q", code)
	}
	wantCloseStatus(t, closeOpNode(t, inbox, goneHerdr()), nodeCommandPaneCloseRelease, "op-2", fleet.CloseOpClosing, false)
	wantCloseOp(t, closeOpNode(t, inbox, goneHerdr()), "op-2", fleet.CloseOpUnknown, false)
}

// closing is recorded before the final look at the pane, so the close still
// re-reads the pane after the journal write: a pane that went back to work
// during that write is refused, and one that vanished is gone.
func TestCloseOpRechecksThePaneAfterRecordingClosing_t4594(t *testing.T) {
	for _, test := range []struct {
		second, outcome string
	}{
		{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "working", "/work/a")), fleet.CloseOpRefused},
		{nodeAgentList(), fleet.CloseOpGone},
		{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a")), fleet.CloseOpClosed},
	} {
		inbox := t.TempDir()
		first := nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"))
		fake := &nodeFakeHerdr{agents: []string{first, test.second}, read: nodeReapScreen("❯ ")}
		client := closeOpNode(t, inbox, fake)
		journal := filepath.Join(inbox, "jobs", closeOpJob, fleet.CloseOpDir, "op-1.json")
		var stateAtSecondRead string
		run := fake.run
		client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
			if len(args) >= 2 && args[0] == "agent" && args[1] == "list" && fake.called("agent", "list") == 1 {
				raw, _ := os.ReadFile(journal)
				stateAtSecondRead = string(raw)
			}
			return run(ctx, args...)
		}
		wantCloseOp(t, client, "op-1", test.outcome, false)
		if !strings.Contains(stateAtSecondRead, `"state":"closing"`) {
			t.Fatalf("closing not recorded before the final look: %s", stateAtSecondRead)
		}
	}
}

// The watcher's committed ending of the attempt preempts a registration: the
// node closes nothing. A job whose watcher predates the journal gets no
// operation at all.
func TestCloseOpPreemptedAndUnsupported_t4594(t *testing.T) {
	inbox := t.TempDir()
	dir := seedCloseOpJob(t, inbox, true)
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	attempt, err := fleet.JobDirAttemptKey(root)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := fleet.CommitWatcherEnding(dir, "w1:p1", attempt, time.Now()); err != nil || !ok {
		t.Fatalf("commit=%v err=%v", ok, err)
	}
	fake := idlePaneHerdr()
	client := closeOpNode(t, inbox, fake)
	wantCloseOp(t, client, "op-1", fleet.CloseOpPreempted, false)
	wantCloseOp(t, client, "op-1", fleet.CloseOpPreempted, true)
	if len(fake.calls) != 0 {
		t.Fatalf("preempted operation touched herdr: %v", fake.calls)
	}

	old := t.TempDir()
	seedCloseOpJob(t, old, false)
	oldFake := idlePaneHerdr()
	wantCloseOp(t, &HubClient{nodeHerdr: oldFake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: old}, "op-1", nodeCloseOpUnsupported, false)
	if len(oldFake.calls) != 0 {
		t.Fatalf("unsupported job touched herdr: %v", oldFake.calls)
	}
	if _, err := os.Stat(filepath.Join(old, "jobs", closeOpJob, fleet.CloseOpDir, "op-1.json")); !os.IsNotExist(err) {
		t.Fatalf("unsupported job journalled an operation: %v", err)
	}
}

// The journal is the job's own: no job directory, no close; a request id
// reused for another pane is refused; and the public endpoint refuses all
// three ops.
func TestCloseOpBoundaries_t4594(t *testing.T) {
	inbox := t.TempDir()
	fake := idlePaneHerdr()
	client := &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox}
	// A directory named after the pane label is not the job's.
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0o700); err != nil {
		t.Fatal(err)
	}
	wantCloseOp(t, client, "op-1", nodeCaptureNotAJob, false)
	if fake.called("pane", "close") != 0 {
		t.Fatal("closed a pane with no job directory to journal in")
	}
	client = closeOpNode(t, inbox, fake)
	wantCloseOp(t, client, "op-1", fleet.CloseOpClosed, false)
	other := closeOpArgs("op-1")
	other["pane_id"] = "w1:p2"
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, other); code != "invalid_args" {
		t.Fatalf("reused id for another pane code=%q", code)
	}
	for _, bad := range []map[string]any{
		{"request_id": ".op", "job": closeOpJob, "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false},
		{"request_id": "op-1", "job": "../x", "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false},
		{"request_id": "../x", "job": closeOpJob, "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false},
		{"request_id": "op-1", "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a"},
	} {
		if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, bad); code != "invalid_args" {
			t.Fatalf("args=%v code=%q", bad, code)
		}
	}
	h := &HubServer{}
	for _, op := range []string{nodeCommandPaneCloseOp, nodeCommandPaneCloseStatus, nodeCommandPaneCloseRelease} {
		if access := h.authorizeNodeCommand(nil, op, "m1"); access != hubAccessForbidden {
			t.Fatalf("%s public access=%v", op, access)
		}
	}
}

// Release cancels an operation that never started its close; one left
// closing stays unreleased until a repeated close_op resolves it.
func TestCloseOpReleaseCancelsOnlyBeforeClosing_t4594(t *testing.T) {
	inbox := t.TempDir()
	seedNodeCloseOp(t, inbox, "op-1", fleet.CloseOpRegistered)
	fake := idlePaneHerdr()
	client := closeOpNode(t, inbox, fake)
	wantCloseStatus(t, client, nodeCommandPaneCloseRelease, "op-1", fleet.CloseOpCancelled, true)
	wantCloseOp(t, client, "op-1", fleet.CloseOpCancelled, true)
	if fake.called("pane", "close") != 0 {
		t.Fatalf("a cancelled operation closed the pane: %v", fake.calls)
	}
	seedNodeCloseOp(t, inbox, "op-2", fleet.CloseOpClosing)
	wantCloseStatus(t, client, nodeCommandPaneCloseRelease, "op-2", fleet.CloseOpClosing, false)
}

// The three ops cross a real hub connection from a node that advertises
// close_ops_v1, and the hub accepts their results.
func TestCloseOpOverHubConnection_t4594(t *testing.T) {
	hub, server := t14Hub(t)
	fake := idlePaneHerdr()
	client, err := NewHubClient(HubClientConfig{
		URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour,
		SpawnConfigPath:     filepath.Join(t.TempDir(), "missing-spawn.json"),
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("fixture") },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, errors.New("fixture") },
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, errors.New("fixture") },
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := t.TempDir()
	seedCloseOpJob(t, inbox, true)
	client.nodeHerdr = fake.run
	client.nodeCloseSettle = time.Millisecond
	client.jobsInboxRoot = inbox
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	eventually(t, 2*time.Second, "node connected with close_ops_v1", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes["machine-a"]
		return record != nil && record.agent != nil && record.state == "connected" && record.capabilities[hubCapabilityCloseOps]
	})
	args := nodePaneCloseOpArgs{RequestID: "op-1", Job: closeOpJob, PaneID: "w1:p1", Name: "job-a", AgentStatus: "idle", Cwd: "/work/a"}
	var closed nodePaneCloseOpResult
	if !hub.nodeCall(t.Context(), "machine-a", nodeCommandPaneCloseOp, args, 5*time.Second, &closed) || closed.Outcome != fleet.CloseOpClosed || closed.Replayed {
		t.Fatalf("close_op=%+v", closed)
	}
	if !hub.nodeCall(t.Context(), "machine-a", nodeCommandPaneCloseOp, args, 5*time.Second, &closed) || closed.Outcome != fleet.CloseOpClosed || !closed.Replayed {
		t.Fatalf("replayed close_op=%+v", closed)
	}
	ref := nodeCloseOpRefArgs{RequestID: "op-1", Job: closeOpJob}
	var status nodeCloseOpStatusResult
	if !hub.nodeCall(t.Context(), "machine-a", nodeCommandPaneCloseRelease, ref, 5*time.Second, &status) || status.State != fleet.CloseOpClosed || !status.Released {
		t.Fatalf("release=%+v", status)
	}
	if !hub.nodeCall(t.Context(), "machine-a", nodeCommandPaneCloseStatus, ref, 5*time.Second, &status) || !status.Released {
		t.Fatalf("status=%+v", status)
	}
	if fake.called("pane", "close") != 1 {
		t.Fatalf("calls=%v", fake.calls)
	}
}
