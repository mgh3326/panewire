package panewire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// closeOpNode is a node with a job directory for job-a and a fake herdr.
func closeOpNode(t *testing.T, inbox string, fake *nodeFakeHerdr) *HubClient {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0o700); err != nil {
		t.Fatal(err)
	}
	return &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox}
}

func closeOpArgs(id string) map[string]any {
	return map[string]any{"request_id": id, "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false}
}

func closeOpRef(id string) map[string]any { return map[string]any{"request_id": id, "name": "job-a"} }

func idlePaneHerdr() *nodeFakeHerdr {
	return &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"))}, read: nodeReapScreen("❯ ")}
}

func goneHerdr() *nodeFakeHerdr {
	return &nodeFakeHerdr{agents: []string{nodeAgentList()}, read: nodeReapScreen("❯ ")}
}

func seedNodeCloseOp(t *testing.T, inbox, id, state string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(filepath.Join(inbox, "jobs", "job-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	now := time.Now().UTC()
	if err := fleet.WriteCloseOp(root, fleet.CloseOp{RequestID: id, PaneID: "w1:p1", State: state, Created: now, Updated: now}); err != nil {
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
// the pane is absent, and the outcome is unknown — not gone, not closed.
func TestCloseOpRestartBetweenCloseAndRecordIsUnknown_t4594(t *testing.T) {
	inbox := t.TempDir()
	seedNodeCloseOp(t, inbox, "op-1", fleet.CloseOpClosing)
	wantCloseOp(t, closeOpNode(t, inbox, goneHerdr()), "op-1", fleet.CloseOpUnknown, false)
	wantCloseOp(t, closeOpNode(t, inbox, goneHerdr()), "op-1", fleet.CloseOpUnknown, true)

	// A herdr close that failed leaves closing; a pane found absent after it
	// is likewise unknown.
	failing := idlePaneHerdr()
	failing.closeErr = errors.New("close failed")
	if _, code := nodeRun(t, closeOpNode(t, inbox, failing), nodeCommandPaneCloseOp, closeOpArgs("op-2")); code != "close_failed" {
		t.Fatalf("failed close code=%q", code)
	}
	wantCloseStatus(t, closeOpNode(t, inbox, goneHerdr()), nodeCommandPaneCloseRelease, "op-2", fleet.CloseOpClosing, false)
	wantCloseOp(t, closeOpNode(t, inbox, goneHerdr()), "op-2", fleet.CloseOpUnknown, false)

	// An interrupted close whose pane is still there is closed again under
	// the same guards.
	seedNodeCloseOp(t, inbox, "op-3", fleet.CloseOpClosing)
	again := idlePaneHerdr()
	wantCloseOp(t, closeOpNode(t, inbox, again), "op-3", fleet.CloseOpClosed, false)
	if again.called("pane", "close") != 1 {
		t.Fatalf("calls=%v", again.calls)
	}
}

// The journal is the job's own: no job directory, no close; a request id
// reused for another pane is refused; and the public endpoint refuses all
// three ops.
func TestCloseOpBoundaries_t4594(t *testing.T) {
	inbox := t.TempDir()
	fake := idlePaneHerdr()
	client := &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox}
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
		{"request_id": "../x", "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false},
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
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0o700); err != nil {
		t.Fatal(err)
	}
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
	args := nodePaneCloseOpArgs{RequestID: "op-1", PaneID: "w1:p1", Name: "job-a", AgentStatus: "idle", Cwd: "/work/a"}
	var closed nodePaneCloseOpResult
	if !hub.nodeCall(t.Context(), "machine-a", nodeCommandPaneCloseOp, args, 5*time.Second, &closed) || closed.Outcome != fleet.CloseOpClosed || closed.Replayed {
		t.Fatalf("close_op=%+v", closed)
	}
	if !hub.nodeCall(t.Context(), "machine-a", nodeCommandPaneCloseOp, args, 5*time.Second, &closed) || closed.Outcome != fleet.CloseOpClosed || !closed.Replayed {
		t.Fatalf("replayed close_op=%+v", closed)
	}
	ref := nodeCloseOpRefArgs{RequestID: "op-1", Name: "job-a"}
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
