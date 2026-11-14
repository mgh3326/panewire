package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The job directory is jobs/job-1; herdr labels its pane job-a, as a spawn
// with --job job-1 -l job-a does.
const closeOpJob = "job-1"

// closeOpAttempt is the seeded attempt: its spawn receipt's event file.
const closeOpAttempt = "00002-job.spawned.json"

// seedCloseOpJob makes the job directory with a claim and a spawn receipt
// binding the attempt to pane w1:p1 and label job-a (a reviewer).
func seedCloseOpJob(t *testing.T, inbox string) string {
	t.Helper()
	dir := filepath.Join(inbox, "jobs", closeOpJob)
	if err := os.MkdirAll(filepath.Join(dir, "events"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"00001-job.claim.json":   `{"kind":"job.claim","created_at":"2026-11-14T00:00:00Z","payload":{"owner_lane":"owner"}}`,
		"00002-job.spawned.json": `{"kind":"job.spawned","created_at":"2026-11-14T00:00:01Z","payload":{"pane_id":"w1:p1","label":"job-a","role":"reviewer","herdr_session":"fleet"}}`,
	} {
		path := filepath.Join(dir, "events", name)
		if _, err := os.Stat(path); err != nil {
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	return dir
}

// closeOpNode is a node with the job directory and a fake herdr.
func closeOpNode(t *testing.T, inbox string, fake *nodeFakeHerdr) *HubClient {
	t.Helper()
	seedCloseOpJob(t, inbox)
	return &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox,
		nodeHubCloseIntent: noHubCloseIntent}
}

// noHubCloseIntent stands in for the ledger write, so a fixture never reaches
// the developer's arbiter state.
func noHubCloseIntent(context.Context, string, fleet.HubCloseIntent) error { return nil }

func closeOpArgs(id string) map[string]any {
	return map[string]any{"request_id": id, "job": closeOpJob, "attempt": closeOpAttempt, "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false, "end_reason": "quota_stop pool=devin-free replaced_by=job-c"}
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
	root, err := os.OpenRoot(seedCloseOpJob(t, inbox))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	target, err := readNodeCloseTarget(root)
	if err != nil {
		t.Fatal(err)
	}
	attempt := target.attempt
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

// The journal is the job's own: no job directory, no close; a request id
// reused for another pane is refused; and the public endpoint refuses all
// three ops.
func TestCloseOpBoundaries_t4594(t *testing.T) {
	inbox := t.TempDir()
	fake := idlePaneHerdr()
	client := &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox, nodeHubCloseIntent: noHubCloseIntent}
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
		{"request_id": ".op", "job": closeOpJob, "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false, "end_reason": "quota_stop pool=devin-free replaced_by=job-c"},
		{"request_id": "op-1", "job": "../x", "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false, "end_reason": "quota_stop pool=devin-free replaced_by=job-c"},
		{"request_id": "../x", "job": closeOpJob, "pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false, "end_reason": "quota_stop pool=devin-free replaced_by=job-c"},
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

// The three ops cross a real hub connection, and the hub accepts their
// results.
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
	seedCloseOpJob(t, inbox)
	client.nodeHerdr = fake.run
	client.nodeCloseSettle = time.Millisecond
	client.jobsInboxRoot = inbox
	client.nodeHubCloseIntent = noHubCloseIntent
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
	eventually(t, 2*time.Second, "node connected", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes["machine-a"]
		return record != nil && record.agent != nil && record.state == "connected"
	})
	args := nodePaneCloseOpArgs{RequestID: "op-1", Job: closeOpJob, Attempt: closeOpAttempt, PaneID: "w1:p1", Name: "job-a", AgentStatus: "idle", Cwd: "/work/a", EndReason: "quota_stop pool=devin-free"}
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

// The request must name the pane and label the job's current attempt runs in;
// otherwise nothing is opened or closed (a stale or mismatched request could
// otherwise close another job's pane while journalling in this one).
func TestCloseOpRefusesAMismatchedBinding_t4594(t *testing.T) {
	inbox := t.TempDir()
	for _, change := range []func(map[string]any){
		func(a map[string]any) { a["pane_id"] = "w1:p9" },
		func(a map[string]any) { a["name"] = "job-b" },
	} {
		fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p9", "job-b", "idle", "/work/a"), nodeAgentJSON("w1:p1", "job-b", "idle", "/work/a"))}, read: nodeReapScreen("❯ ")}
		args := closeOpArgs("op-x")
		change(args)
		result, code := nodeRun(t, closeOpNode(t, inbox, fake), nodeCommandPaneCloseOp, args)
		if code != "" || result["outcome"] != nodeCloseOpMismatch || len(fake.calls) != 0 {
			t.Fatalf("args=%v result=%v code=%q calls=%v", args, result, code, fake.calls)
		}
	}
	// An ended attempt is not closed either.
	dir := seedCloseOpJob(t, inbox)
	if err := os.WriteFile(filepath.Join(dir, "events", "00003-job.completed.json"), []byte(`{"kind":"job.completed","created_at":"2026-11-14T00:00:02Z","payload":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := idlePaneHerdr()
	wantCloseOp(t, closeOpNode(t, inbox, fake), "op-y", nodeCloseOpMismatch, false)
	if len(fake.calls) != 0 {
		t.Fatalf("closed an ended attempt: %v", fake.calls)
	}
}

// A registered operation resumed after its attempt moved on (a re-prompt on
// the same pane) is settled preempted without touching herdr.
func TestCloseOpResumeRevalidatesTheAttempt_t4594(t *testing.T) {
	inbox := t.TempDir()
	seedNodeCloseOp(t, inbox, "op-1", fleet.CloseOpRegistered)
	dir := filepath.Join(inbox, "jobs", closeOpJob)
	if err := os.WriteFile(filepath.Join(dir, "events", "00003-job.reprompted.json"), []byte(`{"kind":"job.reprompted","created_at":"2026-11-14T00:00:02Z","payload":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := idlePaneHerdr()
	wantCloseOp(t, closeOpNode(t, inbox, fake), "op-1", fleet.CloseOpPreempted, false)
	if len(fake.calls) != 0 {
		t.Fatalf("resumed a stale attempt: %v", fake.calls)
	}

}

// The blocked-reviewer guard reads the role from the job's own ledger, not
// from a directory named after the pane label.
func TestCloseOpBlockedReviewerRoleComesFromTheJob_t4594(t *testing.T) {
	inbox := t.TempDir()
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "blocked", "/work/a"))}, read: nodeReapScreen("❯ ")}
	client := closeOpNode(t, inbox, fake)
	args := closeOpArgs("op-1")
	args["agent_status"] = "blocked"
	result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
	if code != "" || result["outcome"] != fleet.CloseOpClosed {
		t.Fatalf("blocked reviewer under job-1/label job-a: result=%v code=%q calls=%v", result, code, fake.calls)
	}
	// A decoy ledger at jobs/job-a naming another role changes nothing.
	decoy := filepath.Join(inbox, "jobs", "job-a", "events")
	if err := os.MkdirAll(decoy, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(decoy, "00001-job.claim.json"), []byte(`{"kind":"job.claim","payload":{"role":"implementer"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fake2 := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "blocked", "/work/a"))}, read: nodeReapScreen("❯ ")}
	client.nodeHerdr = fake2.run
	args["request_id"] = "op-2"
	if result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "" || result["outcome"] != fleet.CloseOpClosed {
		t.Fatalf("decoy role changed the guard: result=%v code=%q", result, code)
	}
}

func addCloseOpEvent(t *testing.T, inbox, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(inbox, "jobs", closeOpJob, "events", name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A reclaim starts an attempt with no receipt until its own spawn: the old
// receipt's pane is not the new attempt's, so a close of it opens nothing.
func TestCloseOpReclaimBeforeReceiptIsNotBound_t4594(t *testing.T) {
	inbox := t.TempDir()
	seedCloseOpJob(t, inbox)
	addCloseOpEvent(t, inbox, "00003-job.reclaim.json", `{"kind":"job.reclaim","created_at":"2026-11-14T00:00:02Z","payload":{"owner_lane":"owner"}}`)
	root, err := os.OpenRoot(filepath.Join(inbox, "jobs", closeOpJob))
	if err != nil {
		t.Fatal(err)
	}
	target, err := readNodeCloseTarget(root)
	root.Close()
	if err != nil || target.receipt || target.attempt != "00003-job.reclaim.json" {
		t.Fatalf("target=%+v err=%v", target, err)
	}
	fake := idlePaneHerdr()
	wantCloseOp(t, closeOpNode(t, inbox, fake), "op-1", nodeCloseOpMismatch, false)
	if len(fake.calls) != 0 {
		t.Fatalf("closed the previous attempt's pane: %v", fake.calls)
	}
	if _, err := os.Stat(filepath.Join(inbox, "jobs", closeOpJob, fleet.CloseOpDir, "op-1.json")); !os.IsNotExist(err) {
		t.Fatalf("operation opened: %v", err)
	}
}

// An escalation ends the attempt: a new close opens nothing, and a resumed
// registered one settles preempted without touching herdr. A quota stop
// announced for the hub's hand-over is not an end.
func TestCloseOpEscalationEndsTheAttempt_t4594(t *testing.T) {
	inbox := t.TempDir()
	seedNodeCloseOp(t, inbox, "op-1", fleet.CloseOpRegistered)
	addCloseOpEvent(t, inbox, "00003-job.escalate.json", `{"kind":"job.escalate","created_at":"2026-11-14T00:00:02Z","payload":{}}`)
	fake := idlePaneHerdr()
	client := closeOpNode(t, inbox, fake)
	wantCloseOp(t, client, "op-1", fleet.CloseOpPreempted, false)
	wantCloseOp(t, client, "op-2", nodeCloseOpMismatch, false)
	if len(fake.calls) != 0 {
		t.Fatalf("closed an escalated attempt: %v", fake.calls)
	}

}

// A quota stop announced for the hub's hand-over is not an end by itself: the
// hk record decides whether the hand-over grace already ended the job. A live
// record leaves the attempt bound, a terminal one does not, and an unreadable
// one refuses without guessing.
func TestCloseOpAnnouncedQuotaStopAsksHK_t4594(t *testing.T) {
	for _, test := range []struct {
		name    string
		state   string
		getErr  error
		outcome string
		code    string
	}{
		{"hand-over pending", "spawned", nil, fleet.CloseOpClosed, ""},
		{"grace already ended it", "lost", nil, nodeCloseOpMismatch, ""},
		{"record unreadable", "spawned", errors.New("hk down"), "", "record_unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			inbox := t.TempDir()
			seedCloseOpJob(t, inbox)
			addCloseOpEvent(t, inbox, "00003-job.lost.json", `{"kind":"job.lost","created_at":"2026-11-14T00:00:02Z","payload":{"pane_id":"w1:p1","reason":"quota_stop","pool":"claude"}}`)
			fake := idlePaneHerdr()
			client := closeOpNode(t, inbox, fake)
			store := &roleJobs{records: map[string]map[string]any{closeOpJob: {"state": test.state}}}
			if test.getErr != nil {
				store.getErr = map[string]error{closeOpJob: test.getErr}
			}
			client.nodeJobs = store
			result, code := nodeRun(t, client, nodeCommandPaneCloseOp, closeOpArgs("op-1"))
			if code != test.code || (test.code == "" && result["outcome"] != test.outcome) {
				t.Fatalf("result=%v code=%q", result, code)
			}
			if test.outcome != fleet.CloseOpClosed && fake.called("pane", "close") != 0 {
				t.Fatalf("closed: %v", fake.calls)
			}
		})
	}
}

// Completion receipts end the attempt: the settled record and the legacy
// job.completion event, not only job.completed.
func TestCloseOpCompletionReceiptsEndTheAttempt_t4594(t *testing.T) {
	for _, kind := range []string{"job.record_settled", "job.completion"} {
		inbox := t.TempDir()
		seedCloseOpJob(t, inbox)
		addCloseOpEvent(t, inbox, "00003-"+kind+".json", `{"kind":"`+kind+`","created_at":"2026-11-14T00:00:02Z","payload":{}}`)
		fake := idlePaneHerdr()
		wantCloseOp(t, closeOpNode(t, inbox, fake), "op-1", nodeCloseOpMismatch, false)
		if len(fake.calls) != 0 {
			t.Fatalf("%s: closed a completed attempt: %v", kind, fake.calls)
		}
	}
}

// The proof is read again after the settle: a re-prompt the watcher records
// during it preempts the close, which never runs.
func TestCloseOpRepromptDuringSettlePreempts_t4594(t *testing.T) {
	inbox := t.TempDir()
	fake := idlePaneHerdr()
	client := closeOpNode(t, inbox, fake)
	run := fake.run
	client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
		// The first look happens before the settle; the watcher records a
		// re-prompt then.
		if len(args) >= 2 && args[0] == "agent" && args[1] == "list" && fake.called("agent", "list") == 0 {
			addCloseOpEvent(t, inbox, "00003-job.reprompted.json", `{"kind":"job.reprompted","created_at":"2026-11-14T00:00:02Z","payload":{}}`)
		}
		return run(ctx, args...)
	}
	wantCloseOp(t, client, "op-1", fleet.CloseOpPreempted, false)
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed a re-prompted attempt: %v", fake.calls)
	}
	wantCloseOp(t, client, "op-1", fleet.CloseOpPreempted, true)
}

// Every herdr call of a close operation runs in the session its job's
// receipt names, not the session of whichever job last used the pane id.
func TestCloseOpUsesItsJobsSession_t4594(t *testing.T) {
	fakeDir := t.TempDir()
	fakeFleetHerdr(t, fakeDir, "old")
	t.Setenv("HERDR_SESSION", "default")
	inbox := t.TempDir()
	for _, job := range []struct{ id, session, at string }{{closeOpJob, "old", "2026-11-14T00:00:01Z"}, {"job-newer", "fleet", "2026-11-14T00:10:00Z"}} {
		events := filepath.Join(inbox, "jobs", job.id, "events")
		if err := os.MkdirAll(events, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(events, "00001-job.claim.json"), []byte(`{"kind":"job.claim","created_at":"`+job.at+`","payload":{}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(events, "00002-job.spawned.json"), []byte(`{"kind":"job.spawned","created_at":"`+job.at+`","payload":{"pane_id":"w1:pW","label":"`+t3276Worker+`","role":"reviewer","herdr_session":"`+job.session+`"}}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	client := &HubClient{nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox, nodeHubCloseIntent: noHubCloseIntent}
	args := map[string]any{"request_id": "op-1", "job": closeOpJob, "attempt": closeOpAttempt, "pane_id": "w1:pW", "name": t3276Worker, "agent_status": "idle", "cwd": "/work/a", "allow_unnamed": false, "end_reason": "quota_stop pool=devin-free replaced_by=job-c"}
	result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args)
	if errs, _ := os.ReadFile(filepath.Join(fakeDir, "errors")); len(errs) != 0 {
		t.Fatalf("herdr called in another job's session: %s", errs)
	}
	// The fake lists the pane without a cwd, so the guard refuses: what
	// matters here is that every call reached the job's own session.
	if code != "" || result["outcome"] != fleet.CloseOpRefused {
		t.Fatalf("result=%v code=%q", result, code)
	}
}

// An empty role in the job's own ledger is unknown, and refuses a blocked
// close even when a directory named after the label says reviewer.
func TestCloseOpUnknownRoleRefusesBlocked_t4594(t *testing.T) {
	inbox := t.TempDir()
	dir := filepath.Join(inbox, "jobs", closeOpJob, "events")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	addCloseOpEvent(t, inbox, "00001-job.claim.json", `{"kind":"job.claim","created_at":"2026-11-14T00:00:00Z","payload":{}}`)
	addCloseOpEvent(t, inbox, "00002-job.spawned.json", `{"kind":"job.spawned","created_at":"2026-11-14T00:00:01Z","payload":{"pane_id":"w1:p1","label":"job-a","herdr_session":"fleet"}}`)
	decoy := filepath.Join(inbox, "jobs", "job-a", "events")
	if err := os.MkdirAll(decoy, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(decoy, "00001-job.claim.json"), []byte(`{"kind":"job.claim","payload":{"role":"reviewer"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "blocked", "/work/a"))}, read: nodeReapScreen("❯ ")}
	args := closeOpArgs("op-1")
	args["agent_status"] = "blocked"
	result, code := nodeRun(t, &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox, nodeHubCloseIntent: noHubCloseIntent}, nodeCommandPaneCloseOp, args)
	if code != "" || result["outcome"] != fleet.CloseOpRefused || fake.called("pane", "close") != 0 {
		t.Fatalf("result=%v code=%q calls=%v", result, code, fake.calls)
	}
}

// A job id spawn accepts, such as one containing "..", is accepted.
func TestCloseOpAcceptsSpawnJobIDs_t4594(t *testing.T) {
	for _, id := range []string{"build..42", "a.b"} {
		args := closeOpArgs("op-1")
		args["job"] = id
		raw, _ := json.Marshal(args)
		if _, valid := decodeNodeCommandArgs(nodeCommandPaneCloseOp, raw); !valid {
			t.Fatalf("job %q refused", id)
		}
		if _, valid := decodeNodeCommandArgs(nodeCommandPaneCloseStatus, json.RawMessage(`{"request_id":"op-1","job":"`+id+`"}`)); !valid {
			t.Fatalf("status for job %q refused", id)
		}
	}
	for _, id := range []string{"..", ".", "a/b", "-x", ""} {
		args := closeOpArgs("op-1")
		args["job"] = id
		raw, _ := json.Marshal(args)
		if _, valid := decodeNodeCommandArgs(nodeCommandPaneCloseOp, raw); valid {
			t.Fatalf("job %q accepted", id)
		}
	}
}

// A failed write of closing is a journal failure, not a close failure, and
// leaves the pane open.
func TestCloseOpFailedClosingWriteIsJournalFailed_t4594(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the directory permission this test relies on")
	}
	inbox := t.TempDir()
	fake := idlePaneHerdr()
	client := closeOpNode(t, inbox, fake)
	ops := filepath.Join(inbox, "jobs", closeOpJob, fleet.CloseOpDir)
	t.Cleanup(func() { _ = os.Chmod(ops, 0o700) })
	run := fake.run
	client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
		// After registration, before the closing write.
		if len(args) >= 2 && args[0] == "agent" && args[1] == "list" {
			_ = os.Chmod(ops, 0o500)
		}
		return run(ctx, args...)
	}
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, closeOpArgs("op-1")); code != "journal_failed" {
		t.Fatalf("code=%q", code)
	}
	if fake.called("pane", "close") != 0 {
		t.Fatalf("closed after a failed journal write: %v", fake.calls)
	}
}

// The hub names the attempt it decided on (read from pane.close_status): a
// request for an attempt that is no longer current opens nothing, even when
// the pane, label and status match again after a re-prompt.
func TestCloseOpRequiresTheDecidedAttempt_t4594(t *testing.T) {
	inbox := t.TempDir()
	fake := idlePaneHerdr()
	client := closeOpNode(t, inbox, fake)
	status, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("op-1"))
	if code != "" || status["state"] != nodeCloseOpNone || status["attempt"] != closeOpAttempt {
		t.Fatalf("status=%v code=%q", status, code)
	}
	addCloseOpEvent(t, inbox, "00003-job.reprompted.json", `{"kind":"job.reprompted","created_at":"2026-11-14T00:00:02Z","payload":{}}`)
	wantCloseOp(t, client, "op-1", nodeCloseOpMismatch, false)
	if len(fake.calls) != 0 {
		t.Fatalf("closed the re-prompted attempt: %v", fake.calls)
	}
	args := closeOpArgs("op-2")
	args["attempt"] = "00003-job.reprompted.json"
	if result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "" || result["outcome"] != fleet.CloseOpClosed {
		t.Fatalf("current attempt: result=%v code=%q", result, code)
	}
	// A repeat must name the attempt the operation was registered for.
	args["attempt"] = closeOpAttempt
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "invalid_args" {
		t.Fatalf("repeat with another attempt code=%q", code)
	}
}

// A receipt that recorded no herdr session cannot say where its pane lives:
// the node refuses rather than guessing the configured session.
func TestCloseOpRefusesAReceiptWithoutSession_t4594(t *testing.T) {
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", closeOpJob, "events"), 0o700); err != nil {
		t.Fatal(err)
	}
	addCloseOpEvent(t, inbox, "00001-job.claim.json", `{"kind":"job.claim","created_at":"2026-11-14T00:00:00Z","payload":{}}`)
	addCloseOpEvent(t, inbox, "00002-job.spawned.json", `{"kind":"job.spawned","created_at":"2026-11-14T00:00:01Z","payload":{"pane_id":"w1:p1","label":"job-a","role":"reviewer"}}`)
	fake := idlePaneHerdr()
	wantCloseOp(t, &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox, nodeHubCloseIntent: noHubCloseIntent}, "op-1", nodeCloseOpMismatch, false)
	if len(fake.calls) != 0 {
		t.Fatalf("acted in a guessed session: %v", fake.calls)
	}
}

// An open reviewer quota-stop intent defers to hk like an announced stop: a
// watcher that wrote hk lost and crashed before its final job.lost leaves an
// ended attempt the node must not close.
func TestCloseOpOpenQuotaIntentAsksHK_t4594(t *testing.T) {
	for _, test := range []struct {
		state, outcome string
	}{{"lost", nodeCloseOpMismatch}, {"spawned", fleet.CloseOpClosed}} {
		inbox := t.TempDir()
		seedCloseOpJob(t, inbox)
		addCloseOpEvent(t, inbox, "00003-job.quota_stop_intent.json", `{"kind":"job.quota_stop_intent","created_at":"2026-11-14T00:00:02Z","payload":{"pane_id":"w1:p1","pool":"devin-free","end_reason":"quota_stop pool=devin-free"}}`)
		fake := idlePaneHerdr()
		client := closeOpNode(t, inbox, fake)
		client.nodeJobs = &roleJobs{records: map[string]map[string]any{closeOpJob: {"state": test.state}}}
		wantCloseOp(t, client, "op-1", test.outcome, false)
	}
}

// A repeat naming another attempt than the one its request id was registered
// for is invalid_args before any replay or settlement: attempt B never gets
// attempt A's recorded outcome.
func TestCloseOpRepeatChecksTheAttemptFirst_t4594(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	wantCloseOp(t, client, "op-1", fleet.CloseOpClosed, false)
	seedNodeCloseOp(t, inbox, "op-2", fleet.CloseOpClosing)
	for _, id := range []string{"op-1", "op-2"} {
		args := closeOpArgs(id)
		args["attempt"] = "00009-job.reprompted.json"
		if result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "invalid_args" {
			t.Fatalf("%s with another attempt: result=%v code=%q", id, result, code)
		}
	}
	wantCloseOp(t, client, "op-1", fleet.CloseOpClosed, true)
}

// A quota intent the watcher cancelled as superseded (hk already terminal,
// no local terminal event) ends the attempt; another cancellation reason
// clears the intent and leaves the attempt live.
func TestCloseOpSupersededQuotaIntentIsAnEnd_t4594(t *testing.T) {
	for _, test := range []struct {
		reason, outcome string
	}{{"superseded", nodeCloseOpMismatch}, {"pane_moved_on", fleet.CloseOpClosed}} {
		inbox := t.TempDir()
		seedCloseOpJob(t, inbox)
		addCloseOpEvent(t, inbox, "00003-job.quota_stop_intent.json", `{"kind":"job.quota_stop_intent","created_at":"2026-11-14T00:00:02Z","payload":{"pane_id":"w1:p1","pool":"devin-free","end_reason":"quota_stop pool=devin-free"}}`)
		addCloseOpEvent(t, inbox, "00004-job.quota_stop_cancelled.json", `{"kind":"job.quota_stop_cancelled","created_at":"2026-11-14T00:00:03Z","payload":{"reason":"`+test.reason+`"}}`)
		fake := idlePaneHerdr()
		wantCloseOp(t, closeOpNode(t, inbox, fake), "op-1", test.outcome, false)
		if test.outcome != fleet.CloseOpClosed && len(fake.calls) != 0 {
			t.Fatalf("%s: closed an ended attempt: %v", test.reason, fake.calls)
		}
	}
}

// A registration records the hub-close intent, bound to the decided attempt,
// pane and label with the planned end_reason, before the journal entry; a
// replay records nothing more, a mismatch nothing at all, and a failed intent
// write registers no operation (hk 4652).
func TestCloseOpRecordsTheHubCloseIntentFirst_t4652(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	var intents []fleet.HubCloseIntent
	client.nodeHubCloseIntent = func(_ context.Context, job string, in fleet.HubCloseIntent) error {
		if job != closeOpJob {
			t.Errorf("intent for job %q", job)
		}
		if _, err := os.Stat(filepath.Join(inbox, "jobs", closeOpJob, fleet.CloseOpDir, in.RequestID+".json")); err == nil {
			t.Error("operation registered before its intent")
		}
		intents = append(intents, in)
		return nil
	}
	wantCloseOp(t, client, "op-1", fleet.CloseOpClosed, false)
	wantCloseOp(t, client, "op-1", fleet.CloseOpClosed, true)
	want := fleet.HubCloseIntent{RequestID: "op-1", Attempt: closeOpAttempt, Pane: "w1:p1", Label: "job-a", EndReason: "quota_stop pool=devin-free replaced_by=job-c"}
	if len(intents) != 1 || intents[0] != want {
		t.Fatalf("intents %+v, want exactly %+v", intents, want)
	}
	args := closeOpArgs("op-2")
	args["name"] = "job-b"
	if result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "" || result["outcome"] != nodeCloseOpMismatch || len(intents) != 1 {
		t.Fatalf("mismatch: result=%v code=%q intents=%d", result, code, len(intents))
	}
	client.nodeHubCloseIntent = func(context.Context, string, fleet.HubCloseIntent) error { return errors.New("ledger down") }
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, closeOpArgs("op-3")); code != "journal_failed" {
		t.Fatalf("failed intent write: code=%q, want journal_failed", code)
	}
	wantCloseStatus(t, client, nodeCommandPaneCloseStatus, "op-3", "none", false)
}

// end_reason is required: without a planned record the watcher could not
// finish the job.
func TestCloseOpRequiresAnEndReason_t4652(t *testing.T) {
	client := closeOpNode(t, t.TempDir(), idlePaneHerdr())
	for _, reason := range []any{"", "two\nlines", nil} {
		args := closeOpArgs("op-1")
		if reason == nil {
			delete(args, "end_reason")
		} else {
			args["end_reason"] = reason
		}
		if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "invalid_args" {
			t.Fatalf("end_reason %q: code=%q, want invalid_args", reason, code)
		}
	}
}

func TestHelloAdvertisesCloseOps_t4652(t *testing.T) {
	if !slices.Contains(hubSpawnCapabilities, "close_ops_v1") {
		t.Fatalf("hello capabilities %v lack close_ops_v1", hubSpawnCapabilities)
	}
}
