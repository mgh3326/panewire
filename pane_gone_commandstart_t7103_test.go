package panewire

// hk 7103: a pane.close marks only the jobs that completed before the close
// command started. A job that completed after the close began, on the same pane
// id (the id can be reused), must keep its PaneGone unset so its lane events are
// not dropped job_ended.

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPaneCloseMarksOnlyJobsCompletedBeforeCommandStart_t7103(t *testing.T) {
	start := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	clock := start.Add(-time.Minute)
	hub, err := NewHubServer(HubServerConfig{
		InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"),
		Tokens:         map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken},
		Now:            func() time.Time { return clock }, GracePeriod: time.Hour, OrphanGrace: time.Hour, KeepaliveInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	useLegacyOwnerStore(hub)
	agent := &hubAgent{revocations: make(chan hubJobRevokedEvent, 2)}
	hub.connect("node-a", "t7103", "fixture", agent, false)
	jobs := []HubActiveJob{
		{JobID: "done-before", AgentLabel: "agent-a", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"},
		{JobID: "done-after", AgentLabel: "agent-b", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"},
	}
	hub.observeActiveJobs("node-a", jobs, clock)

	// Both jobs finish on the node: one before the close command started, one
	// after it. The completion is observed at the hub's receive time.
	if !hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: "done-before", Epoch: 1}, start.Add(-time.Second)) {
		t.Fatal("completion before the close was rejected")
	}
	if !hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: "done-after", Epoch: 1}, start.Add(time.Second)) {
		t.Fatal("completion after the close began was rejected")
	}
	// A job completes before the close, and a repeated completion lands after the
	// close began. The repeat rewrites LastSeen, not the completion, so the job is
	// still marked: LastSeen is not a completion time.
	hub.observeActiveJobs("node-a", append(jobs, HubActiveJob{JobID: "done-twice", AgentLabel: "agent-c", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"}), clock)
	if !hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: "done-twice", Epoch: 1}, start.Add(-2*time.Second)) {
		t.Fatal("completion of the job was rejected")
	}
	if !hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: "done-twice", Epoch: 1}, start.Add(time.Second)) {
		t.Fatal("repeated completion after the close began was rejected")
	}

	closed := hubNodeCommandResult{Result: []byte(`{"outcome":"closed"}`)}
	hub.releaseClosedPaneLanes("node-a", nodePaneCloseArgs{PaneID: "reused-pane"}, closed, start)

	hub.mu.Lock()
	before, after := hub.jobs["done-before"].PaneGone, hub.jobs["done-after"].PaneGone
	twice := hub.jobs["done-twice"].PaneGone
	hub.mu.Unlock()
	if !before {
		t.Fatal("a job completed before the close began was not marked pane gone")
	}
	if after {
		t.Fatal("a job completed after the close began was marked pane gone by a close it did not belong to")
	}
	if !twice {
		t.Fatal("a job whose repeated completion landed after the close began was not marked pane gone")
	}
}
